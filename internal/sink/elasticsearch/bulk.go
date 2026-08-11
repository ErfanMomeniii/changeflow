// Package elasticsearch writes documents to Elasticsearch using the bulk API.
//
// Every write carries an external version, which is what makes the sink
// idempotent: Elasticsearch compares the version it holds and refuses anything
// not newer. A replayed batch therefore converges instead of corrupting, and a
// version conflict is an expected outcome rather than an error.
//
// The official client is not used on the write path. It re-marshals documents,
// which would undo the pipeline's encode-once property, and it hides the per-item
// status codes this sink has to distinguish between.
package elasticsearch

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/sink"
)

// Options configures a sink.
type Options struct {
	// Addresses are the cluster's HTTP endpoints, tried in rotation.
	Addresses []string
	// Index is the concrete index written to. Readers should query an alias
	// instead, so a rebuild is a single atomic alias move.
	Index string
	// Alias is recorded for rebuild tooling and is never written to directly.
	Alias string

	Username string
	Password string

	Workers int

	// MaxAttempts bounds retries of a batch. Exhausting it fails the batch, which
	// keeps the checkpoint from advancing past unwritten documents.
	MaxAttempts int
	// BaseBackoff is the first retry delay; later delays grow exponentially.
	BaseBackoff time.Duration
	MaxBackoff  time.Duration

	// Compress gzips request bodies. Worth enabling in production, where bulk
	// bodies are large and mostly text.
	Compress bool

	RequestTimeout time.Duration
	HTTPClient     *http.Client
}

// Sink writes documents to Elasticsearch.
type Sink struct {
	opts    Options
	client  *http.Client
	nextURL atomic.Uint64
}

// New validates options and returns a sink.
func New(opts Options) (*Sink, error) {
	if len(opts.Addresses) == 0 {
		return nil, errors.New("elasticsearch: at least one address is required")
	}
	if opts.Index == "" {
		return nil, errors.New("elasticsearch: an index is required")
	}
	if opts.Workers < 0 {
		return nil, errors.New("elasticsearch: workers cannot be negative")
	}
	if opts.Workers == 0 {
		opts.Workers = 1
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 5
	}
	if opts.BaseBackoff <= 0 {
		opts.BaseBackoff = 200 * time.Millisecond
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = 30 * time.Second
	}
	if opts.RequestTimeout <= 0 {
		opts.RequestTimeout = 60 * time.Second
	}

	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: opts.RequestTimeout}
	}

	for i, addr := range opts.Addresses {
		opts.Addresses[i] = strings.TrimRight(addr, "/")
	}

	return &Sink{opts: opts, client: client}, nil
}

// Write applies a batch, retrying the documents that failed for a reason a retry
// could fix.
func (s *Sink) Write(ctx context.Context, docs []cdc.Doc) (sink.Result, error) {
	var result sink.Result
	if len(docs) == 0 {
		return result, nil
	}

	pending := docs
	for attempt := 1; ; attempt++ {
		outcome, err := s.attempt(ctx, pending)
		if err != nil {
			if !outcome.retryable || attempt >= s.opts.MaxAttempts {
				return result, err
			}
			if waitErr := s.wait(ctx, attempt); waitErr != nil {
				return result, fmt.Errorf("elasticsearch: %w (last error: %v)", waitErr, err)
			}
			continue
		}

		result.Applied += outcome.applied
		result.Stale += outcome.stale
		result.Rejected = append(result.Rejected, outcome.rejected...)

		if len(outcome.retry) == 0 {
			return result, nil
		}
		if attempt >= s.opts.MaxAttempts {
			return result, fmt.Errorf("elasticsearch: %d document(s) still rejected for capacity reasons after %d attempts", len(outcome.retry), attempt)
		}
		// Only the documents that failed are resent; the rest are already applied.
		pending = outcome.retry
		if err := s.wait(ctx, attempt); err != nil {
			return result, err
		}
	}
}

// outcome is one attempt's result.
type outcome struct {
	applied   int
	stale     int
	rejected  []sink.Rejection
	retry     []cdc.Doc
	retryable bool
}

func (s *Sink) attempt(ctx context.Context, docs []cdc.Doc) (outcome, error) {
	body, err := s.encodeBulk(docs)
	if err != nil {
		return outcome{}, err
	}

	status, payload, err := s.post(ctx, body)
	if err != nil {
		// A transport failure says nothing about whether the batch was applied, so
		// it is retried and idempotency covers the overlap.
		return outcome{retryable: true}, fmt.Errorf("elasticsearch: bulk request: %w", err)
	}

	switch {
	case status == http.StatusRequestEntityTooLarge:
		// The request exceeded the cluster's limit. Splitting is the only thing that
		// can help, and a smaller half may still succeed.
		if len(docs) > 1 {
			return s.writeHalves(ctx, docs)
		}
		return outcome{}, fmt.Errorf("elasticsearch: a single document exceeds the cluster's request limit (key %s)", docs[0].Key)

	case status == http.StatusTooManyRequests, status >= 500:
		return outcome{retryable: true}, fmt.Errorf("elasticsearch: bulk request failed with status %d: %s", status, snippet(payload))

	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		return outcome{}, fmt.Errorf("elasticsearch: rejected our credentials with status %d; retrying cannot help: %s", status, snippet(payload))

	case status >= 400:
		return outcome{}, fmt.Errorf("elasticsearch: rejected the request with status %d; the request itself is malformed: %s", status, snippet(payload))
	}

	return s.classify(docs, payload)
}

// writeHalves splits an over-large request and applies each half.
func (s *Sink) writeHalves(ctx context.Context, docs []cdc.Doc) (outcome, error) {
	mid := len(docs) / 2
	var combined outcome

	for _, half := range [][]cdc.Doc{docs[:mid], docs[mid:]} {
		res, err := s.Write(ctx, half)
		combined.applied += res.Applied
		combined.stale += res.Stale
		combined.rejected = append(combined.rejected, res.Rejected...)
		if err != nil {
			return combined, err
		}
	}
	return combined, nil
}

// bulkResponse is the part of a bulk reply this sink reads.
type bulkResponse struct {
	Errors bool              `json:"errors"`
	Items  []json.RawMessage `json:"items"`
}

type bulkItem struct {
	Status int `json:"status"`
	Error  struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
}

// classify maps each response item onto its document.
func (s *Sink) classify(docs []cdc.Doc, payload []byte) (outcome, error) {
	var resp bulkResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return outcome{retryable: true}, fmt.Errorf("elasticsearch: cannot read bulk response: %w", err)
	}

	// Items correspond to actions positionally. A different count means the
	// response cannot be attributed, and assuming success would advance the
	// checkpoint past documents that were never written.
	if len(resp.Items) != len(docs) {
		return outcome{}, fmt.Errorf("elasticsearch: bulk response describes %d of %d documents, so the outcome cannot be attributed", len(resp.Items), len(docs))
	}

	var out outcome
	for i, raw := range resp.Items {
		item, err := parseItem(raw)
		if err != nil {
			return outcome{}, fmt.Errorf("elasticsearch: document %s: %w", docs[i].Key, err)
		}

		switch {
		case item.Status >= 200 && item.Status < 300:
			out.applied++

		case item.Status == http.StatusConflict:
			// Elasticsearch already holds an equal or newer version. This is what a
			// replayed batch looks like, and it means the state is already correct.
			out.stale++

		case item.Status == http.StatusNotFound && docs[i].Deleted:
			// Deleting something already absent leaves the intended state.
			out.stale++

		case item.Status == http.StatusTooManyRequests, item.Status >= 500:
			// Not written, and a retry may succeed.
			out.retry = append(out.retry, docs[i])

		default:
			// A mapping conflict or malformed document fails identically forever.
			out.rejected = append(out.rejected, sink.Rejection{
				Doc:    docs[i],
				Status: item.Status,
				Reason: strings.TrimSpace(item.Error.Type + ": " + item.Error.Reason),
			})
		}
	}
	return out, nil
}

// parseItem unwraps the single-key object that names the action.
func parseItem(raw json.RawMessage) (bulkItem, error) {
	var byAction map[string]bulkItem
	if err := json.Unmarshal(raw, &byAction); err != nil {
		return bulkItem{}, fmt.Errorf("cannot read bulk item: %w", err)
	}
	for _, item := range byAction {
		return item, nil
	}
	return bulkItem{}, errors.New("bulk item names no action")
}

// encodeBulk builds the newline-delimited body.
//
// Document bodies are appended as the pipeline encoded them, never re-marshalled,
// which is what keeps a value serialized exactly once on its way to the cluster.
func (s *Sink) encodeBulk(docs []cdc.Doc) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(docs) * 256)

	for _, d := range docs {
		if d.Key == "" {
			return nil, errors.New("elasticsearch: document has no key")
		}

		action := "index"
		if d.Deleted {
			action = "delete"
		}

		buf.WriteString(`{"`)
		buf.WriteString(action)
		buf.WriteString(`":{"_index":`)
		if err := writeJSONString(&buf, s.opts.Index); err != nil {
			return nil, err
		}
		buf.WriteString(`,"_id":`)
		if err := writeJSONString(&buf, d.Key); err != nil {
			return nil, err
		}
		// External versioning makes the cluster, not us, enforce that an older write
		// never overwrites a newer one.
		buf.WriteString(`,"version":`)
		buf.WriteString(strconv.FormatUint(d.Version, 10))
		buf.WriteString(`,"version_type":"external"}}`)
		buf.WriteByte('\n')

		if !d.Deleted {
			if len(d.Body) == 0 {
				return nil, fmt.Errorf("elasticsearch: document %s has no body", d.Key)
			}
			buf.Write(d.Body)
			buf.WriteByte('\n')
		}
	}

	return buf.Bytes(), nil
}

func writeJSONString(buf *bytes.Buffer, s string) error {
	encoded, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("elasticsearch: encode %q: %w", s, err)
	}
	buf.Write(encoded)
	return nil
}

func (s *Sink) post(ctx context.Context, body []byte) (int, []byte, error) {
	url := s.nextAddress() + "/_bulk"

	var (
		reader  io.Reader = bytes.NewReader(body)
		gzipped bool
	)
	if s.opts.Compress {
		var compressed bytes.Buffer
		zw := gzip.NewWriter(&compressed)
		if _, err := zw.Write(body); err != nil {
			return 0, nil, err
		}
		if err := zw.Close(); err != nil {
			return 0, nil, err
		}
		reader, gzipped = bytes.NewReader(compressed.Bytes()), true
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, reader)
	if err != nil {
		return 0, nil, err
	}
	// The bulk API requires this exact content type.
	req.Header.Set("Content-Type", "application/x-ndjson")
	if gzipped {
		req.Header.Set("Content-Encoding", "gzip")
	}
	if s.opts.Username != "" {
		req.SetBasicAuth(s.opts.Username, s.opts.Password)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("read response: %w", err)
	}
	return resp.StatusCode, payload, nil
}

// nextAddress rotates over the configured endpoints so one node does not absorb
// every request.
func (s *Sink) nextAddress() string {
	if len(s.opts.Addresses) == 1 {
		return s.opts.Addresses[0]
	}
	i := s.nextURL.Add(1) - 1
	return s.opts.Addresses[i%uint64(len(s.opts.Addresses))]
}

// wait backs off before the next attempt, with jitter so several workers recovering
// from the same outage do not resend in lockstep.
func (s *Sink) wait(ctx context.Context, attempt int) error {
	delay := s.opts.BaseBackoff << (attempt - 1)
	if delay > s.opts.MaxBackoff || delay <= 0 {
		delay = s.opts.MaxBackoff
	}
	delay += time.Duration(rand.Int63n(int64(delay/2) + 1))

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Close releases idle connections.
func (s *Sink) Close() error {
	s.client.CloseIdleConnections()
	return nil
}

func snippet(payload []byte) string {
	const limit = 300
	if len(payload) > limit {
		return string(payload[:limit]) + "..."
	}
	return string(payload)
}
