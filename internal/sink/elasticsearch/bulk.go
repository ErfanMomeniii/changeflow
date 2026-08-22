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
	Addresses      []string
	Index          string
	Alias          string
	Username       string
	Password       string
	Workers        int
	MaxAttempts    int
	BaseBackoff    time.Duration
	MaxBackoff     time.Duration
	Compress       bool
	RequestTimeout time.Duration
	HTTPClient     *http.Client
}

// Sink writes documents to Elasticsearch.
type Sink struct {
	opts         Options
	client       *http.Client
	indexAction  []byte
	deleteAction []byte
	nextURL      atomic.Uint64
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
	encodedIndex, err := json.Marshal(opts.Index)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch: encode index name %q: %w", opts.Index, err)
	}
	return &Sink{
		opts:         opts,
		client:       client,
		indexAction:  []byte(`{"index":{"_index":` + string(encodedIndex) + `,"_id":`),
		deleteAction: []byte(`{"delete":{"_index":` + string(encodedIndex) + `,"_id":`),
	}, nil
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
		pending = outcome.retry
		if err := s.wait(ctx, attempt); err != nil {
			return result, err
		}
	}
}

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
		return outcome{retryable: true}, fmt.Errorf("elasticsearch: bulk request: %w", err)
	}
	switch {
	case status == http.StatusRequestEntityTooLarge:
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

func (s *Sink) classify(docs []cdc.Doc, payload []byte) (outcome, error) {
	var resp bulkResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return outcome{retryable: true}, fmt.Errorf("elasticsearch: cannot read bulk response: %w", err)
	}
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
			out.stale++
		case item.Status == http.StatusNotFound && docs[i].Deleted:
			out.stale++
		case item.Status == http.StatusTooManyRequests, item.Status >= 500:
			out.retry = append(out.retry, docs[i])
		default:
			out.rejected = append(out.rejected, sink.Rejection{
				Doc:    docs[i],
				Status: item.Status,
				Reason: strings.TrimSpace(item.Error.Type + ": " + item.Error.Reason),
			})
		}
	}
	return out, nil
}

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

func (s *Sink) encodeBulk(docs []cdc.Doc) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(docs) * 256)
	for _, d := range docs {
		if d.Key == "" {
			return nil, errors.New("elasticsearch: document has no key")
		}
		if d.Deleted {
			buf.Write(s.deleteAction)
		} else {
			buf.Write(s.indexAction)
		}
		appendJSONString(&buf, d.Key)
		buf.WriteString(`,"version":`)
		buf.Write(strconv.AppendUint(buf.AvailableBuffer(), d.Version, 10))
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

func appendJSONString(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			buf.WriteString(`\"`)
		case c == '\\':
			buf.WriteString(`\\`)
		case c < 0x20:
			const hexDigits = "0123456789abcdef"
			buf.WriteString(`\u00`)
			buf.WriteByte(hexDigits[c>>4])
			buf.WriteByte(hexDigits[c&0x0f])
		default:
			buf.WriteByte(c)
		}
	}
	buf.WriteByte('"')
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

func (s *Sink) nextAddress() string {
	if len(s.opts.Addresses) == 1 {
		return s.opts.Addresses[0]
	}
	i := s.nextURL.Add(1) - 1
	return s.opts.Addresses[i%uint64(len(s.opts.Addresses))]
}

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
