package clickhouse

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
	"github.com/ErfanMomeniii/changeflow/internal/sink"
)

// Options configures a sink.
type Options struct {
	DSN            string
	Table          string
	Workers        int
	MaxAttempts    int
	BaseBackoff    time.Duration
	MaxBackoff     time.Duration
	Compress       bool
	RequestTimeout time.Duration
	HTTPClient     *http.Client
}

// Sink writes documents to ClickHouse.
type Sink struct {
	opts     Options
	client   *http.Client
	endpoint *url.URL
	user     string
	password string
	database string
}

// New validates options and returns a sink.
func New(opts Options) (*Sink, error) {
	if opts.DSN == "" {
		return nil, errors.New("clickhouse: a dsn is required")
	}
	if opts.Table == "" {
		return nil, errors.New("clickhouse: a table is required")
	}
	if opts.Workers < 0 {
		return nil, errors.New("clickhouse: workers cannot be negative")
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
		opts.RequestTimeout = 120 * time.Second
	}
	endpoint, err := url.Parse(opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: parse dsn: %w", err)
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return nil, fmt.Errorf("clickhouse: dsn scheme %q is not http or https; this sink uses the HTTP interface", endpoint.Scheme)
	}
	s := &Sink{opts: opts, endpoint: endpoint, database: endpoint.Query().Get("database")}
	if endpoint.User != nil {
		s.user = endpoint.User.Username()
		s.password, _ = endpoint.User.Password()
		s.endpoint.User = nil
	}
	s.client = opts.HTTPClient
	if s.client == nil {
		s.client = &http.Client{Timeout: opts.RequestTimeout}
	}
	return s, nil
}

// Write inserts a batch.
//
// The engine deduplicates by version, so re-inserting a row that is already present is
// harmless and needs no per-document check.
func (s *Sink) Write(ctx context.Context, docs []cdc.Doc) (sink.Result, error) {
	var result sink.Result
	if len(docs) == 0 {
		return result, nil
	}
	return s.write(ctx, docs, 1)
}

func (s *Sink) write(ctx context.Context, docs []cdc.Doc, depth int) (sink.Result, error) {
	var result sink.Result
	body, err := s.encodeRows(docs)
	if err != nil {
		return result, err
	}
	for attempt := 1; ; attempt++ {
		status, payload, err := s.post(ctx, body, dedupToken(docs))
		switch {
		case err != nil:
			if attempt >= s.opts.MaxAttempts {
				return result, fmt.Errorf("clickhouse: insert %d row(s): %w", len(docs), err)
			}
		case status < 300:
			result.Applied = len(docs)
			return result, nil
		case retryableStatus(status):
			if attempt >= s.opts.MaxAttempts {
				return result, fmt.Errorf("clickhouse: insert failed with status %d after %d attempts: %s",
					status, attempt, snippet(payload))
			}
		default:
			return s.isolate(ctx, docs, depth, status, payload)
		}
		if waitErr := s.wait(ctx, attempt); waitErr != nil {
			return result, waitErr
		}
	}
}

const maxIsolationDepth = 12

func (s *Sink) isolate(ctx context.Context, docs []cdc.Doc, depth, status int, payload []byte) (sink.Result, error) {
	reason := fmt.Sprintf("status %d: %s", status, snippet(payload))
	if len(docs) == 1 {
		return sink.Result{Rejected: []sink.Rejection{{
			Doc: docs[0], Status: status, Reason: reason,
		}}}, nil
	}
	if depth >= maxIsolationDepth {
		return sink.Result{}, fmt.Errorf("clickhouse: rejected a batch of %d rows and stopped narrowing after %d splits: %s",
			len(docs), depth, reason)
	}
	mid := len(docs) / 2
	var combined sink.Result
	for _, half := range [][]cdc.Doc{docs[:mid], docs[mid:]} {
		part, err := s.write(ctx, half, depth+1)
		combined.Applied += part.Applied
		combined.Stale += part.Stale
		combined.Rejected = append(combined.Rejected, part.Rejected...)
		if err != nil {
			return combined, err
		}
	}
	return combined, nil
}

func (s *Sink) encodeRows(docs []cdc.Doc) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(docs) * 256)
	for _, d := range docs {
		if len(d.Body) == 0 {
			return nil, fmt.Errorf("clickhouse: document %s has no body", d.Key)
		}
		trimmed := bytes.TrimRight(d.Body, " \t\r\n")
		if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
			return nil, fmt.Errorf("clickhouse: document %s is not a JSON object", d.Key)
		}
		buf.Write(trimmed[:len(trimmed)-1])
		if len(trimmed) > 2 {
			buf.WriteByte(',')
		}
		buf.WriteString(`"`)
		buf.WriteString(schema.VersionColumn)
		buf.WriteString(`":`)
		buf.WriteString(strconv.FormatUint(d.Version, 10))
		buf.WriteString(`,"`)
		buf.WriteString(schema.DeletedColumn)
		buf.WriteString(`":`)
		if d.Deleted {
			buf.WriteByte('1')
		} else {
			buf.WriteByte('0')
		}
		buf.WriteString("}\n")
	}
	return buf.Bytes(), nil
}

func dedupToken(docs []cdc.Doc) string {
	if len(docs) == 0 {
		return ""
	}
	return fmt.Sprintf("%s-%d-%d-%d",
		docs[0].Key, docs[0].Version, docs[len(docs)-1].Version, len(docs))
}

func (s *Sink) post(ctx context.Context, body []byte, token string) (int, []byte, error) {
	endpoint := *s.endpoint
	query := endpoint.Query()
	query.Set("query", fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow", s.qualifiedTable()))
	if token != "" {
		query.Set("insert_deduplication_token", token)
	}
	query.Set("async_insert", "0")
	endpoint.RawQuery = query.Encode()
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), reader)
	if err != nil {
		return 0, nil, err
	}
	if gzipped {
		req.Header.Set("Content-Encoding", "gzip")
	}
	if s.user != "" {
		req.Header.Set("X-ClickHouse-User", s.user)
		req.Header.Set("X-ClickHouse-Key", s.password)
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

func (s *Sink) qualifiedTable() string {
	if s.database != "" && !strings.Contains(s.opts.Table, ".") {
		return s.database + "." + s.opts.Table
	}
	return s.opts.Table
}

func retryableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
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
	text := strings.TrimSpace(string(payload))
	if len(text) > limit {
		return text[:limit] + "..."
	}
	return text
}
