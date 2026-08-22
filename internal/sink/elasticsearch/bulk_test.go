package elasticsearch

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
)

type recorder struct {
	mu       sync.Mutex
	bodies   []string
	requests int
	respond  func(attempt int, body string) (int, string)
}

func (r *recorder) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		body := readBody(req)
		r.mu.Lock()
		r.requests++
		attempt := r.requests
		r.bodies = append(r.bodies, body)
		r.mu.Unlock()
		status, reply := 200, ""
		if r.respond != nil {
			status, reply = r.respond(attempt, body)
		}
		if reply == "" {
			reply = allItemsOK(body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(reply))
	}
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.requests
}

func (r *recorder) allBodies() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.bodies...)
}

func readBody(req *http.Request) string {
	var reader io.Reader = req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(req.Body)
		if err != nil {
			return ""
		}
		defer zr.Close()
		reader = zr
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		return ""
	}
	return string(raw)
}

func allItemsOK(body string) string {
	var items []string
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		if line == "" {
			continue
		}
		var action map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &action); err != nil {
			continue
		}
		for name := range action {
			switch name {
			case "index", "create", "update":
				items = append(items, `{"index":{"status":201}}`)
			case "delete":
				items = append(items, `{"delete":{"status":200}}`)
			}
		}
	}
	return fmt.Sprintf(`{"took":3,"errors":false,"items":[%s]}`, strings.Join(items, ","))
}

func newTestSink(t *testing.T, rec *recorder, tune func(*Options)) *Sink {
	t.Helper()
	server := httptest.NewServer(rec.handler())
	t.Cleanup(server.Close)
	opts := Options{
		Addresses:   []string{server.URL},
		Index:       "orders-v1",
		Workers:     1,
		BaseBackoff: time.Millisecond,
		MaxAttempts: 4,
	}
	if tune != nil {
		tune(&opts)
	}
	s, err := New(opts)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func upsert(key string, version uint64) cdc.Doc {
	return cdc.Doc{Key: key, Version: version, Body: []byte(`{"id":` + key + `}`)}
}

func tombstone(key string, version uint64) cdc.Doc {
	return cdc.Doc{Key: key, Version: version, Deleted: true}
}

func TestWriteSendsBulkWithExternalVersioning(t *testing.T) {
	rec := &recorder{}
	s := newTestSink(t, rec, nil)
	res, err := s.Write(context.Background(), []cdc.Doc{upsert("42", 1000)})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if res.Applied != 1 {
		t.Errorf("applied = %d, want 1", res.Applied)
	}
	body := rec.allBodies()[0]
	if !strings.Contains(body, `"version_type":"external"`) {
		t.Errorf("bulk action lacks external versioning: %s", body)
	}
	if !strings.Contains(body, `"version":1000`) {
		t.Errorf("bulk action lacks the document version: %s", body)
	}
	if !strings.Contains(body, `"_id":"42"`) {
		t.Errorf("bulk action lacks the document id: %s", body)
	}
	if !strings.Contains(body, `"_index":"orders-v1"`) {
		t.Errorf("bulk action lacks the index: %s", body)
	}
}

// The body must be newline-delimited JSON with a trailing newline, or
// Elasticsearch rejects the request outright.
func TestBulkBodyIsNDJSON(t *testing.T) {
	rec := &recorder{}
	s := newTestSink(t, rec, nil)
	if _, err := s.Write(context.Background(), []cdc.Doc{upsert("1", 1), upsert("2", 2)}); err != nil {
		t.Fatalf("write: %v", err)
	}
	body := rec.allBodies()[0]
	if !strings.HasSuffix(body, "\n") {
		t.Error("bulk body must end with a newline")
	}
	lines := strings.Split(strings.TrimSuffix(body, "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected 4 lines for 2 documents, got %d: %q", len(lines), body)
	}
	for i, line := range lines {
		if !json.Valid([]byte(line)) {
			t.Errorf("line %d is not valid JSON: %s", i, line)
		}
	}
}

// A delete is a versioned action too, so a replayed older update cannot resurrect
// the document.
func TestDeleteIsVersioned(t *testing.T) {
	rec := &recorder{}
	s := newTestSink(t, rec, nil)
	if _, err := s.Write(context.Background(), []cdc.Doc{tombstone("42", 2000)}); err != nil {
		t.Fatalf("write: %v", err)
	}
	body := rec.allBodies()[0]
	if !strings.Contains(body, `"delete":`) {
		t.Errorf("expected a delete action: %s", body)
	}
	if !strings.Contains(body, `"version":2000`) || !strings.Contains(body, `"version_type":"external"`) {
		t.Errorf("delete must carry the version: %s", body)
	}
	if lines := strings.Split(strings.TrimSuffix(body, "\n"), "\n"); len(lines) != 1 {
		t.Errorf("expected a single line for a delete, got %d", len(lines))
	}
}

// A 409 means the index already holds an equal or newer version. That is the
// expected outcome of replaying a batch, so it is counted, not failed.
func TestVersionConflictIsCountedAsStale(t *testing.T) {
	rec := &recorder{respond: func(int, string) (int, string) {
		return 200, `{"took":1,"errors":true,"items":[
			{"index":{"status":409,"error":{"type":"version_conflict_engine_exception","reason":"current version is newer"}}},
			{"index":{"status":201}}
		]}`
	}}
	s := newTestSink(t, rec, nil)
	res, err := s.Write(context.Background(), []cdc.Doc{upsert("42", 1), upsert("43", 2)})
	if err != nil {
		t.Fatalf("a version conflict must not fail the batch: %v", err)
	}
	if res.Stale != 1 {
		t.Errorf("stale = %d, want 1", res.Stale)
	}
	if res.Applied != 1 {
		t.Errorf("applied = %d, want 1", res.Applied)
	}
	if len(res.Rejected) != 0 {
		t.Errorf("a conflict is not a rejection: %v", res.Rejected)
	}
}

// A mapping conflict will fail identically forever, so it is reported per document
// for the dead letter queue rather than retried.
func TestMappingFailureIsRejectedNotRetried(t *testing.T) {
	rec := &recorder{respond: func(int, string) (int, string) {
		return 200, `{"took":1,"errors":true,"items":[
			{"index":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [total] of type [long]"}}},
			{"index":{"status":201}}
		]}`
	}}
	s := newTestSink(t, rec, nil)
	res, err := s.Write(context.Background(), []cdc.Doc{upsert("42", 1), upsert("43", 2)})
	if err != nil {
		t.Fatalf("one bad document must not fail the batch: %v", err)
	}
	if len(res.Rejected) != 1 {
		t.Fatalf("expected 1 rejection, got %d", len(res.Rejected))
	}
	if res.Rejected[0].Doc.Key != "42" {
		t.Errorf("rejection names the wrong document: %+v", res.Rejected[0])
	}
	if !strings.Contains(res.Rejected[0].Reason, "mapper_parsing_exception") {
		t.Errorf("rejection should carry the reason: %q", res.Rejected[0].Reason)
	}
	if res.Applied != 1 {
		t.Errorf("applied = %d, want 1", res.Applied)
	}
	if rec.count() != 1 {
		t.Errorf("a permanent failure must not be retried, got %d requests", rec.count())
	}
}

// A 429 is back pressure: the cluster is asking us to slow down, and the documents
// are still unwritten.
func TestRejectedExecutionIsRetried(t *testing.T) {
	rec := &recorder{respond: func(attempt int, body string) (int, string) {
		if attempt == 1 {
			return 429, `{"error":{"type":"es_rejected_execution_exception"},"status":429}`
		}
		return 200, allItemsOK(body)
	}}
	s := newTestSink(t, rec, nil)
	res, err := s.Write(context.Background(), []cdc.Doc{upsert("42", 1)})
	if err != nil {
		t.Fatalf("expected the retry to succeed: %v", err)
	}
	if res.Applied != 1 {
		t.Errorf("applied = %d, want 1", res.Applied)
	}
	if rec.count() != 2 {
		t.Errorf("expected 2 attempts, got %d", rec.count())
	}
}

// Per-item 429s mean those documents were not written, so they are retried rather
// than counted as applied.
func TestPerItemRejectionIsRetried(t *testing.T) {
	rec := &recorder{respond: func(attempt int, body string) (int, string) {
		if attempt == 1 {
			return 200, `{"took":1,"errors":true,"items":[
				{"index":{"status":429,"error":{"type":"es_rejected_execution_exception","reason":"queue full"}}},
				{"index":{"status":201}}
			]}`
		}
		return 200, `{"took":1,"errors":false,"items":[{"index":{"status":201}}]}`
	}}
	s := newTestSink(t, rec, nil)
	res, err := s.Write(context.Background(), []cdc.Doc{upsert("42", 1), upsert("43", 2)})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if res.Applied != 2 {
		t.Errorf("applied = %d, want 2 once the retry succeeded", res.Applied)
	}
	second := rec.allBodies()[1]
	if strings.Contains(second, `"_id":"43"`) {
		t.Errorf("the retry resent an already-applied document: %s", second)
	}
	if !strings.Contains(second, `"_id":"42"`) {
		t.Errorf("the retry omitted the failed document: %s", second)
	}
}

func TestServerErrorIsRetriedThenFails(t *testing.T) {
	rec := &recorder{respond: func(int, string) (int, string) {
		return 503, `{"error":"unavailable"}`
	}}
	s := newTestSink(t, rec, func(o *Options) { o.MaxAttempts = 3 })
	_, err := s.Write(context.Background(), []cdc.Doc{upsert("42", 1)})
	if err == nil {
		t.Fatal("expected a persistent 5xx to fail the batch so the checkpoint does not advance")
	}
	if rec.count() != 3 {
		t.Errorf("expected 3 attempts, got %d", rec.count())
	}
}

// A 400 on the whole request means the request itself is malformed, which retrying
// cannot fix.
func TestRequestLevelBadRequestIsNotRetried(t *testing.T) {
	rec := &recorder{respond: func(int, string) (int, string) {
		return 400, `{"error":{"type":"illegal_argument_exception"}}`
	}}
	s := newTestSink(t, rec, nil)
	if _, err := s.Write(context.Background(), []cdc.Doc{upsert("42", 1)}); err == nil {
		t.Fatal("expected a malformed request to fail")
	}
	if rec.count() != 1 {
		t.Errorf("expected no retry of a malformed request, got %d attempts", rec.count())
	}
}

func TestUnauthorizedIsNotRetried(t *testing.T) {
	rec := &recorder{respond: func(int, string) (int, string) {
		return 401, `{"error":"unauthorized"}`
	}}
	s := newTestSink(t, rec, nil)
	if _, err := s.Write(context.Background(), []cdc.Doc{upsert("42", 1)}); err == nil {
		t.Fatal("expected an authentication failure to fail the batch")
	}
	if rec.count() != 1 {
		t.Errorf("retrying a credential problem cannot help, got %d attempts", rec.count())
	}
}

func TestWriteOfEmptyBatchDoesNothing(t *testing.T) {
	rec := &recorder{}
	s := newTestSink(t, rec, nil)
	res, err := s.Write(context.Background(), nil)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if res.Total() != 0 {
		t.Errorf("expected an empty result, got %+v", res)
	}
	if rec.count() != 0 {
		t.Error("an empty batch must not produce a request")
	}
}

// Every document has to be accounted for, or a silent drop would look like success
// and the checkpoint would advance past data that was never written.
func TestResultAccountsForEveryDocument(t *testing.T) {
	rec := &recorder{respond: func(int, string) (int, string) {
		return 200, `{"took":1,"errors":true,"items":[
			{"index":{"status":201}},
			{"index":{"status":409}},
			{"index":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"bad"}}}
		]}`
	}}
	s := newTestSink(t, rec, nil)
	docs := []cdc.Doc{upsert("1", 1), upsert("2", 2), upsert("3", 3)}
	res, err := s.Write(context.Background(), docs)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if res.Total() != len(docs) {
		t.Fatalf("result accounts for %d of %d documents: %+v", res.Total(), len(docs), res)
	}
}

// A response with fewer items than the batch cannot be interpreted, and assuming
// success would advance the checkpoint past unwritten documents.
func TestTruncatedResponseIsAnError(t *testing.T) {
	rec := &recorder{respond: func(int, string) (int, string) {
		return 200, `{"took":1,"errors":false,"items":[{"index":{"status":201}}]}`
	}}
	s := newTestSink(t, rec, nil)
	if _, err := s.Write(context.Background(), []cdc.Doc{upsert("1", 1), upsert("2", 2)}); err == nil {
		t.Fatal("expected a mismatched item count to fail rather than be assumed successful")
	}
}

func TestContextCancellationStopsRetrying(t *testing.T) {
	rec := &recorder{respond: func(int, string) (int, string) {
		return 503, `{"error":"unavailable"}`
	}}
	s := newTestSink(t, rec, func(o *Options) {
		o.MaxAttempts = 100
		o.BaseBackoff = 50 * time.Millisecond
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := s.Write(ctx, []cdc.Doc{upsert("42", 1)}); err == nil {
		t.Fatal("expected an error when the context expires")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("retrying continued past the deadline, took %v", elapsed)
	}
}

func TestNewRejectsIncompleteOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{"no addresses", Options{Index: "i"}},
		{"no index", Options{Addresses: []string{"http://localhost:9200"}}},
		{"negative workers", Options{Addresses: []string{"http://x"}, Index: "i", Workers: -1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.opts); err == nil {
				t.Fatalf("expected options %+v to be rejected", tc.opts)
			}
		})
	}
}

// Writes go to a concrete index while readers use an alias, which is what makes a
// rebuild a single atomic alias move.
func TestWritesTargetTheConcreteIndexNotTheAlias(t *testing.T) {
	rec := &recorder{}
	s := newTestSink(t, rec, func(o *Options) {
		o.Index = "orders-v2"
		o.Alias = "orders"
	})
	if _, err := s.Write(context.Background(), []cdc.Doc{upsert("42", 1)}); err != nil {
		t.Fatalf("write: %v", err)
	}
	body := rec.allBodies()[0]
	if !strings.Contains(body, `"_index":"orders-v2"`) {
		t.Errorf("expected the concrete index: %s", body)
	}
	if strings.Contains(body, `"_index":"orders"`) {
		t.Errorf("writes must not target the alias: %s", body)
	}
}

// Compression is worth enabling in production, where bulk bodies are large and
// mostly text, so the path must be exercised rather than assumed.
func TestCompressedBodyIsSentAndUnderstood(t *testing.T) {
	rec := &recorder{}
	s := newTestSink(t, rec, func(o *Options) { o.Compress = true })
	res, err := s.Write(context.Background(), []cdc.Doc{upsert("42", 1), upsert("43", 2)})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if res.Applied != 2 {
		t.Fatalf("applied = %d, want 2", res.Applied)
	}
	if body := rec.allBodies()[0]; !strings.Contains(body, `"_id":"42"`) {
		t.Fatalf("compressed body did not round trip: %q", body)
	}
}
