package clickhouse

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
)

// recorder is a stub ClickHouse: it records the request bodies and query parameters it
// receives, and replies with whatever the test queued.
type recorder struct {
	mu       sync.Mutex
	bodies   []string
	queries  []url.Values
	requests int

	respond func(attempt int, body string) (int, string)
}

func (r *recorder) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		body := readBody(req)

		r.mu.Lock()
		r.requests++
		attempt := r.requests
		r.bodies = append(r.bodies, body)
		r.queries = append(r.queries, req.URL.Query())
		r.mu.Unlock()

		status, reply := http.StatusOK, ""
		if r.respond != nil {
			status, reply = r.respond(attempt, body)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(reply))
	}
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

func (r *recorder) lastQuery() url.Values {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.queries) == 0 {
		return nil
	}
	return r.queries[len(r.queries)-1]
}

func newTestSink(t *testing.T, rec *recorder, tune func(*Options)) *Sink {
	t.Helper()

	server := httptest.NewServer(rec.handler())
	t.Cleanup(server.Close)

	opts := Options{
		DSN:         server.URL + "/?database=analytics",
		Table:       "orders",
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

func row(key string, version uint64) cdc.Doc {
	return cdc.Doc{Key: key, Version: version, Body: []byte(`{"id":` + key + `,"status":"paid"}`)}
}

func tombstone(key string, version uint64) cdc.Doc {
	return cdc.Doc{Key: key, Version: version, Deleted: true, Body: []byte(`{"id":` + key + `}`)}
}

// rows parses a JSONEachRow body.
func rows(t *testing.T, body string) []map[string]any {
	t.Helper()

	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		if line == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("line is not valid JSON: %q: %v", line, err)
		}
		out = append(out, row)
	}
	return out
}

func TestWriteInsertsRowsWithReplicationColumns(t *testing.T) {
	rec := &recorder{}
	s := newTestSink(t, rec, nil)

	res, err := s.Write(context.Background(), []cdc.Doc{row("1", 1000), row("2", 1001)})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if res.Applied != 2 {
		t.Errorf("applied = %d, want 2", res.Applied)
	}

	parsed := rows(t, rec.allBodies()[0])
	if len(parsed) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(parsed))
	}
	// The engine compares these to decide which copy of a row survives, so a row
	// without them would never be deduplicated.
	if parsed[0]["_version"] != float64(1000) {
		t.Errorf("_version = %v, want 1000", parsed[0]["_version"])
	}
	if parsed[0]["_is_deleted"] != float64(0) {
		t.Errorf("_is_deleted = %v, want 0", parsed[0]["_is_deleted"])
	}
	// The row's own values survive the splice.
	if parsed[0]["status"] != "paid" {
		t.Errorf("status = %v, want paid", parsed[0]["status"])
	}
}

// ClickHouse has no delete: a removed row is superseded by a tombstone carrying its
// key, which the engine drops during a merge.
func TestDeleteBecomesATombstoneRow(t *testing.T) {
	rec := &recorder{}
	s := newTestSink(t, rec, nil)

	if _, err := s.Write(context.Background(), []cdc.Doc{tombstone("42", 2000)}); err != nil {
		t.Fatalf("write: %v", err)
	}

	parsed := rows(t, rec.allBodies()[0])
	if len(parsed) != 1 {
		t.Fatalf("expected 1 row, got %d", len(parsed))
	}
	if parsed[0]["_is_deleted"] != float64(1) {
		t.Errorf("_is_deleted = %v, want 1", parsed[0]["_is_deleted"])
	}
	if parsed[0]["id"] != float64(42) {
		t.Errorf("a tombstone must carry the key, got %v", parsed[0]["id"])
	}
	if parsed[0]["_version"] != float64(2000) {
		t.Errorf("_version = %v, want 2000", parsed[0]["_version"])
	}
}

func TestBodyIsJSONEachRow(t *testing.T) {
	rec := &recorder{}
	s := newTestSink(t, rec, nil)

	if _, err := s.Write(context.Background(), []cdc.Doc{row("1", 1), row("2", 2), row("3", 3)}); err != nil {
		t.Fatalf("write: %v", err)
	}

	body := rec.allBodies()[0]
	lines := strings.Split(strings.TrimSuffix(body, "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected one line per row, got %d: %q", len(lines), body)
	}
	if !strings.HasSuffix(body, "\n") {
		t.Error("the body should end with a newline")
	}

	query := rec.lastQuery()
	if !strings.Contains(query.Get("query"), "INSERT INTO analytics.orders FORMAT JSONEachRow") {
		t.Errorf("statement is wrong: %q", query.Get("query"))
	}
	// changeflow does its own batching and needs the rows stored before it records a
	// position, so server-side buffering is switched off.
	if query.Get("async_insert") != "0" {
		t.Errorf("async_insert = %q, want 0", query.Get("async_insert"))
	}
	// Lets the server discard an identical retry rather than create redundant parts.
	if query.Get("insert_deduplication_token") == "" {
		t.Error("expected a deduplication token")
	}
}

// An empty body would be a row with only replication columns, which is meaningless and
// would insert a tombstone for no key.
func TestDocumentWithoutABodyIsRefused(t *testing.T) {
	rec := &recorder{}
	s := newTestSink(t, rec, nil)

	_, err := s.Write(context.Background(), []cdc.Doc{{Key: "1", Version: 1}})
	if err == nil {
		t.Fatal("expected a document with no body to be refused")
	}
	if rec.count() != 0 {
		t.Error("nothing should have been sent")
	}
}

func TestServerErrorIsRetriedThenFails(t *testing.T) {
	rec := &recorder{respond: func(int, string) (int, string) {
		return http.StatusInternalServerError, "DB::Exception: too many parts"
	}}
	s := newTestSink(t, rec, func(o *Options) { o.MaxAttempts = 3 })

	if _, err := s.Write(context.Background(), []cdc.Doc{row("1", 1)}); err == nil {
		t.Fatal("expected a persistent failure to fail the batch so the position does not advance")
	}
	if rec.count() != 3 {
		t.Errorf("expected 3 attempts, got %d", rec.count())
	}
}

func TestTooManyRequestsIsRetried(t *testing.T) {
	rec := &recorder{respond: func(attempt int, _ string) (int, string) {
		if attempt == 1 {
			return http.StatusTooManyRequests, "too many simultaneous queries"
		}
		return http.StatusOK, ""
	}}
	s := newTestSink(t, rec, nil)

	res, err := s.Write(context.Background(), []cdc.Doc{row("1", 1)})
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

// JSONEachRow reports no per-row outcome, so a rejection applies to the whole request.
// Halving finds the row responsible, so one bad value does not discard the batch around
// it.
func TestARejectedRowIsIsolatedFromTheBatch(t *testing.T) {
	const badKey = "77"

	rec := &recorder{respond: func(_ int, body string) (int, string) {
		if strings.Contains(body, `"id":`+badKey+`,`) {
			return http.StatusBadRequest, "DB::Exception: Cannot parse input: expected UInt64"
		}
		return http.StatusOK, ""
	}}
	s := newTestSink(t, rec, nil)

	var docs []cdc.Doc
	for i := 70; i < 80; i++ {
		docs = append(docs, row(itoa(i), uint64(1000+i)))
	}

	res, err := s.Write(context.Background(), docs)
	if err != nil {
		t.Fatalf("one bad row must not fail the batch: %v", err)
	}

	if len(res.Rejected) != 1 {
		t.Fatalf("expected exactly 1 rejection, got %d", len(res.Rejected))
	}
	if res.Rejected[0].Doc.Key != badKey {
		t.Errorf("rejected the wrong row: %s", res.Rejected[0].Doc.Key)
	}
	if !strings.Contains(res.Rejected[0].Reason, "Cannot parse input") {
		t.Errorf("rejection should carry the server's reason: %q", res.Rejected[0].Reason)
	}
	// Everything else must still have been applied.
	if res.Applied != len(docs)-1 {
		t.Errorf("applied = %d, want %d", res.Applied, len(docs)-1)
	}
	if res.Total() != len(docs) {
		t.Errorf("result accounts for %d of %d rows", res.Total(), len(docs))
	}
}

// A systematic rejection must not turn one batch into thousands of requests.
func TestIsolationIsBounded(t *testing.T) {
	rec := &recorder{respond: func(int, string) (int, string) {
		return http.StatusBadRequest, "DB::Exception: NO_SUCH_COLUMN_IN_TABLE"
	}}
	s := newTestSink(t, rec, nil)

	var docs []cdc.Doc
	for i := 0; i < 64; i++ {
		docs = append(docs, row(itoa(i), uint64(i+1)))
	}

	res, err := s.Write(context.Background(), docs)
	// Every row is rejected individually, or the narrowing gives up; either way the
	// request count stays proportional rather than exploding.
	if err == nil && res.Total() != len(docs) {
		t.Errorf("result accounts for %d of %d rows", res.Total(), len(docs))
	}
	if rec.count() > 4*len(docs) {
		t.Errorf("narrowing made %d requests for %d rows", rec.count(), len(docs))
	}
}

func TestWriteOfEmptyBatchDoesNothing(t *testing.T) {
	rec := &recorder{}
	s := newTestSink(t, rec, nil)

	res, err := s.Write(context.Background(), nil)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if res.Total() != 0 || rec.count() != 0 {
		t.Errorf("expected nothing to happen, got %+v and %d requests", res, rec.count())
	}
}

func TestContextCancellationStopsRetrying(t *testing.T) {
	rec := &recorder{respond: func(int, string) (int, string) {
		return http.StatusInternalServerError, "unavailable"
	}}
	s := newTestSink(t, rec, func(o *Options) {
		o.MaxAttempts = 100
		o.BaseBackoff = 50 * time.Millisecond
	})

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	start := time.Now()
	if _, err := s.Write(ctx, []cdc.Doc{row("1", 1)}); err == nil {
		t.Fatal("expected an error when the context expires")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("retrying continued past the deadline, took %v", elapsed)
	}
}

func TestCompressedBodyIsSentAndUnderstood(t *testing.T) {
	rec := &recorder{}
	s := newTestSink(t, rec, func(o *Options) { o.Compress = true })

	if _, err := s.Write(context.Background(), []cdc.Doc{row("1", 1)}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if body := rec.allBodies()[0]; !strings.Contains(body, `"id":1`) {
		t.Fatalf("compressed body did not round trip: %q", body)
	}
}

// Credentials in a URL end up in logs and error messages, so they travel as headers.
func TestCredentialsAreSentAsHeadersNotInTheURL(t *testing.T) {
	var gotUser, gotKey, gotURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser = r.Header.Get("X-ClickHouse-User")
		gotKey = r.Header.Get("X-ClickHouse-Key")
		gotURL = r.URL.String()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	withCredentials := strings.Replace(server.URL, "http://", "http://writer:secret@", 1)
	s, err := New(Options{DSN: withCredentials + "/?database=analytics", Table: "orders"})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	defer s.Close()

	if _, err := s.Write(context.Background(), []cdc.Doc{row("1", 1)}); err != nil {
		t.Fatalf("write: %v", err)
	}

	if gotUser != "writer" || gotKey != "secret" {
		t.Errorf("credentials were not sent as headers: user=%q key=%q", gotUser, gotKey)
	}
	if strings.Contains(gotURL, "secret") {
		t.Errorf("the password appeared in the request URL: %s", gotURL)
	}
}

func TestTableIsQualifiedFromTheDSNDatabase(t *testing.T) {
	rec := &recorder{}
	s := newTestSink(t, rec, nil)

	if _, err := s.Write(context.Background(), []cdc.Doc{row("1", 1)}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := rec.lastQuery().Get("query"); !strings.Contains(got, "analytics.orders") {
		t.Errorf("statement should qualify the table from the dsn database: %q", got)
	}
}

func TestAlreadyQualifiedTableIsLeftAlone(t *testing.T) {
	rec := &recorder{}
	s := newTestSink(t, rec, func(o *Options) { o.Table = "other.orders" })

	if _, err := s.Write(context.Background(), []cdc.Doc{row("1", 1)}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := rec.lastQuery().Get("query"); !strings.Contains(got, "other.orders") {
		t.Errorf("statement should keep the given qualification: %q", got)
	}
}

func TestNewRejectsIncompleteOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{"no dsn", Options{Table: "orders"}},
		{"no table", Options{DSN: "http://localhost:8123"}},
		{"negative workers", Options{DSN: "http://localhost:8123", Table: "orders", Workers: -1}},
		// The native protocol is a different port and wire format.
		{"native protocol dsn", Options{DSN: "clickhouse://localhost:9000", Table: "orders"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.opts); err == nil {
				t.Fatalf("expected options %+v to be rejected", tc.opts)
			}
		})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}
