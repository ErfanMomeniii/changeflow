package dlq

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/sink"
)

func newTestWriter(t *testing.T, tune func(*Options)) (*Writer, string) {
	t.Helper()
	dir := t.TempDir()
	opts := Options{Dir: dir, Stream: "orders_to_es"}
	if tune != nil {
		tune(&opts)
	}
	w, err := New(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w, dir
}

func rejection(key, reason string) sink.Rejection {
	return sink.Rejection{
		Doc:    cdc.Doc{Key: key, Version: 1234, Body: []byte(`{"id":1,"secret":"card-4111"}`)},
		Status: 400,
		Reason: reason,
	}
}

func lines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	var out []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if line := strings.TrimSpace(scanner.Text()); line != "" {
			out = append(out, line)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return out
}

func TestRecordWritesOneLinePerRejection(t *testing.T) {
	w, dir := newTestWriter(t, nil)
	err := w.Record(t.Context(), []sink.Rejection{
		rejection("42", "mapper_parsing_exception"),
		rejection("43", "mapper_parsing_exception"),
	})
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	got := lines(t, filepath.Join(dir, "orders_to_es.jsonl"))
	if len(got) != 2 {
		t.Fatalf("expected 2 records, got %d", len(got))
	}
	for i, line := range got {
		if !json.Valid([]byte(line)) {
			t.Errorf("record %d is not valid JSON: %s", i, line)
		}
	}
}

// A record has to carry enough to understand and replay the failure without the
// original event.
func TestRecordCarriesEnoughToDiagnoseAndReplay(t *testing.T) {
	w, dir := newTestWriter(t, nil)
	if err := w.Record(t.Context(), []sink.Rejection{rejection("42", "failed to parse field [total]")}); err != nil {
		t.Fatalf("record: %v", err)
	}
	var rec Record
	if err := json.Unmarshal([]byte(lines(t, filepath.Join(dir, "orders_to_es.jsonl"))[0]), &rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rec.Stream != "orders_to_es" {
		t.Errorf("stream = %q", rec.Stream)
	}
	if rec.Key != "42" {
		t.Errorf("key = %q", rec.Key)
	}
	if rec.Status != 400 {
		t.Errorf("status = %d", rec.Status)
	}
	if !strings.Contains(rec.Reason, "failed to parse field") {
		t.Errorf("reason = %q", rec.Reason)
	}
	if rec.Version != 1234 {
		t.Errorf("version = %d, want the document's version so a replay keeps its ordering", rec.Version)
	}
	if rec.RecordedAt.IsZero() {
		t.Error("record has no timestamp")
	}
}

// Row values can hold personal data, so the payload is withheld unless an operator
// asks for it. A dead letter file is far more likely to be copied around than a
// database is.
func TestPayloadIsWithheldByDefault(t *testing.T) {
	w, dir := newTestWriter(t, nil)
	if err := w.Record(t.Context(), []sink.Rejection{rejection("42", "bad")}); err != nil {
		t.Fatalf("record: %v", err)
	}
	line := lines(t, filepath.Join(dir, "orders_to_es.jsonl"))[0]
	if strings.Contains(line, "card-4111") {
		t.Fatalf("document body leaked into the dead letter file: %s", line)
	}
	var rec Record
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(rec.Body) != 0 {
		t.Errorf("body = %s, want it omitted", rec.Body)
	}
	if rec.BodyBytes == 0 {
		t.Error("expected the body size to be recorded even when its content is not")
	}
}

func TestPayloadIsIncludedWhenRequested(t *testing.T) {
	w, dir := newTestWriter(t, func(o *Options) { o.IncludePayload = true })
	if err := w.Record(t.Context(), []sink.Rejection{rejection("42", "bad")}); err != nil {
		t.Fatalf("record: %v", err)
	}
	line := lines(t, filepath.Join(dir, "orders_to_es.jsonl"))[0]
	if !strings.Contains(line, "card-4111") {
		t.Fatalf("expected the body to be recorded: %s", line)
	}
}

func TestRecordAppendsAcrossCalls(t *testing.T) {
	w, dir := newTestWriter(t, nil)
	for _, key := range []string{"1", "2", "3"} {
		if err := w.Record(t.Context(), []sink.Rejection{rejection(key, "bad")}); err != nil {
			t.Fatalf("record: %v", err)
		}
	}
	if got := lines(t, filepath.Join(dir, "orders_to_es.jsonl")); len(got) != 3 {
		t.Fatalf("expected 3 records, got %d", len(got))
	}
}

// The pipeline advances its position once a rejection is recorded, so the record
// must survive a crash that happens immediately afterwards. Reading the file
// without closing the writer shows the data has left our buffers.
func TestRecordIsDurableBeforeReturning(t *testing.T) {
	w, dir := newTestWriter(t, nil)
	if err := w.Record(t.Context(), []sink.Rejection{rejection("42", "bad")}); err != nil {
		t.Fatalf("record: %v", err)
	}
	if got := lines(t, filepath.Join(dir, "orders_to_es.jsonl")); len(got) != 1 {
		t.Fatalf("record was still buffered after Record returned, found %d lines", len(got))
	}
}

func TestRecordOfNothingDoesNothing(t *testing.T) {
	w, dir := newTestWriter(t, nil)
	if err := w.Record(t.Context(), nil); err != nil {
		t.Fatalf("record: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "orders_to_es.jsonl")); !os.IsNotExist(err) {
		t.Error("an empty record should not create a file")
	}
}

func TestWriterCreatesMissingDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "dlq")
	w, err := New(Options{Dir: dir, Stream: "s"})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	defer w.Close()
	if err := w.Record(t.Context(), []sink.Rejection{rejection("1", "bad")}); err != nil {
		t.Fatalf("record: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "s.jsonl")); err != nil {
		t.Fatalf("expected the file to exist: %v", err)
	}
}

// Without rotation a long-running stream with a persistent mapping problem would
// fill its volume.
func TestFileRotatesWhenItGrowsPastTheLimit(t *testing.T) {
	w, dir := newTestWriter(t, func(o *Options) { o.MaxBytes = 400 })
	for i := 0; i < 20; i++ {
		if err := w.Record(t.Context(), []sink.Rejection{rejection("key", strings.Repeat("x", 60))}); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) < 2 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("expected rotation to produce more than one file, got %v", names)
	}
	var total int
	for _, e := range entries {
		total += len(lines(t, filepath.Join(dir, e.Name())))
	}
	if total != 20 {
		t.Fatalf("expected 20 records across all files, found %d", total)
	}
}

func TestConcurrentRecordsProduceIntactLines(t *testing.T) {
	w, dir := newTestWriter(t, nil)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if err := w.Record(t.Context(), []sink.Rejection{rejection("k", "bad")}); err != nil {
					t.Errorf("record: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	got := lines(t, filepath.Join(dir, "orders_to_es.jsonl"))
	if len(got) != 160 {
		t.Fatalf("expected 160 records, got %d", len(got))
	}
	for i, line := range got {
		if !json.Valid([]byte(line)) {
			t.Fatalf("record %d is torn, so concurrent writes interleaved: %s", i, line)
		}
	}
}

// Replay needs the records back, so reading is part of the contract rather than an
// afterthought.
func TestReadReturnsRecordedFailures(t *testing.T) {
	w, dir := newTestWriter(t, nil)
	if err := w.Record(t.Context(), []sink.Rejection{rejection("42", "one"), rejection("43", "two")}); err != nil {
		t.Fatalf("record: %v", err)
	}
	records, err := Read(filepath.Join(dir, "orders_to_es.jsonl"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0].Key != "42" || records[1].Key != "43" {
		t.Fatalf("records came back in the wrong order: %s then %s", records[0].Key, records[1].Key)
	}
}

func TestReadReportsCorruptLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broken.jsonl")
	if err := os.WriteFile(path, []byte("{\"key\":\"1\"}\nnot json\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := Read(path); err == nil {
		t.Fatal("expected a corrupt line to be reported rather than skipped silently")
	}
}

func TestNewRejectsIncompleteOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{"no directory", Options{Stream: "s"}},
		{"no stream", Options{Dir: t.TempDir()}},
		{"stream escaping the directory", Options{Dir: t.TempDir(), Stream: "../../etc/passwd"}},
		{"stream with a separator", Options{Dir: t.TempDir(), Stream: "a/b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.opts); err == nil {
				t.Fatalf("expected options %+v to be rejected", tc.opts)
			}
		})
	}
}

// Any count above zero means documents that are not in the destination, so it has to
// include the files rotation moved aside.
func TestCountIncludesRotatedFiles(t *testing.T) {
	stamp := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	w, dir := newTestWriter(t, func(o *Options) {
		o.MaxBytes = 1
		o.Now = func() time.Time { return stamp }
	})
	if err := w.Record(t.Context(), []sink.Rejection{rejection("1", "refused"), rejection("2", "refused")}); err != nil {
		t.Fatalf("record: %v", err)
	}
	stamp = stamp.Add(time.Second)
	if err := w.Record(t.Context(), []sink.Rejection{rejection("3", "refused")}); err != nil {
		t.Fatalf("record: %v", err)
	}
	files, err := Files(dir, "orders_to_es")
	if err != nil {
		t.Fatalf("files: %v", err)
	}
	if len(files) < 2 {
		t.Fatalf("found %d file(s), expected the rotated ones too: %v", len(files), files)
	}
	count, err := Count(dir, "orders_to_es")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 3 {
		t.Errorf("count = %d, want every refused document across rotated files", count)
	}
}

func TestCountIsPerStream(t *testing.T) {
	dir := t.TempDir()
	for stream, records := range map[string]int{"orders_to_es": 2, "users_to_es": 1} {
		w, err := New(Options{Dir: dir, Stream: stream})
		if err != nil {
			t.Fatalf("new writer: %v", err)
		}
		rejections := make([]sink.Rejection, records)
		for i := range rejections {
			rejections[i] = rejection("k", "refused")
		}
		if err := w.Record(t.Context(), rejections); err != nil {
			t.Fatalf("record: %v", err)
		}
		w.Close()
	}
	if got, err := Count(dir, "orders_to_es"); err != nil || got != 2 {
		t.Errorf("count for one stream = %d, %v; want 2", got, err)
	}
	if got, err := Count(dir, ""); err != nil || got != 3 {
		t.Errorf("count for every stream = %d, %v; want 3", got, err)
	}
}

// The usual case, and the one worth being sure about: nothing refused, no directory yet,
// and no error to explain away.
func TestCountIsZeroWhenNothingWasRefused(t *testing.T) {
	count, err := Count(filepath.Join(t.TempDir(), "never-created"), "orders_to_es")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
}

// A stream name reaches a glob pattern, and one stream's count must not include another's
// merely because the names share a prefix.
func TestCountDoesNotCountAnotherStreamWithASharedPrefix(t *testing.T) {
	dir := t.TempDir()
	for _, stream := range []string{"orders", "orders_archive"} {
		w, err := New(Options{Dir: dir, Stream: stream})
		if err != nil {
			t.Fatalf("new writer: %v", err)
		}
		if err := w.Record(t.Context(), []sink.Rejection{rejection("1", "refused")}); err != nil {
			t.Fatalf("record: %v", err)
		}
		w.Close()
	}
	if got, err := Count(dir, "orders"); err != nil || got != 1 {
		t.Errorf("count = %d, %v; want only this stream's single record", got, err)
	}
}
