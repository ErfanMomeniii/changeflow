package pipeline

import (
	"testing"
	"time"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
)

type fakeClock struct{ at time.Time }

func (c *fakeClock) now() time.Time          { return c.at }
func (c *fakeClock) advance(d time.Duration) { c.at = c.at.Add(d) }

func newTestBatcher(t *testing.T, limits Limits) (*Batcher, *fakeClock) {
	t.Helper()
	clock := &fakeClock{at: time.UnixMilli(1_786_000_000_000)}
	b, err := NewBatcher(limits, clock.now)
	if err != nil {
		t.Fatalf("new batcher: %v", err)
	}
	return b, clock
}

func doc(key string, size int) cdc.Doc {
	body := make([]byte, size)
	for i := range body {
		body[i] = 'x'
	}
	return cdc.Doc{Key: key, Body: body, Version: 1}
}

func keysOf(docs []cdc.Doc) []string {
	out := make([]string, len(docs))
	for i, d := range docs {
		out[i] = d.Key
	}
	return out
}

func TestBatcherHoldsDocsUntilRowLimit(t *testing.T) {
	b, _ := newTestBatcher(t, Limits{MaxRows: 3, MaxBytes: 1 << 20, FlushInterval: time.Second})
	if got := b.Add(doc("1", 1)); got != nil {
		t.Fatalf("expected no flush after 1 of 3, got %d docs", len(got))
	}
	if got := b.Add(doc("2", 1)); got != nil {
		t.Fatalf("expected no flush after 2 of 3, got %d docs", len(got))
	}
	flushed := b.Add(doc("3", 1))
	if len(flushed) != 3 {
		t.Fatalf("expected a flush of 3 docs, got %d", len(flushed))
	}
	if b.Len() != 0 {
		t.Fatalf("expected the batcher to be empty after flushing, holds %d", b.Len())
	}
}

// The byte limit exists to keep a request under what the destination accepts, so a
// batch must not exceed it. The document that would overflow starts the next batch
// rather than being appended to this one.
func TestBatcherFlushesBeforeExceedingByteLimit(t *testing.T) {
	b, _ := newTestBatcher(t, Limits{MaxRows: 1000, MaxBytes: 300, FlushInterval: time.Second})
	if got := b.Add(doc("1", 100)); got != nil {
		t.Fatal("expected no flush well under the byte limit")
	}
	flushed := b.Add(doc("2", 250))
	if len(flushed) != 1 || flushed[0].Key != "1" {
		t.Fatalf("expected only the documents that fit, got %v", keysOf(flushed))
	}
	if b.Len() != 1 {
		t.Fatalf("expected the overflowing document to be held for the next batch, batcher holds %d", b.Len())
	}
	if b.Bytes() > 300 {
		t.Fatalf("pending batch is %d bytes, past the limit", b.Bytes())
	}
}

// Filling exactly to the limit should send, not wait.
func TestBatcherFlushesWhenByteLimitIsReachedExactly(t *testing.T) {
	b, _ := newTestBatcher(t, Limits{MaxRows: 1000, MaxBytes: 202, FlushInterval: time.Second})
	b.Add(doc("1", 100))
	flushed := b.Add(doc("2", 100))
	if len(flushed) != 2 {
		t.Fatalf("expected a flush of both documents at exactly the limit, got %v", keysOf(flushed))
	}
}

// A document larger than the whole byte budget must still be delivered, or it
// would wedge the stream permanently.
func TestBatcherEmitsOversizedDocument(t *testing.T) {
	b, _ := newTestBatcher(t, Limits{MaxRows: 1000, MaxBytes: 100, FlushInterval: time.Second})
	flushed := b.Add(doc("huge", 5000))
	if len(flushed) != 1 {
		t.Fatalf("expected the oversized document to be flushed on its own, got %d docs", len(flushed))
	}
	if b.Len() != 0 {
		t.Fatal("expected nothing left behind")
	}
}

// An oversized document arriving after other docs must not lose them or reorder
// them.
func TestBatcherFlushesPendingBeforeOversizedDocument(t *testing.T) {
	b, _ := newTestBatcher(t, Limits{MaxRows: 1000, MaxBytes: 100, FlushInterval: time.Second})
	b.Add(doc("a", 10))
	b.Add(doc("b", 10))
	flushed := b.Add(doc("huge", 5000))
	if want := []string{"a", "b", "huge"}; len(flushed) != 3 {
		t.Fatalf("got %v, want %v", keysOf(flushed), want)
	}
	if keysOf(flushed)[2] != "huge" {
		t.Fatalf("order changed: %v", keysOf(flushed))
	}
}

// The interval is measured from the first document in the batch, so latency stays
// bounded rather than resetting with every arrival.
func TestBatcherIntervalRunsFromFirstDocument(t *testing.T) {
	b, clock := newTestBatcher(t, Limits{MaxRows: 1000, MaxBytes: 1 << 20, FlushInterval: 500 * time.Millisecond})
	b.Add(doc("1", 1))
	clock.advance(300 * time.Millisecond)
	b.Add(doc("2", 1))
	if b.DueForFlush() {
		t.Fatal("not due yet at 300ms")
	}
	clock.advance(250 * time.Millisecond)
	if !b.DueForFlush() {
		t.Fatal("expected the batch to be due 500ms after its first document")
	}
}

func TestBatcherIsNeverDueWhenEmpty(t *testing.T) {
	b, clock := newTestBatcher(t, Limits{MaxRows: 10, MaxBytes: 1 << 20, FlushInterval: time.Millisecond})
	clock.advance(time.Hour)
	if b.DueForFlush() {
		t.Fatal("an empty batcher must never ask to be flushed")
	}
	if b.Flush() != nil {
		t.Fatal("flushing an empty batcher must return nothing")
	}
}

func TestBatcherResetsIntervalAfterFlush(t *testing.T) {
	b, clock := newTestBatcher(t, Limits{MaxRows: 1000, MaxBytes: 1 << 20, FlushInterval: 500 * time.Millisecond})
	b.Add(doc("1", 1))
	clock.advance(600 * time.Millisecond)
	if !b.DueForFlush() {
		t.Fatal("expected due")
	}
	b.Flush()
	b.Add(doc("2", 1))
	if b.DueForFlush() {
		t.Fatal("the interval must restart with the new batch")
	}
}

// Writes to one key must reach the destination in the order they happened, or a
// delete could be undone by an earlier insert.
func TestBatcherPreservesPerKeyOrder(t *testing.T) {
	b, _ := newTestBatcher(t, Limits{MaxRows: 100, MaxBytes: 1 << 20, FlushInterval: time.Second})
	b.Add(cdc.Doc{Key: "42", Version: 1, Body: []byte(`{"v":1}`)})
	b.Add(cdc.Doc{Key: "99", Version: 2, Body: []byte(`{"v":2}`)})
	b.Add(cdc.Doc{Key: "42", Version: 3, Deleted: true})
	b.Add(cdc.Doc{Key: "42", Version: 4, Body: []byte(`{"v":4}`)})
	flushed := b.Flush()
	var versionsFor42 []uint64
	for _, d := range flushed {
		if d.Key == "42" {
			versionsFor42 = append(versionsFor42, d.Version)
		}
	}
	if len(versionsFor42) != 3 {
		t.Fatalf("expected 3 writes for key 42, got %d", len(versionsFor42))
	}
	for i := 1; i < len(versionsFor42); i++ {
		if versionsFor42[i] <= versionsFor42[i-1] {
			t.Fatalf("writes for one key were reordered: %v", versionsFor42)
		}
	}
}

// The caller keeps the flushed slice while the batcher fills the next one, so the
// two must not share memory.
func TestFlushedBatchIsNotAliasedByLaterAdds(t *testing.T) {
	b, _ := newTestBatcher(t, Limits{MaxRows: 2, MaxBytes: 1 << 20, FlushInterval: time.Second})
	b.Add(doc("a", 1))
	flushed := b.Add(doc("b", 1))
	if len(flushed) != 2 {
		t.Fatalf("expected a flush of 2, got %d", len(flushed))
	}
	before := keysOf(flushed)
	b.Add(doc("c", 1))
	b.Add(doc("d", 1))
	after := keysOf(flushed)
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("the flushed batch changed after later adds: %v then %v", before, after)
		}
	}
}

func TestBatcherReportsSize(t *testing.T) {
	b, _ := newTestBatcher(t, Limits{MaxRows: 100, MaxBytes: 1 << 20, FlushInterval: time.Second})
	b.Add(cdc.Doc{Key: "ab", Body: []byte("12345")})
	if b.Len() != 1 {
		t.Errorf("Len() = %d, want 1", b.Len())
	}
	if b.Bytes() != 7 {
		t.Errorf("Bytes() = %d, want 7", b.Bytes())
	}
}

// A delete carries no body but still occupies a slot and a key's worth of bytes.
func TestBatcherCountsDeletes(t *testing.T) {
	b, _ := newTestBatcher(t, Limits{MaxRows: 2, MaxBytes: 1 << 20, FlushInterval: time.Second})
	b.Add(cdc.Doc{Key: "42", Deleted: true, Version: 1})
	flushed := b.Add(cdc.Doc{Key: "43", Deleted: true, Version: 2})
	if len(flushed) != 2 {
		t.Fatalf("expected deletes to count toward the row limit, got %d", len(flushed))
	}
}

func TestNewBatcherRejectsUnusableLimits(t *testing.T) {
	for _, tc := range []struct {
		name   string
		limits Limits
	}{
		{"no row limit", Limits{MaxRows: 0, MaxBytes: 100, FlushInterval: time.Second}},
		{"no byte limit", Limits{MaxRows: 10, MaxBytes: 0, FlushInterval: time.Second}},
		{"no interval", Limits{MaxRows: 10, MaxBytes: 100, FlushInterval: 0}},
		{"negative interval", Limits{MaxRows: 10, MaxBytes: 100, FlushInterval: -time.Second}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewBatcher(tc.limits, nil); err == nil {
				t.Fatalf("expected limits %+v to be rejected", tc.limits)
			}
		})
	}
}

func TestBatcherFlushReturnsDocsInArrivalOrder(t *testing.T) {
	b, _ := newTestBatcher(t, Limits{MaxRows: 100, MaxBytes: 1 << 20, FlushInterval: time.Second})
	want := []string{"first", "second", "third"}
	for _, k := range want {
		b.Add(doc(k, 1))
	}
	got := keysOf(b.Flush())
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}
