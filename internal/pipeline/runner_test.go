package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/checkpoint"
	"github.com/ErfanMomeniii/changeflow/internal/config"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
	"github.com/ErfanMomeniii/changeflow/internal/sink"
)

// call records one Write the runner made.
type call struct {
	docs []cdc.Doc
}

// stubSink records batches and replies with whatever the test queued.
type stubSink struct {
	mu    sync.Mutex
	calls []call

	// reply decides each call's outcome, counting from 1.
	reply func(attempt int, docs []cdc.Doc) (sink.Result, error)
	// onWrite runs before the reply, for tests that need to observe ordering.
	onWrite func(docs []cdc.Doc)
}

func (s *stubSink) Write(_ context.Context, docs []cdc.Doc) (sink.Result, error) {
	s.mu.Lock()
	s.calls = append(s.calls, call{docs: append([]cdc.Doc(nil), docs...)})
	n := len(s.calls)
	s.mu.Unlock()

	if s.onWrite != nil {
		s.onWrite(docs)
	}
	if s.reply != nil {
		return s.reply(n, docs)
	}
	return sink.Result{Applied: len(docs)}, nil
}

func (s *stubSink) Close() error { return nil }

func (s *stubSink) written() []cdc.Doc {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []cdc.Doc
	for _, c := range s.calls {
		out = append(out, c.docs...)
	}
	return out
}

func (s *stubSink) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

// recordingDLQ captures documents the runner gave up on.
type recordingDLQ struct {
	mu    sync.Mutex
	items []sink.Rejection
}

func (d *recordingDLQ) Record(_ context.Context, rejections []sink.Rejection) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.items = append(d.items, rejections...)
	return nil
}

func (d *recordingDLQ) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.items)
}

func runnerMeta() *schema.TableMeta {
	m := &schema.TableMeta{
		Schema: "shop", Table: "orders",
		Columns: []schema.Column{
			{Name: "id", Position: 0, DataType: "bigint", ColumnType: "bigint unsigned", Unsigned: true},
			{Name: "status", Position: 1, DataType: "varchar", ColumnType: "varchar(16)"},
		},
		PrimaryKey: []string{"id"},
	}
	return m
}

func runnerPlan(t *testing.T) *Plan {
	t.Helper()
	p, err := Compile(runnerMeta(), config.Mapping{Key: []string{"id"}}, DialectElasticsearch, time.UTC, "null")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return p
}

func event(seq uint64, id uint64, status, gtid string) cdc.ChangeEvent {
	return cdc.ChangeEvent{
		Meta:      runnerMeta(),
		Op:        cdc.OpInsert,
		After:     cdc.Row{id, status},
		Seq:       seq,
		GTID:      gtid,
		Timestamp: time.UnixMilli(1_786_000_000_000),
	}
}

type runnerHarness struct {
	runner *Runner
	events chan cdc.ChangeEvent
	sink   *stubSink
	dlq    *recordingDLQ
	store  *checkpoint.MemoryStore
	clock  *fakeClock
}

func newRunner(t *testing.T, tune func(*RunnerOptions)) *runnerHarness {
	t.Helper()

	h := &runnerHarness{
		events: make(chan cdc.ChangeEvent, 64),
		sink:   &stubSink{},
		dlq:    &recordingDLQ{},
		store:  checkpoint.NewMemoryStore(),
		clock:  &fakeClock{at: time.UnixMilli(1_786_000_000_000)},
	}

	opts := RunnerOptions{
		// These tests provoke refusals and shutdowns on purpose, so their logs are
		// noise rather than signal.
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Stream: "orders_to_es",
		Plan:   runnerPlan(t),
		Sink:   h.sink,
		DLQ:    h.dlq,
		Store:  h.store,
		Limits: Limits{MaxRows: 2, MaxBytes: 1 << 20, FlushInterval: time.Hour},
		Now:    h.clock.now,
	}
	if tune != nil {
		tune(&opts)
	}

	r, err := NewRunner(opts)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	h.runner = r
	return h
}

// run drives the runner to completion over a closed channel.
func (h *runnerHarness) run(t *testing.T, events ...cdc.ChangeEvent) error {
	t.Helper()
	for _, ev := range events {
		h.events <- ev
	}
	close(h.events)
	return h.runner.Run(context.Background(), h.events)
}

func (h *runnerHarness) checkpoint(t *testing.T) checkpoint.Checkpoint {
	t.Helper()
	cp, err := h.store.Load(context.Background(), "orders_to_es")
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	return cp
}

func TestRunnerWritesBatchesAndCheckpoints(t *testing.T) {
	h := newRunner(t, nil)

	if err := h.run(t, event(1, 1, "paid", "uuid:1"), event(2, 2, "paid", "uuid:2")); err != nil {
		t.Fatalf("run: %v", err)
	}

	if got := len(h.sink.written()); got != 2 {
		t.Fatalf("expected 2 documents written, got %d", got)
	}
	if cp := h.checkpoint(t); cp.GTIDSet != "uuid:2" {
		t.Fatalf("checkpoint = %q, want the last acknowledged position uuid:2", cp.GTIDSet)
	}
}

// The rule the whole design rests on: a position is durable only once the sink has
// accepted the documents that reached it. Reversing this loses data on a crash.
func TestCheckpointIsWrittenOnlyAfterTheSinkAcknowledges(t *testing.T) {
	h := newRunner(t, nil)

	var checkpointAtWriteTime string
	h.sink.onWrite = func([]cdc.Doc) {
		// Observed from inside the sink call, before it returns.
		cp, err := h.store.Load(context.Background(), "orders_to_es")
		if err == nil {
			checkpointAtWriteTime = cp.GTIDSet
		}
	}

	if err := h.run(t, event(1, 1, "paid", "uuid:1"), event(2, 2, "paid", "uuid:2")); err != nil {
		t.Fatalf("run: %v", err)
	}

	if checkpointAtWriteTime == "uuid:2" {
		t.Fatal("the checkpoint was advanced before the sink acknowledged the batch")
	}
	if cp := h.checkpoint(t); cp.GTIDSet != "uuid:2" {
		t.Fatalf("checkpoint should advance after the acknowledgement, got %q", cp.GTIDSet)
	}
}

// A failed write must leave the position untouched, so a restart replays the batch
// rather than skipping it.
func TestFailedWriteDoesNotAdvanceCheckpoint(t *testing.T) {
	h := newRunner(t, nil)
	h.sink.reply = func(int, []cdc.Doc) (sink.Result, error) {
		return sink.Result{}, errors.New("elasticsearch unavailable")
	}

	err := h.run(t, event(1, 1, "paid", "uuid:1"), event(2, 2, "paid", "uuid:2"))
	if err == nil {
		t.Fatal("expected the run to fail when the sink cannot be written")
	}

	if _, loadErr := h.store.Load(context.Background(), "orders_to_es"); loadErr == nil {
		cp := h.checkpoint(t)
		if cp.GTIDSet != "" {
			t.Fatalf("checkpoint advanced to %q despite the write failing", cp.GTIDSet)
		}
	}
}

// Documents the destination refuses permanently would otherwise stall the stream
// forever, so they are recorded and the batch proceeds.
func TestRejectedDocumentsGoToTheDeadLetterQueue(t *testing.T) {
	h := newRunner(t, nil)
	h.sink.reply = func(_ int, docs []cdc.Doc) (sink.Result, error) {
		return sink.Result{
			Applied: len(docs) - 1,
			Rejected: []sink.Rejection{{
				Doc: docs[0], Status: 400, Reason: "mapper_parsing_exception",
			}},
		}, nil
	}

	if err := h.run(t, event(1, 1, "paid", "uuid:1"), event(2, 2, "paid", "uuid:2")); err != nil {
		t.Fatalf("a rejected document must not fail the run: %v", err)
	}

	if h.dlq.count() != 1 {
		t.Fatalf("expected 1 dead letter, got %d", h.dlq.count())
	}
	if cp := h.checkpoint(t); cp.GTIDSet != "uuid:2" {
		t.Fatal("the position should still advance once the rest of the batch is applied")
	}
}

// If rejected documents could not be recorded, advancing the position would lose
// them silently, so the run fails instead.
func TestRunFailsWhenDeadLetterQueueCannotAccept(t *testing.T) {
	h := newRunner(t, func(o *RunnerOptions) { o.DLQ = failingDLQ{} })
	h.sink.reply = func(_ int, docs []cdc.Doc) (sink.Result, error) {
		return sink.Result{Rejected: []sink.Rejection{{Doc: docs[0], Status: 400, Reason: "bad"}}}, nil
	}

	if err := h.run(t, event(1, 1, "paid", "uuid:1"), event(2, 2, "paid", "uuid:2")); err == nil {
		t.Fatal("expected the run to fail rather than drop documents it cannot record")
	}
}

// A stale document means the destination already holds an equal or newer version,
// which is a success for convergence.
func TestStaleDocumentsStillAdvanceCheckpoint(t *testing.T) {
	h := newRunner(t, nil)
	h.sink.reply = func(_ int, docs []cdc.Doc) (sink.Result, error) {
		return sink.Result{Stale: len(docs)}, nil
	}

	if err := h.run(t, event(1, 1, "paid", "uuid:1"), event(2, 2, "paid", "uuid:2")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if cp := h.checkpoint(t); cp.GTIDSet != "uuid:2" {
		t.Fatalf("checkpoint = %q, want uuid:2", cp.GTIDSet)
	}
}

// A response that accounts for fewer documents than were sent cannot be trusted,
// and advancing past them would lose data.
func TestUnaccountedDocumentsFailTheRun(t *testing.T) {
	h := newRunner(t, nil)
	h.sink.reply = func(_ int, docs []cdc.Doc) (sink.Result, error) {
		return sink.Result{Applied: len(docs) - 1}, nil
	}

	if err := h.run(t, event(1, 1, "paid", "uuid:1"), event(2, 2, "paid", "uuid:2")); err == nil {
		t.Fatal("expected a run failure when the sink does not account for every document")
	}
}

// Whatever is pending when the source ends must be written, or the last partial
// batch would be lost on every clean shutdown.
func TestPendingBatchIsFlushedWhenTheSourceEnds(t *testing.T) {
	h := newRunner(t, func(o *RunnerOptions) {
		o.Limits = Limits{MaxRows: 100, MaxBytes: 1 << 20, FlushInterval: time.Hour}
	})

	if err := h.run(t, event(1, 1, "paid", "uuid:1")); err != nil {
		t.Fatalf("run: %v", err)
	}

	if got := len(h.sink.written()); got != 1 {
		t.Fatalf("expected the partial batch to be flushed, wrote %d", got)
	}
	if cp := h.checkpoint(t); cp.GTIDSet != "uuid:1" {
		t.Fatalf("checkpoint = %q, want uuid:1", cp.GTIDSet)
	}
}

// A key-changing update becomes two documents, and both must reach the sink in
// order, even though they arrived as one event.
func TestKeyChangeProducesBothDocumentsInOrder(t *testing.T) {
	h := newRunner(t, func(o *RunnerOptions) {
		o.Limits = Limits{MaxRows: 100, MaxBytes: 1 << 20, FlushInterval: time.Hour}
	})

	ev := cdc.ChangeEvent{
		Meta:   runnerMeta(),
		Op:     cdc.OpUpdate,
		Before: cdc.Row{uint64(1), "paid"},
		After:  cdc.Row{uint64(2), "paid"},
		Seq:    10,
		GTID:   "uuid:5",
	}

	if err := h.run(t, ev); err != nil {
		t.Fatalf("run: %v", err)
	}

	written := h.sink.written()
	if len(written) != 2 {
		t.Fatalf("expected 2 documents, got %d", len(written))
	}
	if !written[0].Deleted || written[0].Key != "1" {
		t.Errorf("first document should delete the old key, got %+v", written[0])
	}
	if written[1].Deleted || written[1].Key != "2" {
		t.Errorf("second document should write the new key, got %+v", written[1])
	}
	if written[1].Version != 10 {
		t.Errorf("version = %d, want the event version 10", written[1].Version)
	}
}

// An event that cannot be transformed at all would otherwise stop the stream, so it
// is recorded and skipped.
func TestUntransformableEventGoesToTheDeadLetterQueue(t *testing.T) {
	h := newRunner(t, func(o *RunnerOptions) {
		o.Limits = Limits{MaxRows: 100, MaxBytes: 1 << 20, FlushInterval: time.Hour}
	})

	poison := cdc.ChangeEvent{
		Meta:  runnerMeta(),
		Op:    cdc.OpInsert,
		After: cdc.Row{nil, "paid"}, // a null key cannot identify a row
		Seq:   1,
		GTID:  "uuid:1",
	}

	if err := h.run(t, poison, event(2, 2, "paid", "uuid:2")); err != nil {
		t.Fatalf("a poison event must not stop the stream: %v", err)
	}
	if h.dlq.count() != 1 {
		t.Fatalf("expected the poison event to be recorded, got %d dead letters", h.dlq.count())
	}
	if got := len(h.sink.written()); got != 1 {
		t.Fatalf("expected the healthy event to still be written, wrote %d", got)
	}
}

func TestRunnerTracksLagFromEventTimestamps(t *testing.T) {
	h := newRunner(t, nil)
	h.clock.at = time.UnixMilli(1_786_000_002_500)

	ev := event(1, 1, "paid", "uuid:1")
	ev.Timestamp = time.UnixMilli(1_786_000_000_000)
	if err := h.run(t, ev, event(2, 2, "paid", "uuid:2")); err != nil {
		t.Fatalf("run: %v", err)
	}

	cp := h.checkpoint(t)
	if cp.LastEventTsMs != 1_786_000_000_000 {
		t.Fatalf("last event timestamp = %d, want the source's timestamp", cp.LastEventTsMs)
	}
	lag, ok := cp.LagAt(h.clock.at)
	if !ok || lag != 2500*time.Millisecond {
		t.Fatalf("lag = %v (ok=%v), want 2.5s", lag, ok)
	}
}

func TestRunnerStopsOnContextCancellation(t *testing.T) {
	h := newRunner(t, func(o *RunnerOptions) {
		o.Limits = Limits{MaxRows: 1000, MaxBytes: 1 << 20, FlushInterval: time.Hour}
	})

	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan cdc.ChangeEvent)

	done := make(chan error, 1)
	go func() { done <- h.runner.Run(ctx, events) }()

	events <- event(1, 1, "paid", "uuid:1")
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation to be reported")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not stop after cancellation")
	}
}

// A scan position must be recorded on the same terms as a GTID: only once the
// destination has accepted the rows. Recording it on emit would advance past rows
// that were never written, and a scan never revisits them.
func TestScanCursorIsRecordedOnlyAfterTheSinkAcknowledges(t *testing.T) {
	h := newRunner(t, nil)

	var cursorAtWriteTime []byte
	h.sink.onWrite = func([]cdc.Doc) {
		if cp, err := h.store.Load(context.Background(), "orders_to_es"); err == nil {
			cursorAtWriteTime = cp.SnapshotCursor
		}
	}

	// Two scanned rows, the second carrying the chunk's cursor.
	first := cdc.ChangeEvent{Meta: runnerMeta(), Op: cdc.OpSnapshot, After: cdc.Row{uint64(1), "paid"}, Seq: 500}
	second := cdc.ChangeEvent{
		Meta: runnerMeta(), Op: cdc.OpSnapshot, After: cdc.Row{uint64(2), "paid"}, Seq: 500,
		Cursor: []byte(`["2"]`), RowsScanned: 2,
	}

	if err := h.run(t, first, second); err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(cursorAtWriteTime) > 0 {
		t.Fatalf("the cursor was recorded before the sink acknowledged: %s", cursorAtWriteTime)
	}
	cp := h.checkpoint(t)
	if string(cp.SnapshotCursor) != `["2"]` {
		t.Errorf("cursor = %s, want the chunk's cursor once acknowledged", cp.SnapshotCursor)
	}
	if cp.SnapshotRowsDone != 2 {
		t.Errorf("rows done = %d, want 2", cp.SnapshotRowsDone)
	}
}

// A failed write must leave the scan position where it was, so the chunk is read
// again rather than skipped.
func TestFailedWriteDoesNotAdvanceTheScanCursor(t *testing.T) {
	h := newRunner(t, nil)
	h.sink.reply = func(int, []cdc.Doc) (sink.Result, error) {
		return sink.Result{}, errors.New("elasticsearch unavailable")
	}

	first := cdc.ChangeEvent{Meta: runnerMeta(), Op: cdc.OpSnapshot, After: cdc.Row{uint64(1), "paid"}, Seq: 500}
	second := cdc.ChangeEvent{
		Meta: runnerMeta(), Op: cdc.OpSnapshot, After: cdc.Row{uint64(2), "paid"}, Seq: 500,
		Cursor: []byte(`["2"]`), RowsScanned: 2,
	}

	if err := h.run(t, first, second); err == nil {
		t.Fatal("expected the run to fail when the sink cannot be written")
	}

	if cp, err := h.store.Load(context.Background(), "orders_to_es"); err == nil && len(cp.SnapshotCursor) > 0 {
		t.Fatalf("cursor advanced to %s despite the write failing", cp.SnapshotCursor)
	}
}

// A graceful stop must write what it is holding, so a shutdown does not hand the
// next start a batch to redo.
func TestPendingBatchIsWrittenOnCancellation(t *testing.T) {
	h := newRunner(t, func(o *RunnerOptions) {
		o.Limits = Limits{MaxRows: 1000, MaxBytes: 1 << 20, FlushInterval: time.Hour}
		o.ShutdownGrace = 5 * time.Second
	})

	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan cdc.ChangeEvent)

	done := make(chan error, 1)
	go func() { done <- h.runner.Run(ctx, events) }()

	events <- event(1, 1, "paid", "uuid:1")
	// Let the runner take the event before cancelling.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not stop")
	}

	if got := len(h.sink.written()); got != 1 {
		t.Fatalf("expected the pending document to be written during shutdown, wrote %d", got)
	}
	if cp := h.checkpoint(t); cp.GTIDSet != "uuid:1" {
		t.Fatalf("checkpoint = %q, want the drained batch to be recorded", cp.GTIDSet)
	}
}

func TestNewRunnerRejectsMissingDependencies(t *testing.T) {
	base := func() RunnerOptions {
		return RunnerOptions{
			Stream: "s",
			Plan:   runnerPlan(t),
			Sink:   &stubSink{},
			Store:  checkpoint.NewMemoryStore(),
			Limits: Limits{MaxRows: 1, MaxBytes: 1, FlushInterval: time.Second},
		}
	}

	for _, tc := range []struct {
		name   string
		break_ func(*RunnerOptions)
	}{
		{"no stream", func(o *RunnerOptions) { o.Stream = "" }},
		{"no plan", func(o *RunnerOptions) { o.Plan = nil }},
		{"no sink", func(o *RunnerOptions) { o.Sink = nil }},
		{"no store", func(o *RunnerOptions) { o.Store = nil }},
		{"unusable limits", func(o *RunnerOptions) { o.Limits = Limits{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := base()
			tc.break_(&opts)
			if _, err := NewRunner(opts); err == nil {
				t.Fatal("expected the runner to refuse to start")
			}
		})
	}
}

type failingDLQ struct{}

func (failingDLQ) Record(context.Context, []sink.Rejection) error {
	return errors.New("dlq volume is full")
}
