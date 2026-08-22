package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/checkpoint"
	"github.com/ErfanMomeniii/changeflow/internal/sink"
)

// DeadLetterQueue records documents a destination refused permanently.
//
// Recording has to be durable, because the position advances past these documents:
// if they were lost here, they would be lost entirely.
type DeadLetterQueue interface {
	Record(ctx context.Context, rejections []sink.Rejection) error
}

// Observer receives what a stream is doing, for metrics.
//
// An interface rather than a concrete recorder, so this package depends on no metrics
// library and a test can assert on the reports themselves.
type Observer interface {
	Event(operation cdc.Operation)
	Lag(seconds float64)
	Batch(rows int)
	Write(applied, stale, rejected int, elapsed time.Duration, failed bool)
	DeadLettered(n int)
}

type nopObserver struct{}

func (nopObserver) Event(cdc.Operation)                      {}
func (nopObserver) Lag(float64)                              {}
func (nopObserver) Batch(int)                                {}
func (nopObserver) Write(int, int, int, time.Duration, bool) {}
func (nopObserver) DeadLettered(int)                         {}

// RunnerOptions configures one stream's pipeline.
type RunnerOptions struct {
	Stream        string
	Plan          *Plan
	Sink          sink.Sink
	DLQ           DeadLetterQueue
	Store         checkpoint.Store
	Limits        Limits
	Now           func() time.Time
	ShutdownGrace time.Duration
	Observer      Observer
	Logger        *slog.Logger
}

// Runner drives one stream: transform, batch, write, and only then record the position.
//
// That order is the central rule. A position recorded before the sink accepted the
// documents would lose them on a crash, with nothing left to say anything was missing.
type Runner struct {
	stream           string
	plan             *Plan
	sink             sink.Sink
	dlq              DeadLetterQueue
	store            checkpoint.Store
	batcher          *Batcher
	now              func() time.Time
	shutdownGrace    time.Duration
	observer         Observer
	log              *slog.Logger
	pendingGTID      string
	pendingEventTsMs int64
	pendingCursor    []byte
	pendingRows      uint64
}

// NewRunner validates dependencies and prepares a runner.
func NewRunner(opts RunnerOptions) (*Runner, error) {
	switch {
	case opts.Stream == "":
		return nil, errors.New("pipeline: a stream name is required")
	case opts.Plan == nil:
		return nil, errors.New("pipeline: a compiled plan is required")
	case opts.Sink == nil:
		return nil, errors.New("pipeline: a sink is required")
	case opts.Store == nil:
		return nil, errors.New("pipeline: a checkpoint store is required")
	}
	batcher, err := NewBatcher(opts.Limits, opts.Now)
	if err != nil {
		return nil, err
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	grace := opts.ShutdownGrace
	if grace <= 0 {
		grace = 30 * time.Second
	}
	observer := opts.Observer
	if observer == nil {
		observer = nopObserver{}
	}
	return &Runner{
		stream:        opts.Stream,
		plan:          opts.Plan,
		sink:          opts.Sink,
		dlq:           opts.DLQ,
		store:         opts.Store,
		batcher:       batcher,
		now:           now,
		shutdownGrace: grace,
		observer:      observer,
		log:           log,
	}, nil
}

// Run consumes events until the channel closes or the context is cancelled.
//
// Reading from a channel rather than owning the source is what lets a snapshot
// scan and a binlog stream share this code unchanged.
func (r *Runner) Run(ctx context.Context, events <-chan cdc.ChangeEvent) error {
	for {
		var timer *time.Timer
		var tick <-chan time.Time
		if deadline, ok := r.batcher.NextDeadline(); ok {
			delay := deadline.Sub(r.now())
			if delay < 0 {
				delay = 0
			}
			timer = time.NewTimer(delay)
			tick = timer.C
		}
		select {
		case <-ctx.Done():
			stopTimer(timer)
			r.drain()
			return ctx.Err()
		case <-tick:
			stopTimer(timer)
			if err := r.flush(ctx); err != nil {
				return err
			}
		case ev, ok := <-events:
			stopTimer(timer)
			if !ok {
				return r.flush(ctx)
			}
			if err := r.handle(ctx, ev); err != nil {
				return err
			}
		}
	}
}

func stopTimer(t *time.Timer) {
	if t != nil {
		t.Stop()
	}
}

func (r *Runner) handle(ctx context.Context, ev cdc.ChangeEvent) error {
	docs, err := r.plan.Apply(&ev)
	if err != nil {
		return r.deadLetter(ctx, []sink.Rejection{{
			Doc:    cdc.Doc{Key: ev.GTID},
			Reason: fmt.Sprintf("transform failed: %v", err),
		}})
	}
	r.observer.Event(ev.Operation)
	if !ev.Timestamp.IsZero() {
		r.observer.Lag(r.now().Sub(ev.Timestamp).Seconds())
	}
	if ev.GTID != "" {
		r.pendingGTID = ev.GTID
	}
	if ms := ev.Timestamp.UnixMilli(); !ev.Timestamp.IsZero() && ms > r.pendingEventTsMs {
		r.pendingEventTsMs = ms
	}
	if len(ev.Cursor) > 0 {
		r.pendingCursor = ev.Cursor
		r.pendingRows = ev.RowsScanned
	}
	for _, d := range docs {
		if batch := r.batcher.Add(d); batch != nil {
			if err := r.write(ctx, batch); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Runner) drain() {
	if r.batcher.Len() == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), r.shutdownGrace)
	defer cancel()
	pending := r.batcher.Len()
	if err := r.flush(ctx); err != nil {
		r.log.Warn("could not write the final batch during shutdown; it will be replayed on the next start",
			"stream", r.stream, "documents", pending, "error", err)
		return
	}
	r.log.Info("wrote the final batch during shutdown", "stream", r.stream, "documents", pending)
}

func (r *Runner) flush(ctx context.Context) error {
	batch := r.batcher.Flush()
	if len(batch) == 0 {
		return nil
	}
	return r.write(ctx, batch)
}

func (r *Runner) write(ctx context.Context, batch []cdc.Doc) error {
	r.observer.Batch(len(batch))
	started := r.now()
	result, err := r.sink.Write(ctx, batch)
	r.observer.Write(result.Applied, result.Stale, len(result.Rejected), r.now().Sub(started), err != nil)
	if err != nil {
		return fmt.Errorf("pipeline %s: write batch of %d: %w", r.stream, len(batch), err)
	}
	if result.Total() != len(batch) {
		return fmt.Errorf("pipeline %s: sink accounted for %d of %d documents, so the batch outcome is unknown",
			r.stream, result.Total(), len(batch))
	}
	if len(result.Rejected) > 0 {
		if err := r.deadLetter(ctx, result.Rejected); err != nil {
			return err
		}
	}
	return r.advance(ctx)
}

func (r *Runner) deadLetter(ctx context.Context, rejections []sink.Rejection) error {
	if r.dlq == nil {
		return fmt.Errorf("pipeline %s: %d document(s) were refused and no dead letter queue is configured: %s",
			r.stream, len(rejections), rejections[0])
	}
	if err := r.dlq.Record(ctx, rejections); err != nil {
		return fmt.Errorf("pipeline %s: cannot record %d refused document(s): %w", r.stream, len(rejections), err)
	}
	r.observer.DeadLettered(len(rejections))
	for _, rej := range rejections {
		r.log.Warn("document refused by destination",
			"stream", r.stream, "key", rej.Doc.Key, "status", rej.Status, "reason", rej.Reason)
	}
	return nil
}

func (r *Runner) advance(ctx context.Context) error {
	if r.pendingGTID == "" && len(r.pendingCursor) == 0 {
		return nil
	}
	cp, err := r.store.Load(ctx, r.stream)
	if err != nil && !errors.Is(err, checkpoint.ErrNotFound) {
		return fmt.Errorf("pipeline %s: load checkpoint: %w", r.stream, err)
	}
	cp.Stream = r.stream
	if r.pendingGTID != "" {
		cp.GTIDSet = r.pendingGTID
	}
	if len(r.pendingCursor) > 0 {
		cp.SnapshotCursor = r.pendingCursor
		cp.SnapshotRowsDone = r.pendingRows
	}
	if r.pendingEventTsMs > 0 {
		cp.LastEventTsMs = r.pendingEventTsMs
	}
	if cp.SchemaVersion == 0 {
		cp.SchemaVersion = checkpoint.SchemaVersion
	}
	if err := r.store.Save(ctx, cp); err != nil {
		return fmt.Errorf("pipeline %s: save checkpoint: %w", r.stream, err)
	}
	return nil
}
