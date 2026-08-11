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

// RunnerOptions configures one stream's pipeline.
type RunnerOptions struct {
	Stream string
	Plan   *Plan
	Sink   sink.Sink
	DLQ    DeadLetterQueue
	Store  checkpoint.Store
	Limits Limits

	// Now is injected so tests control batching deadlines.
	Now    func() time.Time
	Logger *slog.Logger
}

// Runner drives one stream: it transforms events, batches the documents, writes
// them, and only then records the position.
//
// That order is the design's central rule. Recording a position before the sink
// has accepted the documents would mean a crash in between loses them, with
// nothing left to indicate anything is missing.
type Runner struct {
	stream  string
	plan    *Plan
	sink    sink.Sink
	dlq     DeadLetterQueue
	store   checkpoint.Store
	batcher *Batcher
	now     func() time.Time
	log     *slog.Logger

	// pendingGTID is the position the current batch reaches once acknowledged.
	pendingGTID string
	// pendingEventTsMs is the source timestamp of the newest event in the batch,
	// which is what lag is measured from.
	pendingEventTsMs int64
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

	return &Runner{
		stream:  opts.Stream,
		plan:    opts.Plan,
		sink:    opts.Sink,
		dlq:     opts.DLQ,
		store:   opts.Store,
		batcher: batcher,
		now:     now,
		log:     log,
	}, nil
}

// Run consumes events until the channel closes or the context is cancelled.
//
// Reading from a channel rather than owning the source is what lets a snapshot
// scan and a binlog stream share this code unchanged.
func (r *Runner) Run(ctx context.Context, events <-chan cdc.ChangeEvent) error {
	for {
		// A deadline exists only while documents are pending, so an idle stream does
		// not wake up for nothing.
		var timer *time.Timer
		var tick <-chan time.Time
		if deadline, ok := r.batcher.NextDeadline(); ok {
			// Measured against the same clock the batcher used to set the deadline.
			// Mixing an injected clock here with the real one produces a delay that
			// can already be negative, firing the timer at once and splitting a batch
			// that was nowhere near due.
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
			return ctx.Err()

		case <-tick:
			stopTimer(timer)
			if err := r.flush(ctx); err != nil {
				return err
			}

		case ev, ok := <-events:
			stopTimer(timer)
			if !ok {
				// The source ended. Whatever is pending still has to be written, or a
				// clean shutdown would drop the last partial batch.
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

// handle transforms one event and adds the resulting documents to the batch.
func (r *Runner) handle(ctx context.Context, ev cdc.ChangeEvent) error {
	docs, err := r.plan.Apply(&ev)
	if err != nil {
		// An event that cannot be transformed will never transform, so stopping the
		// stream over it would block every later change behind one bad row.
		return r.deadLetter(ctx, []sink.Rejection{{
			Doc:    cdc.Doc{Key: ev.GTID},
			Reason: fmt.Sprintf("transform failed: %v", err),
		}})
	}

	if ev.GTID != "" {
		r.pendingGTID = ev.GTID
	}
	if ms := ev.Timestamp.UnixMilli(); !ev.Timestamp.IsZero() && ms > r.pendingEventTsMs {
		r.pendingEventTsMs = ms
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

// flush writes whatever is pending.
func (r *Runner) flush(ctx context.Context) error {
	batch := r.batcher.Flush()
	if len(batch) == 0 {
		return nil
	}
	return r.write(ctx, batch)
}

// write applies a batch and, once it is accepted, records the position it reached.
func (r *Runner) write(ctx context.Context, batch []cdc.Doc) error {
	result, err := r.sink.Write(ctx, batch)
	if err != nil {
		// The position stays where it was, so a restart replays this batch. The
		// destination's versioning makes that harmless.
		return fmt.Errorf("pipeline %s: write batch of %d: %w", r.stream, len(batch), err)
	}

	// Every document must be accounted for. A sink reporting fewer outcomes than
	// documents may have dropped some, and advancing the position would then lose
	// them with nothing to show anything is missing.
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

// deadLetter records permanently failed documents, failing the run if it cannot.
func (r *Runner) deadLetter(ctx context.Context, rejections []sink.Rejection) error {
	if r.dlq == nil {
		// With nowhere to record them, continuing would discard them silently.
		return fmt.Errorf("pipeline %s: %d document(s) were refused and no dead letter queue is configured: %s",
			r.stream, len(rejections), rejections[0])
	}
	if err := r.dlq.Record(ctx, rejections); err != nil {
		return fmt.Errorf("pipeline %s: cannot record %d refused document(s): %w", r.stream, len(rejections), err)
	}

	for _, rej := range rejections {
		r.log.Warn("document refused by destination",
			"stream", r.stream, "key", rej.Doc.Key, "status", rej.Status, "reason", rej.Reason)
	}
	return nil
}

// advance records the position the acknowledged batch reached.
func (r *Runner) advance(ctx context.Context) error {
	if r.pendingGTID == "" {
		// A snapshot scan produces no positions; its progress is tracked separately.
		return nil
	}

	cp, err := r.store.Load(ctx, r.stream)
	if err != nil && !errors.Is(err, checkpoint.ErrNotFound) {
		return fmt.Errorf("pipeline %s: load checkpoint: %w", r.stream, err)
	}

	cp.Stream = r.stream
	cp.GTIDSet = r.pendingGTID
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
