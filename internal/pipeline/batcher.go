package pipeline

import (
	"errors"
	"time"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
)

// Limits decide when a batch is sent.
//
// All three are required. A batcher without a row or byte limit could grow until
// the destination refuses the request, and one without an interval would hold a
// quiet table's changes indefinitely.
type Limits struct {
	MaxRows       int
	MaxBytes      uint64
	FlushInterval time.Duration
}

// Batcher groups documents so a destination is written in useful sizes rather than
// once per row.
//
// Documents keep their arrival order, which is what preserves the sequence of
// writes to any single key: a delete followed by an insert must reach the
// destination in that order.
//
// A Batcher is not safe for concurrent use. Each pipeline owns one, driven by a
// single goroutine.
type Batcher struct {
	limits Limits
	now    func() time.Time

	pending []cdc.Doc
	bytes   uint64
	// startedAt is when the current batch received its first document, so the
	// interval bounds how long that document waits rather than restarting with
	// every arrival.
	startedAt time.Time
}

// NewBatcher returns a batcher, rejecting limits that could never work.
func NewBatcher(limits Limits, now func() time.Time) (*Batcher, error) {
	switch {
	case limits.MaxRows < 1:
		return nil, errors.New("batcher: MaxRows must be at least 1")
	case limits.MaxBytes == 0:
		return nil, errors.New("batcher: MaxBytes must be above zero")
	case limits.FlushInterval <= 0:
		return nil, errors.New("batcher: FlushInterval must be above zero, or a quiet table would never be written")
	}
	if now == nil {
		now = time.Now
	}
	return &Batcher{limits: limits, now: now}, nil
}

// Add takes one document, returning a batch when the document completes it.
//
// A returned batch is owned by the caller: the batcher starts a fresh slice rather
// than reusing the one it handed over, since the caller still holds it while the
// next batch fills.
func (b *Batcher) Add(d cdc.Doc) []cdc.Doc {
	size := uint64(d.Size())

	// A document that alone exceeds the byte budget still has to be delivered, so
	// send whatever is pending first and then let it through on its own. Refusing
	// it would stall the stream permanently.
	if size >= b.limits.MaxBytes && len(b.pending) > 0 {
		flushed := b.Flush()
		b.append(d, size)
		return append(flushed, b.Flush()...)
	}

	// Flushing before adding keeps a batch inside its byte budget instead of
	// overshooting it by one document.
	if len(b.pending) > 0 && b.bytes+size > b.limits.MaxBytes {
		flushed := b.Flush()
		b.append(d, size)
		return flushed
	}

	b.append(d, size)

	if len(b.pending) >= b.limits.MaxRows || b.bytes >= b.limits.MaxBytes {
		return b.Flush()
	}
	return nil
}

func (b *Batcher) append(d cdc.Doc, size uint64) {
	if len(b.pending) == 0 {
		b.startedAt = b.now()
	}
	b.pending = append(b.pending, d)
	b.bytes += size
}

// DueForFlush reports whether the pending batch has waited long enough. An empty
// batcher is never due.
func (b *Batcher) DueForFlush() bool {
	if len(b.pending) == 0 {
		return false
	}
	return !b.now().Before(b.startedAt.Add(b.limits.FlushInterval))
}

// NextDeadline returns when the pending batch becomes due, so a caller can wait
// rather than poll. The second result is false when nothing is pending.
func (b *Batcher) NextDeadline() (time.Time, bool) {
	if len(b.pending) == 0 {
		return time.Time{}, false
	}
	return b.startedAt.Add(b.limits.FlushInterval), true
}

// Flush hands over the pending documents and starts a new batch. It returns nil
// when nothing is pending, so a caller can flush unconditionally.
func (b *Batcher) Flush() []cdc.Doc {
	if len(b.pending) == 0 {
		return nil
	}
	out := b.pending
	b.pending = nil
	b.bytes = 0
	b.startedAt = time.Time{}
	return out
}

// Len reports how many documents are pending.
func (b *Batcher) Len() int { return len(b.pending) }

// Bytes reports the pending batch's size, counting keys as well as bodies since
// both travel to the destination.
func (b *Batcher) Bytes() uint64 { return b.bytes }
