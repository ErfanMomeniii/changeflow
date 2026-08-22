package pipeline

import (
	"errors"
	"time"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
)

// Limits decide when a batch is sent. All three are required: without a size limit a
// batch grows until the destination refuses it, and without an interval a quiet table's
// changes wait indefinitely.
type Limits struct {
	MaxRows       int
	MaxBytes      uint64
	FlushInterval time.Duration
}

// Batcher groups documents so a destination is written in useful sizes rather than once
// per row.
//
// Arrival order is kept, which is what preserves the sequence of writes to a single key.
// Not safe for concurrent use: each pipeline owns one, driven by one goroutine.
type Batcher struct {
	limits    Limits
	now       func() time.Time
	pending   []cdc.Doc
	bytes     uint64
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
// A returned batch belongs to the caller; the next one fills a fresh slice.
func (b *Batcher) Add(d cdc.Doc) []cdc.Doc {
	size := uint64(d.Size())
	if size >= b.limits.MaxBytes && len(b.pending) > 0 {
		flushed := b.Flush()
		b.append(d, size)
		return append(flushed, b.Flush()...)
	}
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

// DueForFlush reports whether the pending batch has waited long enough.
func (b *Batcher) DueForFlush() bool {
	if len(b.pending) == 0 {
		return false
	}
	return !b.now().Before(b.startedAt.Add(b.limits.FlushInterval))
}

// NextDeadline returns when the pending batch becomes due, so a caller can wait rather
// than poll, and false when nothing is pending.
func (b *Batcher) NextDeadline() (time.Time, bool) {
	if len(b.pending) == 0 {
		return time.Time{}, false
	}
	return b.startedAt.Add(b.limits.FlushInterval), true
}

// Flush hands over the pending documents and starts a new batch, returning nil when
// nothing is pending so a caller can flush unconditionally.
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

// Bytes reports the pending batch's size, keys included: both travel to the destination.
func (b *Batcher) Bytes() uint64 { return b.bytes }
