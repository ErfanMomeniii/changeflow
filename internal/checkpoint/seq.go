package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// floorShift sets how many versions each millisecond of wall clock is worth.
// Ten bits leaves 1024 versions per millisecond, far above any single source's
// event rate, while keeping the value decades away from the signed 64-bit limit
// that sink version fields impose.
const floorShift = 10

// seqFloor converts a wall-clock instant into the lowest version acceptable at
// that moment.
//
// The floor exists for one failure: if the checkpoint store is lost, a watermark
// restarting near zero would make every subsequent write look older than what the
// sink already holds. The sink would reject them all as stale and silently freeze
// on old data while reporting success. Wall clock only moves forward, so flooring
// on it keeps versions increasing even across total state loss.
func seqFloor(now time.Time) uint64 {
	ms := now.UnixMilli()
	if ms < 0 {
		return 0
	}
	return uint64(ms) << floorShift
}

// Allocator hands out monotonically increasing version numbers, reserving them in
// blocks so a durable write is needed once per block rather than once per event.
//
// Reservation happens before values are issued. A crash therefore skips the
// remainder of a block, which is harmless, rather than reissuing values that were
// already used, which would let an older write win in the sink.
type Allocator struct {
	store     Store
	stream    string
	blockSize uint64
	now       func() time.Time

	mu   sync.Mutex
	next uint64 // next value to hand out
	end  uint64 // last value this allocator may hand out before reserving again
}

// NewAllocator prepares an allocator for one stream. It does not reserve
// anything until the first call to Next.
func NewAllocator(ctx context.Context, store Store, stream string, blockSize uint64, now func() time.Time) (*Allocator, error) {
	if store == nil {
		return nil, errors.New("checkpoint: allocator needs a store")
	}
	if stream == "" {
		return nil, errors.New("checkpoint: allocator needs a stream name")
	}
	if blockSize == 0 {
		return nil, errors.New("checkpoint: allocator needs a block size above zero")
	}
	if now == nil {
		now = time.Now
	}
	// next above end means "no block held", which forces the first Next to
	// reserve. Leaving both at zero would hand out zero without reserving.
	return &Allocator{store: store, stream: stream, blockSize: blockSize, now: now, next: 1, end: 0}, nil
}

// Next returns the next version number, reserving a fresh block when the current
// one is spent.
func (a *Allocator) Next(ctx context.Context) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.next > a.end {
		if err := a.reserve(ctx); err != nil {
			return 0, err
		}
	}

	v := a.next
	a.next++
	return v, nil
}

// reserve claims the next block and persists its upper bound before any of it is
// handed out.
func (a *Allocator) reserve(ctx context.Context) error {
	cp, err := a.store.Load(ctx, a.stream)
	switch {
	case errors.Is(err, ErrNotFound):
		cp = Checkpoint{Stream: a.stream, SchemaVersion: SchemaVersion}
	case err != nil:
		return fmt.Errorf("checkpoint: load %s: %w", a.stream, err)
	}

	// Start above both what was previously reserved and what the clock permits,
	// so neither a stale watermark nor a lost one can produce a regression.
	start := cp.SeqWatermark + 1
	if floor := seqFloor(a.now()); floor > start {
		start = floor
	}
	// A block reserved earlier in this process must not be re-handed out if the
	// store somehow reports an older watermark.
	if a.end >= start {
		start = a.end + 1
	}

	end := start + a.blockSize - 1
	if end < start {
		return fmt.Errorf("checkpoint: sequence space exhausted for %s", a.stream)
	}

	cp.Stream = a.stream
	cp.SeqWatermark = end
	if cp.SchemaVersion == 0 {
		cp.SchemaVersion = SchemaVersion
	}
	if err := a.store.Save(ctx, cp); err != nil {
		return fmt.Errorf("checkpoint: reserve sequence block for %s: %w", a.stream, err)
	}

	a.next, a.end = start, end
	return nil
}
