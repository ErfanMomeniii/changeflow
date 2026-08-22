package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const floorShift = 10

func seqFloor(now time.Time) uint64 {
	ms := now.UnixMilli()
	if ms < 0 {
		return 0
	}
	return uint64(ms) << floorShift
}

// Allocator hands out increasing version numbers, reserved in blocks so a durable write
// happens once per block rather than once per event.
//
// Reserved before issued, so a crash skips the rest of a block — harmless — rather than
// reissuing values already used, which would let an older write win.
type Allocator struct {
	store     Store
	stream    string
	blockSize uint64
	now       func() time.Time
	mu        sync.Mutex
	next      uint64
	end       uint64
}

// NewAllocator prepares an allocator for one stream, reserving nothing until Next.
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

func (a *Allocator) reserve(ctx context.Context) error {
	cp, err := a.store.Load(ctx, a.stream)
	switch {
	case errors.Is(err, ErrNotFound):
		cp = Checkpoint{Stream: a.stream, SchemaVersion: SchemaVersion}
	case err != nil:
		return fmt.Errorf("checkpoint: load %s: %w", a.stream, err)
	}
	start := cp.SeqWatermark + 1
	if floor := seqFloor(a.now()); floor > start {
		start = floor
	}
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
