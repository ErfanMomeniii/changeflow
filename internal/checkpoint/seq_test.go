package checkpoint

import (
	"context"
	"sync"
	"testing"
	"time"
)

func fixedClock(ms int64) func() time.Time {
	return func() time.Time { return time.UnixMilli(ms) }
}

const (
	testClockMs = 1_786_000_000_000
	testFloor   = testClockMs << floorShift
)

func newTestAllocator(t *testing.T, store Store, blockSize uint64, clockMs int64) *Allocator {
	t.Helper()
	a, err := NewAllocator(context.Background(), store, "orders_to_es", blockSize, fixedClock(clockMs))
	if err != nil {
		t.Fatalf("new allocator: %v", err)
	}
	return a
}

func TestAllocatorIssuesIncreasingValues(t *testing.T) {
	a := newTestAllocator(t, NewMemoryStore(), 100, testClockMs)
	var prev uint64
	for i := 0; i < 250; i++ {
		got, err := a.Next(context.Background())
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if got <= prev {
			t.Fatalf("value %d did not increase: got %d after %d", i, got, prev)
		}
		prev = got
	}
}

// The floor is what makes versions survive losing the checkpoint store. Without
// it a fresh watermark restarts near zero, every write looks stale to the sink,
// and the sink silently freezes on old data while appearing healthy.
func TestAllocatorFloorsOnWallClock(t *testing.T) {
	a := newTestAllocator(t, NewMemoryStore(), 100, testClockMs)
	got, err := a.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if got < testFloor {
		t.Fatalf("got %d, expected at least the clock floor %d", got, testFloor)
	}
}

func TestAllocatorPrefersPersistedWatermarkOverLowerClock(t *testing.T) {
	store := NewMemoryStore()
	high := uint64(testFloor) + 5_000_000
	mustSave(t, store, Checkpoint{Stream: "orders_to_es", SeqWatermark: high})
	a := newTestAllocator(t, store, 100, 1_000)
	got, err := a.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if got <= high {
		t.Fatalf("got %d, must exceed the persisted watermark %d even when the clock goes backwards", got, high)
	}
}

// A restart must never reissue a value, because a reissued version would let an
// older write win over a newer one in the sink.
func TestAllocatorNeverReissuesAcrossRestart(t *testing.T) {
	store := NewMemoryStore()
	const blockSize = 50
	first := newTestAllocator(t, store, blockSize, testClockMs)
	var issued []uint64
	for i := 0; i < 10; i++ {
		v, err := first.Next(context.Background())
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		issued = append(issued, v)
	}
	highest := issued[len(issued)-1]
	second := newTestAllocator(t, store, blockSize, testClockMs)
	next, err := second.Next(context.Background())
	if err != nil {
		t.Fatalf("next after restart: %v", err)
	}
	if next <= highest {
		t.Fatalf("restart reissued %d, which is not above the highest previously issued %d", next, highest)
	}
	for _, v := range issued {
		if next == v {
			t.Fatalf("restart reissued the exact value %d", v)
		}
	}
}

// The whole point of blocks is to avoid a write per event.
func TestAllocatorPersistsOncePerBlock(t *testing.T) {
	store := NewMemoryStore()
	const blockSize = 100
	a := newTestAllocator(t, store, blockSize, testClockMs)
	for i := 0; i < blockSize*3; i++ {
		if _, err := a.Next(context.Background()); err != nil {
			t.Fatalf("next: %v", err)
		}
	}
	if got := store.Saves(); got != 3 {
		t.Fatalf("expected 3 reservations for %d values at block size %d, got %d", blockSize*3, blockSize, got)
	}
}

func TestAllocatorReservesAheadOfWhatItIssues(t *testing.T) {
	store := NewMemoryStore()
	const blockSize = 100
	a := newTestAllocator(t, store, blockSize, testClockMs)
	first, err := a.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	cp, err := store.Load(context.Background(), "orders_to_es")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cp.SeqWatermark < first+blockSize-1 {
		t.Fatalf("watermark %d does not cover the issued block starting at %d", cp.SeqWatermark, first)
	}
}

func TestAllocatorIsSafeForConcurrentUse(t *testing.T) {
	a := newTestAllocator(t, NewMemoryStore(), 64, testClockMs)
	const goroutines, perGoroutine = 8, 200
	var (
		mu   sync.Mutex
		seen = make(map[uint64]bool, goroutines*perGoroutine)
		wg   sync.WaitGroup
	)
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				v, err := a.Next(context.Background())
				if err != nil {
					t.Errorf("next: %v", err)
					return
				}
				mu.Lock()
				if seen[v] {
					t.Errorf("value %d handed out twice", v)
				}
				seen[v] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != goroutines*perGoroutine {
		t.Fatalf("expected %d distinct values, got %d", goroutines*perGoroutine, len(seen))
	}
}

// Elasticsearch external versions are a signed 64-bit field, so the floor must
// leave room for decades of headroom rather than overflowing into negative.
func TestSeqFloorStaysWellInsideSignedRange(t *testing.T) {
	far := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	floor := seqFloor(time.UnixMilli(far))
	if floor > uint64(1)<<62 {
		t.Fatalf("floor %d is too close to the signed 64-bit limit", floor)
	}
	if floor <= uint64(far) {
		t.Fatalf("floor %d must exceed the millisecond reading %d it derives from", floor, far)
	}
}

// Within a millisecond the floor leaves room for many events, so a busy stream
// cannot outrun the clock and collide with the next millisecond's floor.
func TestSeqFloorLeavesRoomWithinAMillisecond(t *testing.T) {
	now := time.UnixMilli(testClockMs)
	gap := seqFloor(now.Add(time.Millisecond)) - seqFloor(now)
	if gap != 1<<floorShift {
		t.Fatalf("expected %d values per millisecond, got %d", 1<<floorShift, gap)
	}
	if gap < 1000 {
		t.Fatalf("only %d values per millisecond is too few for a busy stream", gap)
	}
}

func TestNewAllocatorRejectsZeroBlockSize(t *testing.T) {
	_, err := NewAllocator(context.Background(), NewMemoryStore(), "orders_to_es", 0, fixedClock(testClockMs))
	if err == nil {
		t.Fatal("expected an error for a zero block size, which would never issue a value")
	}
}

func TestNewAllocatorRejectsEmptyStream(t *testing.T) {
	_, err := NewAllocator(context.Background(), NewMemoryStore(), "", 100, fixedClock(testClockMs))
	if err == nil {
		t.Fatal("expected an error for an empty stream name")
	}
}

func mustSave(t *testing.T, s Store, cp Checkpoint) {
	t.Helper()
	if err := s.Save(context.Background(), cp); err != nil {
		t.Fatalf("save: %v", err)
	}
}
