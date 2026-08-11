package checkpoint

import (
	"testing"
	"time"
)

func TestLagAtReportsDistanceBehindSource(t *testing.T) {
	now := time.UnixMilli(testClockMs)
	cp := Checkpoint{LastEventTsMs: testClockMs - 2_500}

	lag, ok := cp.LagAt(now)
	if !ok {
		t.Fatal("expected lag to be available")
	}
	if lag != 2500*time.Millisecond {
		t.Fatalf("got %v, want 2.5s", lag)
	}
}

// No applied event is not the same state as being caught up, and a dashboard
// showing "0s lag" for a stream that has never run would be a lie.
func TestLagAtReportsUnavailableBeforeAnyEvent(t *testing.T) {
	if _, ok := (Checkpoint{}).LagAt(time.UnixMilli(testClockMs)); ok {
		t.Fatal("expected lag to be unavailable when no event has been applied")
	}
}

// A source clock reading ahead of ours must not surface as negative lag.
func TestLagAtClampsFutureTimestamps(t *testing.T) {
	now := time.UnixMilli(testClockMs)
	cp := Checkpoint{LastEventTsMs: testClockMs + 5_000}

	lag, ok := cp.LagAt(now)
	if !ok {
		t.Fatal("expected lag to be available")
	}
	if lag != 0 {
		t.Fatalf("got %v, want 0 for a source timestamp ahead of us", lag)
	}
}

// The store hands out copies: a caller mutating a returned cursor must not be
// able to corrupt the stored position.
func TestMemoryStoreDoesNotShareCursorMemory(t *testing.T) {
	store := NewMemoryStore()
	mustSave(t, store, Checkpoint{Stream: "s", SnapshotCursor: []byte("key-1")})

	loaded, err := store.Load(t.Context(), "s")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	loaded.SnapshotCursor[0] = 'X'

	again, err := store.Load(t.Context(), "s")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if string(again.SnapshotCursor) != "key-1" {
		t.Fatalf("stored cursor was mutated through a returned slice: %q", again.SnapshotCursor)
	}
}

func TestMemoryStoreReportsMissingStream(t *testing.T) {
	if _, err := NewMemoryStore().Load(t.Context(), "never-seen"); err != ErrNotFound {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}
