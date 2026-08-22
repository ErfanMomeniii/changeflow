package checkpoint

import (
	"context"
	"sync"
	"time"
)

// MemoryStore keeps checkpoints in memory. It exists for tests and for running
// against a throwaway target, and it counts writes so tests can assert that
// reservation happens per block rather than per event.
//
// It is not a usable production backend: losing the process loses the position,
// which forces a re-snapshot.
type MemoryStore struct {
	mu    sync.Mutex
	data  map[string]Checkpoint
	saves int
	now   func() time.Time
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[string]Checkpoint)}
}

// Load returns a copy of the stored checkpoint, or ErrNotFound.
func (s *MemoryStore) Load(_ context.Context, stream string) (Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp, ok := s.data[stream]
	if !ok {
		return Checkpoint{}, ErrNotFound
	}
	cp.SnapshotCursor = append([]byte(nil), cp.SnapshotCursor...)
	return cp, nil
}

// Save stores a copy of the checkpoint.
func (s *MemoryStore) Save(_ context.Context, cp Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp.SnapshotCursor = append([]byte(nil), cp.SnapshotCursor...)
	if s.now != nil {
		cp.UpdatedAt = s.now()
	} else {
		cp.UpdatedAt = time.Now()
	}
	s.data[cp.Stream] = cp
	s.saves++
	return nil
}

// Saves reports how many times Save has been called.
func (s *MemoryStore) Saves() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves
}
