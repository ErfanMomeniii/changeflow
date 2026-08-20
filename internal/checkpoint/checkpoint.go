// Package checkpoint stores each stream's replication position durably.
//
// A checkpoint holds no row data — only a position, a snapshot cursor, and a version
// watermark — so losing it costs a re-snapshot, never the data. Other tools read these
// fields to report progress, so they are additive and versioned rather than repurposed.
package checkpoint

import (
	"context"
	"errors"
	"time"
)

// SchemaVersion identifies the layout of a stored checkpoint. A reader finding a higher
// version must refuse the row rather than guess at fields it does not know.
const SchemaVersion = 1

// ErrNotFound reports that a stream has no checkpoint yet, which is how a first-ever run
// is distinguished from a lost one.
var ErrNotFound = errors.New("checkpoint not found")

// Checkpoint is one stream's durable position.
type Checkpoint struct {
	Stream string

	// GTIDSet is the position the sink has acknowledged. Written only after an
	// acknowledgement, so a crash replays rather than skips.
	GTIDSet string

	// SnapshotDone keeps restarts from rescanning.
	SnapshotDone bool

	// SnapshotStartGTID is where streaming resumes once the scan completes.
	SnapshotStartGTID string

	// SnapshotCursor is the last key the scan wrote, so an interruption resumes.
	SnapshotCursor []byte

	// SnapshotBaseSeq is the version stamped on every snapshot row. Events replayed from
	// SnapshotStartGTID carry higher versions, which is what stops a scanned row from
	// overwriting a newer change.
	SnapshotBaseSeq uint64

	// SnapshotRowsDone and SnapshotRowsTotal drive a progress estimate only; the total
	// comes from table statistics and nothing depends on its accuracy.
	SnapshotRowsDone  uint64
	SnapshotRowsTotal uint64

	// SeqWatermark is the highest version reserved. Values up to it may already have been
	// issued, so a new allocator starts above it.
	SeqWatermark uint64

	// LastEventTsMs is the source timestamp of the most recent applied event; lag is
	// derived from it rather than stored.
	LastEventTsMs int64

	// LastError records why a stream stopped, for operators and status tooling.
	LastError string

	SchemaVersion int

	// UpdatedAt is set by the store on write.
	UpdatedAt time.Time
}

// LagAt reports how far behind the source this checkpoint was at a given moment, and
// false when nothing has been applied: zero lag and no data must not look alike.
func (c Checkpoint) LagAt(now time.Time) (time.Duration, bool) {
	if c.LastEventTsMs == 0 {
		return 0, false
	}
	lag := now.Sub(time.UnixMilli(c.LastEventTsMs))
	if lag < 0 {
		// The source clock can read ahead of ours; report caught up, not negative lag.
		return 0, true
	}
	return lag, true
}

// ClearSnapshot resets the scan state so the next run scans the table again.
//
// The stream position is left alone: a rebuild captures a fresh one when its scan starts,
// and discarding the current one would strand any progress made since.
func (c *Checkpoint) ClearSnapshot() {
	c.SnapshotDone = false
	c.SnapshotStartGTID = ""
	c.SnapshotCursor = nil
	c.SnapshotBaseSeq = 0
	c.SnapshotRowsDone = 0
	c.SnapshotRowsTotal = 0
}

// Store persists checkpoints. Save must be atomic per stream: a torn write would leave a
// position that was never reached.
type Store interface {
	// Load returns a stream's checkpoint, or ErrNotFound if it has none.
	Load(ctx context.Context, stream string) (Checkpoint, error)

	// Save writes the checkpoint, creating it if absent.
	Save(ctx context.Context, cp Checkpoint) error
}
