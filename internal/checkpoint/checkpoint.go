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
	Stream            string
	GTIDSet           string
	SnapshotDone      bool
	SnapshotStartGTID string
	SnapshotCursor    []byte
	SnapshotBaseSeq   uint64
	SnapshotRowsDone  uint64
	SnapshotRowsTotal uint64
	SeqWatermark      uint64
	LastEventTsMs     int64
	LastError         string
	SchemaVersion     int
	UpdatedAt         time.Time
}

// LagAt reports how far behind the source this checkpoint was at a given moment, and
// false when nothing has been applied: zero lag and no data must not look alike.
func (c Checkpoint) LagAt(now time.Time) (time.Duration, bool) {
	if c.LastEventTsMs == 0 {
		return 0, false
	}
	lag := now.Sub(time.UnixMilli(c.LastEventTsMs))
	if lag < 0 {
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
	Load(ctx context.Context, stream string) (Checkpoint, error)
	Save(ctx context.Context, cp Checkpoint) error
}
