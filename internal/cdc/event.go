// Package cdc holds the values that flow between changeflow's stages.
//
// Two shapes cross stage boundaries: a ChangeEvent, which is source-shaped and
// carries MySQL's columns in MySQL's order, and a Doc, which is destination-shaped
// and already encoded. Keeping them separate is what lets a snapshot scan and a
// binlog stream share every stage downstream of the reader.
package cdc

import (
	"time"

	"github.com/ErfanMomeniii/changeflow/internal/schema"
)

// Op is what happened to a row.
type Op uint8

const (
	OpInsert Op = iota
	OpUpdate
	OpDelete
	// OpSnapshot is a row read by a table scan rather than observed changing. It
	// is applied as an upsert, and carries a version low enough that any streamed
	// change to the same row wins.
	OpSnapshot
)

var opNames = map[Op]string{
	OpInsert: "insert", OpUpdate: "update", OpDelete: "delete", OpSnapshot: "snapshot",
}

func (o Op) String() string {
	if name, ok := opNames[o]; ok {
		return name
	}
	return "unknown"
}

// IsUpsert reports whether the operation writes a row rather than removing it.
func (o Op) IsUpsert() bool {
	return o == OpInsert || o == OpUpdate || o == OpSnapshot
}

// Row holds one row's column values in ordinal order, matching
// schema.TableMeta.Columns.
//
// It is positional rather than keyed by name: a map per row costs an allocation
// per column plus a hash on every access, and the names are already known once,
// on the table definition.
type Row []any

// ChangeEvent is one observed change to one row.
type ChangeEvent struct {
	// Meta describes the table. It is shared by pointer and never copied.
	Meta *schema.TableMeta

	Op Op

	// Before holds the prior values, and is nil for an insert or a snapshot row.
	Before Row
	// After holds the new values, and is nil for a delete.
	After Row

	// Timestamp is when the source recorded the change, and is what replication
	// lag is measured against.
	Timestamp time.Time

	// GTID identifies the transaction, and is what gets checkpointed once a sink
	// acknowledges the write.
	GTID string
	// TxID is the transaction's XID, kept so writes could later be grouped.
	TxID uint64

	// Seq is the version stamped on resulting documents. It must increase
	// monotonically, since it is what decides which of two writes to the same key
	// wins in the destination.
	Seq uint64

	// Cursor is the position a table scan resumes after, set on the last event of
	// each chunk and empty otherwise.
	//
	// It travels on the event for the same reason a GTID does: only the stage that
	// sees an acknowledgement may record a position. A scanner recording its own
	// progress on emit would advance past rows that were never written, and nothing
	// would ever read them again.
	Cursor []byte
	// RowsScanned accompanies Cursor, for reporting scan progress.
	RowsScanned uint64
}

// Values returns the row a destination should be written from: the new values for
// an upsert, and the prior values for a delete, which is the only place a deleted
// row's key still exists.
func (e *ChangeEvent) Values() Row {
	if e.Op == OpDelete {
		return e.Before
	}
	return e.After
}

// Doc is one write to a destination, already selected, renamed, and encoded.
type Doc struct {
	// Key identifies the row in the destination. Composite keys are escaped and
	// joined, so no two distinct keys can produce the same string.
	Key string

	// Body is the encoded document, nil when Deleted. It is encoded once, on the
	// way out of Transform, rather than being built as a map and marshalled later.
	Body []byte

	// Version decides which write wins. Destinations compare it and discard
	// anything not newer, which is what makes a replay harmless.
	Version uint64

	// Deleted asks the destination to remove the key. It travels in the same batch
	// as upserts so ordering within a key is preserved.
	Deleted bool
}

// Size estimates the document's contribution to a batch's byte budget.
func (d Doc) Size() int {
	return len(d.Key) + len(d.Body)
}
