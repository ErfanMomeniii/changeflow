// Package cdc holds the values that flow between changeflow's stages.
//
// A ChangeEvent is source-shaped, carrying MySQL's columns in MySQL's order; a Doc is
// destination-shaped and already encoded. Keeping them separate is what lets a snapshot
// scan and a binlog stream share every stage downstream of the reader.
package cdc

import (
	"time"

	"github.com/ErfanMomeniii/changeflow/internal/schema"
)

// Operation is what happened to a row.
type Operation uint8

const (
	OperationInsert Operation = iota
	OperationUpdate
	OperationDelete
	OperationSnapshot
)

var operationNames = map[Operation]string{
	OperationInsert: "insert", OperationUpdate: "update", OperationDelete: "delete", OperationSnapshot: "snapshot",
}

func (o Operation) String() string {
	if name, ok := operationNames[o]; ok {
		return name
	}
	return "unknown"
}

// IsUpsert reports whether the operation writes a row rather than removing it.
func (o Operation) IsUpsert() bool {
	return o == OperationInsert || o == OperationUpdate || o == OperationSnapshot
}

// Row holds one row's column values in ordinal order, matching schema.TableMeta.Columns.
//
// Positional rather than keyed: a map per row costs an allocation per column plus a hash
// on every access, and the names are already known on the table definition.
type Row []any

// ChangeEvent is one observed change to one row.
type ChangeEvent struct {
	// Meta is shared by pointer and never copied.
	Meta *schema.TableMeta

	Operation Operation

	// Before is nil for an insert or a snapshot row; After is nil for a delete.
	Before Row
	After  Row

	// Timestamp is when the source recorded the change, and what lag is measured against.
	Timestamp time.Time

	GTID string
	TxID uint64

	// Seq is the version stamped on resulting documents. It must increase monotonically,
	// since it decides which of two writes to the same key wins.
	Seq uint64

	// Cursor is where a table scan resumes, set on the last event of each chunk.
	//
	// It travels on the event for the same reason a GTID does: only the stage that sees an
	// acknowledgement may record a position. A scanner recording progress on emit would
	// advance past rows that were never written.
	Cursor      []byte
	RowsScanned uint64
}

// Values returns the row to write from: the new values for an upsert, and the prior
// values for a delete, which is the only place a deleted row's key still exists.
func (e *ChangeEvent) Values() Row {
	if e.Operation == OperationDelete {
		return e.Before
	}
	return e.After
}

// Doc is one write to a destination, already selected, renamed, and encoded.
type Doc struct {
	// Key identifies the row. Composite keys are escaped before joining, so no two
	// distinct keys can produce the same string.
	Key string

	// Body is the encoded document, nil when Deleted, encoded once on the way out of
	// Transform rather than built as a map and marshalled later.
	Body []byte

	// Version decides which write wins: destinations discard anything not newer, which is
	// what makes a replay harmless.
	Version uint64

	// Deleted asks the destination to remove the key, in the same batch as upserts so
	// ordering within a key is preserved.
	Deleted bool
}

// Size estimates the document's contribution to a batch's byte budget.
func (d Doc) Size() int {
	return len(d.Key) + len(d.Body)
}
