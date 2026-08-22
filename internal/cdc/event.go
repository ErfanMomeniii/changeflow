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
	Meta        *schema.TableMeta
	Operation   Operation
	Before      Row
	After       Row
	Timestamp   time.Time
	GTID        string
	TxID        uint64
	Seq         uint64
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
	Key     string
	Body    []byte
	Version uint64
	Deleted bool
}

// Size estimates the document's contribution to a batch's byte budget.
func (d Doc) Size() int {
	return len(d.Key) + len(d.Body)
}
