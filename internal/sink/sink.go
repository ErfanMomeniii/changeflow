// Package sink writes documents to a destination.
//
// A sink knows nothing about MySQL. It receives already-encoded documents and is
// responsible for exactly one thing: applying them so that the result does not
// depend on how many times, or in what order, a batch is delivered. Every sink
// must satisfy that contract, because at-least-once delivery means a batch can
// arrive twice and an older write can arrive after a newer one.
package sink

import (
	"context"
	"fmt"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
)

// Sink applies batches of documents to a destination.
type Sink interface {
	// Write applies a batch. It returns a Result describing what happened to each
	// document, and an error only when the batch as a whole could not be applied.
	//
	// A returned error means the caller should retry the batch and must not
	// advance its checkpoint. Per-document failures are reported in the Result
	// instead, since one malformed row must not block the rest.
	Write(ctx context.Context, docs []cdc.Doc) (Result, error)

	// Close releases resources.
	Close() error
}

// Result reports the outcome of one batch.
type Result struct {
	// Applied counts documents the destination accepted.
	Applied int

	// Stale counts documents the destination already had at an equal or newer
	// version. These are expected after a restart replays a batch, and are the
	// mechanism that makes replay harmless rather than a problem to solve.
	Stale int

	// Rejected holds documents the destination refused permanently, such as a
	// value that conflicts with the index mapping. Retrying would fail again, so
	// they belong in a dead letter queue.
	Rejected []Rejection
}

// Rejection is one permanently failed document.
type Rejection struct {
	Doc    cdc.Doc
	Status int
	Reason string
}

func (r Rejection) String() string {
	return fmt.Sprintf("%s: status %d: %s", r.Doc.Key, r.Status, r.Reason)
}

// Total returns how many documents the result accounts for, which lets a caller
// assert that none were silently dropped.
func (r Result) Total() int {
	return r.Applied + r.Stale + len(r.Rejected)
}
