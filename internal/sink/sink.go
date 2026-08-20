// Package sink writes documents to a destination.
//
// A sink knows nothing about MySQL. It receives encoded documents and applies them so the
// result does not depend on how many times, or in what order, a batch arrives — which
// at-least-once delivery makes unavoidable.
package sink

import (
	"context"
	"fmt"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
)

// Sink applies batches of documents to a destination.
type Sink interface {
	// Write applies a batch, reporting per-document outcomes in the Result and returning
	// an error only when the batch as a whole failed.
	//
	// An error means retry and do not advance the checkpoint. Per-document failures go in
	// the Result, so one malformed row does not block the rest.
	Write(ctx context.Context, docs []cdc.Doc) (Result, error)

	Close() error
}

// Result reports the outcome of one batch.
type Result struct {
	Applied int

	// Stale counts documents the destination already had at an equal or newer version,
	// which is what a replayed batch looks like and why replay is harmless.
	Stale int

	// Rejected holds documents refused permanently, such as a value the mapping cannot
	// hold. Retrying would fail again, so they belong in a dead letter queue.
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

// Total returns how many documents the result accounts for, which lets a caller assert
// that none were silently dropped.
func (r Result) Total() int {
	return r.Applied + r.Stale + len(r.Rejected)
}
