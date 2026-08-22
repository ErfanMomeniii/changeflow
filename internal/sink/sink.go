package sink

import (
	"context"
	"fmt"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
)

// Sink applies batches of documents to a destination.
type Sink interface {
	Write(ctx context.Context, docs []cdc.Doc) (Result, error)
	Close() error
}

// Result reports the outcome of one batch.
type Result struct {
	Applied  int
	Stale    int
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
