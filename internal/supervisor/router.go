package supervisor

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
)

// Router delivers each change to the streams configured for its table.
//
// One binlog connection serves every stream on a source, rather than one dump thread and
// one copy of the binlog per stream. A table watched by two streams receives the event
// twice, since each has its own mapping, batching, and position.
type Router struct {
	// byTable is keyed by lowercased database.table.
	byTable map[string][]chan cdc.ChangeEvent
	// closeOnce guards Close, which the reader loop and a failure path may both reach.
	closeOnce sync.Once
	channels  []chan cdc.ChangeEvent
}

// NewRouter builds a router over the given table-to-channel assignments.
func NewRouter() *Router {
	return &Router{byTable: make(map[string][]chan cdc.ChangeEvent)}
}

// Add registers a channel to receive changes for a table.
func (r *Router) Add(table string, out chan cdc.ChangeEvent) {
	key := strings.ToLower(table)
	r.byTable[key] = append(r.byTable[key], out)
	r.channels = append(r.channels, out)
}

// Tables returns every watched table, which is what the reader filters on.
func (r *Router) Tables() []string {
	tables := make([]string, 0, len(r.byTable))
	for table := range r.byTable {
		tables = append(tables, table)
	}
	return tables
}

// Route delivers one event to every stream watching its table.
//
// Sending blocks when a queue is full, which is how back pressure reaches the reader.
// With a shared reader that pressure is shared: a slow destination on one stream slows
// the others, and the queue depth metric is what makes it visible.
func (r *Router) Route(ctx context.Context, ev cdc.ChangeEvent) error {
	if ev.Meta == nil {
		return nil
	}

	targets := r.byTable[strings.ToLower(ev.Meta.Name())]
	for _, out := range targets {
		select {
		case out <- ev:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Close closes every queue, which is how each pipeline learns to flush and finish.
func (r *Router) Close() {
	r.closeOnce.Do(func() {
		for _, out := range r.channels {
			close(out)
		}
	})
}

// sharedStartPosition picks the position a shared reader must start from.
//
// It has to be one no stream has passed, or a stream behind the others would never receive
// the changes it still needs. Re-delivering what a stream ahead already applied is
// harmless, since versions only increase. Streams that cannot be reconciled this way are
// refused rather than silently having changes skipped.
func sharedStartPosition(positions map[string]string) (string, error) {
	if len(positions) == 0 {
		return "", fmt.Errorf("supervisor: no stream provided a start position")
	}

	parsed := make(map[string]mysql.GTIDSet, len(positions))
	for stream, text := range positions {
		set, err := mysql.ParseMysqlGTIDSet(text)
		if err != nil {
			return "", fmt.Errorf("supervisor: stream %s has an unreadable position %q: %w", stream, text, err)
		}
		parsed[stream] = set
	}

	for stream, candidate := range parsed {
		containedInAll := true
		for other, set := range parsed {
			if other == stream {
				continue
			}
			if !set.Contain(candidate) {
				containedInAll = false
				break
			}
		}
		if containedInAll {
			return positions[stream], nil
		}
	}

	var described []string
	for stream, text := range positions {
		described = append(described, fmt.Sprintf("%s at %s", stream, text))
	}
	return "", fmt.Errorf("supervisor: these streams have diverged and no single position serves them all (%s); "+
		"run the stream that is behind on its own with --stream until it catches up, then start them together",
		strings.Join(described, ", "))
}
