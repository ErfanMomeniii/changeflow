package supervisor

import (
	"context"
	"sync"
)

// group runs several tasks and reports the first failure.
//
// Cancelling on the first error is the point: a stream that has stopped must not leave its
// siblings running against a reader that is no longer being consumed, since the router would
// block forever on its queue.
//
// errgroup from the extended libraries does the same thing, kept local to avoid a dependency
// for thirty lines.
type group struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu    sync.Mutex
	first error
}

func newGroup(parent context.Context) (*group, context.Context) {
	ctx, cancel := context.WithCancel(parent)
	return &group{cancel: cancel}, ctx
}

// run starts a task.
func (g *group) run(task func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if err := task(); err != nil {
			g.mu.Lock()
			if g.first == nil {
				g.first = err
			}
			g.mu.Unlock()
			// Bring the others down, rather than leaving them blocked on a queue nobody
			// is draining.
			g.cancel()
		}
	}()
}

// wait blocks until every task has finished and returns the first error.
func (g *group) wait() error {
	g.wg.Wait()
	g.cancel()

	g.mu.Lock()
	defer g.mu.Unlock()
	return g.first
}
