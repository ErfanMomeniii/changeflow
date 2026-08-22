package supervisor

import (
	"context"
	"sync"
)

type group struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	first  error
}

func newGroup(parent context.Context) (*group, context.Context) {
	ctx, cancel := context.WithCancel(parent)
	return &group{cancel: cancel}, ctx
}

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
			g.cancel()
		}
	}()
}

func (g *group) wait() error {
	g.wg.Wait()
	g.cancel()
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.first
}
