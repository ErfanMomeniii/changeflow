package supervisor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/telemetry"
)

type streamObserver struct {
	metrics *telemetry.Metrics
	state   *streamState
}

func (o *streamObserver) Event(operation cdc.Operation) { o.metrics.Event(operation) }

func (o *streamObserver) Lag(seconds float64) {
	o.metrics.Lag(seconds)
	o.state.observed(time.Now())
}

func (o *streamObserver) Batch(rows int) { o.metrics.Batch(rows) }

func (o *streamObserver) Write(applied, stale, rejected int, elapsed time.Duration, failed bool) {
	o.metrics.Write(applied, stale, rejected, elapsed, failed)
}

func (o *streamObserver) DeadLettered(n int) { o.metrics.DeadLettered(n) }

func (s *Supervisor) ready() error {
	s.mu.Lock()
	runtimes := s.running
	s.mu.Unlock()
	if len(runtimes) == 0 {
		return errors.New("not started yet")
	}
	for _, rt := range runtimes {
		streaming, lastEvent, lastErr := rt.state.snapshot()
		if lastErr != nil && !errors.Is(lastErr, context.Canceled) {
			return fmt.Errorf("stream %s: %w", rt.cfg.Name, lastErr)
		}
		if !streaming {
			return fmt.Errorf("stream %s is not streaming yet", rt.cfg.Name)
		}
		if !lastEvent.IsZero() {
			if behind := time.Since(lastEvent); behind > readinessLagLimit {
				return fmt.Errorf("stream %s last applied a change %s ago", rt.cfg.Name, behind.Round(time.Second))
			}
		}
	}
	return nil
}

func (s *Supervisor) reportQueueDepth(ctx context.Context, rt *streamRuntime) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rt.metrics.QueueDepth(len(rt.events))
		}
	}
}
