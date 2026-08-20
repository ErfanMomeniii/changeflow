package supervisor

// Health and metrics reporting for running streams.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/telemetry"
)

// streamObserver adapts one stream to what the pipeline reports, and keeps the state
// readiness is judged on up to date.
type streamObserver struct {
	metrics *telemetry.Metrics
	state   *streamState
}

func (o *streamObserver) Event(op cdc.Op) { o.metrics.Event(op) }

func (o *streamObserver) Lag(seconds float64) {
	o.metrics.Lag(seconds)
	o.state.observed(time.Now())
}

func (o *streamObserver) Batch(rows int) { o.metrics.Batch(rows) }

func (o *streamObserver) Write(applied, stale, rejected int, elapsed time.Duration, failed bool) {
	o.metrics.Write(applied, stale, rejected, elapsed, failed)
}

func (o *streamObserver) DeadLettered(n int) { o.metrics.DeadLettered(n) }

// ready reports whether every stream is fit to serve traffic.
//
// A stream that is merely behind is still alive, so this is readiness rather than liveness:
// restarting a lagging stream would only push it further behind.
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
		// A quiet table produces no events, so silence alone cannot mean unhealthy. Only
		// an event older than the threshold does, which means changes exist and are not
		// being applied.
		if !lastEvent.IsZero() {
			if behind := time.Since(lastEvent); behind > readinessLagLimit {
				return fmt.Errorf("stream %s last applied a change %s ago", rt.cfg.Name, behind.Round(time.Second))
			}
		}
	}
	return nil
}

// reportQueueDepth samples how many events are waiting for a stream.
//
// Pinned at the buffer size means the destination is setting the pace, which is the
// difference between a slow source and a slow sink.
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
