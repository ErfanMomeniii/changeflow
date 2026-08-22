package telemetry

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
)

func newTestMetrics(t *testing.T) (*Metrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	return New(reg, "orders_to_es"), reg
}

func gathered(t *testing.T, reg *prometheus.Registry) string {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var b strings.Builder
	for _, f := range families {
		b.WriteString(f.GetName())
		b.WriteByte('\n')
	}
	return b.String()
}

func TestMetricsAreRegisteredUnderTheirDocumentedNames(t *testing.T) {
	m, reg := newTestMetrics(t)
	m.Lag(1.5)
	m.Event(cdc.OperationInsert)
	m.Batch(100)
	m.Write(90, 5, 5, 20*time.Millisecond, false)
	m.DeadLettered(1)
	m.QueueDepth(3)
	m.CheckpointAge(2 * time.Second)
	m.SnapshotProgress(10, 100)
	m.SnapshotRunning(true)
	names := gathered(t, reg)
	for _, want := range []string{
		"changeflow_binlog_lag_seconds",
		"changeflow_events_total",
		"changeflow_documents_written_total",
		"changeflow_documents_stale_total",
		"changeflow_sink_errors_total",
		"changeflow_sink_write_duration_seconds",
		"changeflow_batch_size_rows",
		"changeflow_queue_depth",
		"changeflow_dead_lettered_total",
		"changeflow_checkpoint_age_seconds",
		"changeflow_snapshot_rows_done",
		"changeflow_snapshot_rows_estimated",
		"changeflow_snapshot_running",
	} {
		if !strings.Contains(names, want) {
			t.Errorf("metric %s is missing from a scrape", want)
		}
	}
}

// Several streams can run in one process, and an aggregate lag figure would hide the
// one stream that is stuck.
func TestMetricsAreLabelledByStream(t *testing.T) {
	reg := prometheus.NewRegistry()
	first := New(reg, "orders_to_es")
	second := New(reg, "users_to_es")
	first.Lag(10)
	second.Lag(2)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	byStream := map[string]float64{}
	for _, f := range families {
		if f.GetName() != "changeflow_binlog_lag_seconds" {
			continue
		}
		for _, metric := range f.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "stream" {
					byStream[label.GetValue()] = metric.GetGauge().GetValue()
				}
			}
		}
	}
	if byStream["orders_to_es"] != 10 {
		t.Errorf("orders_to_es lag = %v, want 10", byStream["orders_to_es"])
	}
	if byStream["users_to_es"] != 2 {
		t.Errorf("users_to_es lag = %v, want 2", byStream["users_to_es"])
	}
}

func TestWriteRecordsOutcomesSeparately(t *testing.T) {
	m, _ := newTestMetrics(t)
	m.Write(10, 3, 2, 5*time.Millisecond, false)
	if got := testutil.ToFloat64(m.docsWritten); got != 10 {
		t.Errorf("written = %v, want 10", got)
	}
	if got := testutil.ToFloat64(m.docsStale); got != 3 {
		t.Errorf("stale = %v, want 3", got)
	}
	if got := testutil.ToFloat64(m.sinkErrors.WithLabelValues("permanent")); got != 2 {
		t.Errorf("permanent errors = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.sinkErrors.WithLabelValues("retryable")); got != 0 {
		t.Errorf("retryable errors = %v, want 0", got)
	}
}

// A retryable failure and a permanently refused document need different responses, so
// they must not share a series.
func TestRetryableFailuresAreCountedApartFromRefusals(t *testing.T) {
	m, _ := newTestMetrics(t)
	m.Write(0, 0, 0, time.Millisecond, true)
	if got := testutil.ToFloat64(m.sinkErrors.WithLabelValues("retryable")); got != 1 {
		t.Errorf("retryable errors = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.sinkErrors.WithLabelValues("permanent")); got != 0 {
		t.Errorf("permanent errors = %v, want 0", got)
	}
}

// A source clock reading ahead of ours is not negative lag, and a negative gauge would
// break an alert threshold.
func TestNegativeLagIsClamped(t *testing.T) {
	m, _ := newTestMetrics(t)
	m.Lag(-5)
	if got := testutil.ToFloat64(m.lag); got != 0 {
		t.Errorf("lag = %v, want 0", got)
	}
}

func TestEventsAreCountedByOperation(t *testing.T) {
	m, _ := newTestMetrics(t)
	m.Event(cdc.OperationInsert)
	m.Event(cdc.OperationInsert)
	m.Event(cdc.OperationDelete)
	m.Event(cdc.OperationSnapshot)
	if got := testutil.ToFloat64(m.events.WithLabelValues("insert")); got != 2 {
		t.Errorf("inserts = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.events.WithLabelValues("delete")); got != 1 {
		t.Errorf("deletes = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.events.WithLabelValues("snapshot")); got != 1 {
		t.Errorf("snapshot rows = %v, want 1", got)
	}
}

// Metrics are optional, so every method must tolerate a nil receiver rather than
// forcing callers to guard each call.
func TestNilMetricsAreSafe(t *testing.T) {
	var m *Metrics
	m.Event(cdc.OperationInsert)
	m.Lag(1)
	m.Batch(10)
	m.Write(1, 0, 0, time.Millisecond, false)
	m.DeadLettered(1)
	m.QueueDepth(1)
	m.CheckpointAge(time.Second)
	m.SnapshotProgress(1, 2)
	m.SnapshotRunning(false)
}

// A metric silently missing during an incident is worse than a refusal to start.
func TestDuplicateRegistrationFailsLoudly(t *testing.T) {
	reg := prometheus.NewRegistry()
	New(reg, "orders_to_es")
	defer func() {
		if recover() == nil {
			t.Fatal("expected registering the same stream twice to fail")
		}
	}()
	New(reg, "orders_to_es")
}

func startTestServer(t *testing.T, health Health) *Server {
	t.Helper()
	reg := prometheus.NewRegistry()
	New(reg, "orders_to_es")
	s := NewServer("127.0.0.1:0", reg, health)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = s.Start(ctx)
	}()
	<-ready
	for i := 0; i < 100; i++ {
		if strings.HasPrefix(s.Addr(), "127.0.0.1:") && !strings.HasSuffix(s.Addr(), ":0") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Cleanup(func() { s.Shutdown() })
	return s
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestMetricsEndpointServesAScrape(t *testing.T) {
	s := startTestServer(t, Health{})
	status, body := get(t, "http://"+s.Addr()+"/metrics")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if !strings.Contains(body, "changeflow_binlog_lag_seconds") {
		t.Errorf("scrape does not include our metrics:\n%s", body)
	}
}

// Liveness must not depend on lag, or a stream that has fallen behind would be
// restarted repeatedly and never given the chance to catch up.
func TestLivenessIgnoresReadiness(t *testing.T) {
	s := startTestServer(t, Health{
		Ready: func() error { return errors.New("lag is 900 seconds") },
	})
	if status, _ := get(t, "http://"+s.Addr()+"/healthz"); status != http.StatusOK {
		t.Errorf("healthz status = %d, want 200 even when not ready", status)
	}
	status, body := get(t, "http://"+s.Addr()+"/readyz")
	if status != http.StatusServiceUnavailable {
		t.Errorf("readyz status = %d, want 503", status)
	}
	if !strings.Contains(body, "lag is 900 seconds") {
		t.Errorf("readyz should explain itself, got %q", body)
	}
}

func TestReadinessSucceedsWhenHealthy(t *testing.T) {
	s := startTestServer(t, Health{Ready: func() error { return nil }})
	if status, _ := get(t, "http://"+s.Addr()+"/readyz"); status != http.StatusOK {
		t.Errorf("readyz status = %d, want 200", status)
	}
}

// A port already in use must fail at startup, rather than leaving a process running
// with no way to observe it.
func TestStartFailsWhenThePortIsTaken(t *testing.T) {
	first := startTestServer(t, Health{})
	reg := prometheus.NewRegistry()
	New(reg, "second_stream")
	second := NewServer(first.Addr(), reg, Health{})
	if err := second.Start(context.Background()); err == nil {
		second.Shutdown()
		t.Fatal("expected a busy port to fail")
	}
}

func TestShutdownStopsServing(t *testing.T) {
	s := startTestServer(t, Health{})
	addr := s.Addr()
	if err := s.Shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if _, err := http.Get("http://" + addr + "/healthz"); err == nil {
		t.Error("expected requests to fail after shutdown")
	}
}

func TestShutdownWithoutStartIsHarmless(t *testing.T) {
	if err := NewServer("127.0.0.1:0", prometheus.NewRegistry(), Health{}).Shutdown(); err != nil {
		t.Fatalf("shutdown before start: %v", err)
	}
}
