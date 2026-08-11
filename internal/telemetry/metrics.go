// Package telemetry reports what a stream is doing.
//
// The metric that matters is replication lag: everything else exists to explain a
// lag figure that is climbing. The set here is deliberately small, and each one
// answers a question an operator actually asks during an incident.
package telemetry

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
)

// Metrics records one stream's activity.
type Metrics struct {
	stream string

	lag             prometheus.Gauge
	events          *prometheus.CounterVec
	docsWritten     prometheus.Counter
	docsStale       prometheus.Counter
	sinkErrors      *prometheus.CounterVec
	writeDuration   prometheus.Histogram
	batchRows       prometheus.Histogram
	queueDepth      prometheus.Gauge
	deadLettered    prometheus.Counter
	checkpointAge   prometheus.Gauge
	snapshotDone    prometheus.Gauge
	snapshotTotal   prometheus.Gauge
	snapshotRunning prometheus.Gauge
}

// New registers a stream's metrics.
//
// Every series is labelled by stream, because a process may run several and an
// aggregate lag figure hides the one stream that is stuck.
func New(reg prometheus.Registerer, stream string) *Metrics {
	labels := prometheus.Labels{"stream": stream}
	factory := promauto{reg}

	return &Metrics{
		stream: stream,

		// The number to alert on. Measured from the source's own timestamp, so it
		// includes time spent queued inside changeflow as well as time in transit.
		lag: factory.gauge(prometheus.GaugeOpts{
			Name:        "changeflow_binlog_lag_seconds",
			Help:        "Seconds between the source recording a change and changeflow applying it.",
			ConstLabels: labels,
		}),

		events: factory.counterVec(prometheus.CounterOpts{
			Name:        "changeflow_events_total",
			Help:        "Change events read from the source, by operation.",
			ConstLabels: labels,
		}, []string{"op"}),

		docsWritten: factory.counter(prometheus.CounterOpts{
			Name:        "changeflow_documents_written_total",
			Help:        "Documents the destination accepted.",
			ConstLabels: labels,
		}),

		// Expected after a restart replays a batch. A steady stream of these at other
		// times means versions are not advancing as they should.
		docsStale: factory.counter(prometheus.CounterOpts{
			Name:        "changeflow_documents_stale_total",
			Help:        "Documents the destination already held at an equal or newer version.",
			ConstLabels: labels,
		}),

		sinkErrors: factory.counterVec(prometheus.CounterOpts{
			Name:        "changeflow_sink_errors_total",
			Help:        "Failed writes, separated into those worth retrying and those that are permanent.",
			ConstLabels: labels,
		}, []string{"kind"}),

		writeDuration: factory.histogram(prometheus.HistogramOpts{
			Name:        "changeflow_sink_write_duration_seconds",
			Help:        "Time spent writing one batch to the destination.",
			ConstLabels: labels,
			Buckets:     []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}),

		// Compared against the configured maximum: a distribution sitting far below it
		// means the flush interval is firing early, paying latency for no throughput.
		batchRows: factory.histogram(prometheus.HistogramOpts{
			Name:        "changeflow_batch_size_rows",
			Help:        "Documents per batch written to the destination.",
			ConstLabels: labels,
			Buckets:     []float64{1, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000},
		}),

		// Pinned at the configured buffer size means the destination, not the source,
		// is setting the pace.
		queueDepth: factory.gauge(prometheus.GaugeOpts{
			Name:        "changeflow_queue_depth",
			Help:        "Events waiting between the reader and the pipeline.",
			ConstLabels: labels,
		}),

		// Any increase deserves attention: these documents are not in the destination
		// and will not arrive without intervention.
		deadLettered: factory.counter(prometheus.CounterOpts{
			Name:        "changeflow_dead_lettered_total",
			Help:        "Documents recorded as permanently refused by the destination.",
			ConstLabels: labels,
		}),

		// Catches a stalled position even while events are flowing, which a lag figure
		// alone can miss.
		checkpointAge: factory.gauge(prometheus.GaugeOpts{
			Name:        "changeflow_checkpoint_age_seconds",
			Help:        "Seconds since the position was last recorded.",
			ConstLabels: labels,
		}),

		snapshotDone: factory.gauge(prometheus.GaugeOpts{
			Name:        "changeflow_snapshot_rows_done",
			Help:        "Rows read so far by the initial table scan.",
			ConstLabels: labels,
		}),

		// An estimate from table statistics, so a progress figure derived from it is
		// approximate by nature.
		snapshotTotal: factory.gauge(prometheus.GaugeOpts{
			Name:        "changeflow_snapshot_rows_estimated",
			Help:        "Estimated total rows for the initial table scan.",
			ConstLabels: labels,
		}),

		snapshotRunning: factory.gauge(prometheus.GaugeOpts{
			Name:        "changeflow_snapshot_running",
			Help:        "1 while the initial table scan is running, 0 otherwise.",
			ConstLabels: labels,
		}),
	}
}

// Event records one change read from the source.
func (m *Metrics) Event(op cdc.Op) {
	if m == nil {
		return
	}
	m.events.WithLabelValues(op.String()).Inc()
}

// Lag records how far behind the source a just-applied event was.
func (m *Metrics) Lag(seconds float64) {
	if m == nil {
		return
	}
	if seconds < 0 {
		// A source clock reading ahead of ours is not negative lag.
		seconds = 0
	}
	m.lag.Set(seconds)
}

// Batch records the size of a batch about to be written.
func (m *Metrics) Batch(rows int) {
	if m == nil {
		return
	}
	m.batchRows.Observe(float64(rows))
}

// Write records the outcome of one batch.
func (m *Metrics) Write(applied, stale, rejected int, elapsed time.Duration, failed bool) {
	if m == nil {
		return
	}
	m.writeDuration.Observe(elapsed.Seconds())
	m.docsWritten.Add(float64(applied))
	m.docsStale.Add(float64(stale))
	if rejected > 0 {
		m.sinkErrors.WithLabelValues("permanent").Add(float64(rejected))
	}
	if failed {
		m.sinkErrors.WithLabelValues("retryable").Inc()
	}
}

// DeadLettered records documents written to the dead letter queue.
func (m *Metrics) DeadLettered(n int) {
	if m == nil {
		return
	}
	m.deadLettered.Add(float64(n))
}

// QueueDepth records how many events are waiting to be processed.
func (m *Metrics) QueueDepth(n int) {
	if m == nil {
		return
	}
	m.queueDepth.Set(float64(n))
}

// CheckpointAge records how long ago the position was last written.
func (m *Metrics) CheckpointAge(d time.Duration) {
	if m == nil {
		return
	}
	m.checkpointAge.Set(d.Seconds())
}

// SnapshotProgress records how far the initial scan has read.
func (m *Metrics) SnapshotProgress(done, estimated uint64) {
	if m == nil {
		return
	}
	m.snapshotDone.Set(float64(done))
	m.snapshotTotal.Set(float64(estimated))
}

// SnapshotRunning records whether the initial scan is in progress.
func (m *Metrics) SnapshotRunning(running bool) {
	if m == nil {
		return
	}
	if running {
		m.snapshotRunning.Set(1)
		return
	}
	m.snapshotRunning.Set(0)
}

// promauto registers each collector as it is built, so a duplicate name fails at
// startup rather than being silently dropped.
type promauto struct {
	reg prometheus.Registerer
}

func (p promauto) gauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	g := prometheus.NewGauge(opts)
	p.mustRegister(g)
	return g
}

func (p promauto) counter(opts prometheus.CounterOpts) prometheus.Counter {
	c := prometheus.NewCounter(opts)
	p.mustRegister(c)
	return c
}

func (p promauto) counterVec(opts prometheus.CounterOpts, labels []string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(opts, labels)
	p.mustRegister(c)
	return c
}

func (p promauto) histogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	h := prometheus.NewHistogram(opts)
	p.mustRegister(h)
	return h
}

func (p promauto) mustRegister(c prometheus.Collector) {
	if p.reg == nil {
		return
	}
	if err := p.reg.Register(c); err != nil {
		// A registration clash is a programming error, and a metric silently missing
		// during an incident is worse than a refusal to start.
		panic("telemetry: register metric: " + err.Error())
	}
}
