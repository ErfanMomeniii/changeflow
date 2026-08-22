// Package supervisor assembles streams from configuration and runs them.
//
// Everything here is wiring; the decisions live in the units it connects. What it does
// enforce is the order: nothing replicates until the source is known to be usable, each
// stream is known not to be running elsewhere, and every mapping is known to fit its
// destination. A failure at any of those points is a refusal to start rather than a problem
// discovered after several hundred thousand documents have been written.
package supervisor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/checkpoint"
	"github.com/ErfanMomeniii/changeflow/internal/config"
	"github.com/ErfanMomeniii/changeflow/internal/pipeline"
	"github.com/ErfanMomeniii/changeflow/internal/preflight"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
	"github.com/ErfanMomeniii/changeflow/internal/sink"
	"github.com/ErfanMomeniii/changeflow/internal/sink/dlq"
	"github.com/ErfanMomeniii/changeflow/internal/telemetry"
)

// seqBlockSize is how many versions are reserved per durable write. Large enough that
// reserving costs nothing measurable, small enough that a crash wastes a negligible slice
// of the sequence space.
const seqBlockSize = 10_000

// readinessLagLimit is how stale the last applied change may be before a stream stops
// reporting itself ready.
const readinessLagLimit = 5 * time.Minute

// Supervisor runs one or more streams from a single source.
//
// Every stream on a source shares one binlog connection. Ten streams would otherwise cost
// the master ten dump threads and ten copies of the same binlog, carrying the same data.
type Supervisor struct {
	cfg     *config.Config
	streams []*config.Stream
	log     *slog.Logger
	dlqDir  string

	registry *prometheus.Registry
	// metrics is one set of series per stream, keyed by stream name.
	metrics map[string]*telemetry.Metrics

	mu      sync.Mutex
	running []*streamRuntime
}

// streamRuntime is everything one stream needs while it runs.
type streamRuntime struct {
	cfg     *config.Stream
	meta    *schema.TableMeta
	plan    *pipeline.Plan
	sink    sink.Sink
	dlq     *dlq.Writer
	runner  *pipeline.Runner
	alloc   *checkpoint.Allocator
	lock    *checkpoint.StreamLock
	metrics *telemetry.Metrics
	// events is this stream's queue, which is what turns a slow destination into visible
	// lag rather than growing memory.
	events chan cdc.ChangeEvent
	state  streamState
}

// streamState is the little that health reporting needs to know.
type streamState struct {
	mu        sync.Mutex
	streaming bool
	lastEvent time.Time
	lastError error
}

func (s *streamState) set(streaming bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streaming = streaming
	s.lastError = err
}

func (s *streamState) observed(at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastEvent = at
}

func (s *streamState) snapshot() (bool, time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streaming, s.lastEvent, s.lastError
}

// New prepares a supervisor: it resolves which streams will run and registers everything
// they report through, so a run only has to connect and start them.
func New(cfg *config.Config, streamName, dlqDir string, log *slog.Logger) (*Supervisor, error) {
	if log == nil {
		log = slog.Default()
	}
	if dlqDir == "" {
		return nil, errors.New("supervisor: a dead letter directory is required, since a refused document must be recorded before its position advances")
	}

	streams, err := selectStreams(cfg, streamName)
	if err != nil {
		return nil, err
	}

	registry := prometheus.NewRegistry()
	// Memory and file descriptor figures are the first things asked about when a container
	// keeps restarting.
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registry.MustRegister(collectors.NewGoCollector())

	// Registered as the supervisor is built rather than as each stream starts, so a scrape
	// during a long startup reports every stream at zero instead of appearing to have none.
	metrics := make(map[string]*telemetry.Metrics, len(streams))
	for _, stream := range streams {
		metrics[stream.Name] = telemetry.New(registry, stream.Name)
	}

	return &Supervisor{
		cfg:      cfg,
		streams:  streams,
		log:      log,
		dlqDir:   dlqDir,
		registry: registry,
		metrics:  metrics,
	}, nil
}

// selectStreams resolves which of the configured streams a supervisor runs.
//
// Naming no stream runs every configured one, which is the normal case. Naming one runs it
// alone, which is how a noisy table is isolated from its siblings and how a stream that has
// fallen behind is caught up before rejoining them.
func selectStreams(cfg *config.Config, streamName string) ([]*config.Stream, error) {
	if streamName != "" {
		stream, err := cfg.Stream(streamName)
		if err != nil {
			return nil, err
		}
		return []*config.Stream{stream}, nil
	}

	// Sorted, so startup order and logs do not vary between runs.
	names := cfg.StreamNames()
	if len(names) == 0 {
		return nil, errors.New("supervisor: no streams are configured")
	}
	streams := make([]*config.Stream, 0, len(names))
	for _, name := range names {
		streams = append(streams, cfg.Streams[name])
	}
	return streams, nil
}

// Run starts every stream and blocks until the context ends or something fails.
//
// The sequence is the contract: report before working, refuse an unusable source, claim and
// validate every stream, finish any outstanding scan, and only then stream.
func (s *Supervisor) Run(ctx context.Context) error {
	stopTelemetry := s.serveTelemetry(ctx)
	defer stopTelemetry()

	sess, err := s.connect(ctx)
	if err != nil {
		return err
	}
	defer sess.close()

	if err := s.checkSource(ctx, sess.sourceDB); err != nil {
		return err
	}

	runtimes, err := s.prepare(ctx, sess)
	defer s.release(ctx, runtimes)
	if err != nil {
		return err
	}
	s.setRunning(runtimes)

	positions, err := s.backfill(ctx, sess, runtimes)
	if err != nil {
		return err
	}

	return s.stream(ctx, sess, runtimes, positions)
}

// session is what one run connects to: the source, the checkpoint store, and a schema cache
// reading from the source.
type session struct {
	sourceDB *sql.DB
	metaDB   *sql.DB
	store    *checkpoint.MySQLStore
	schemas  *schema.Store
}

func (s *Supervisor) connect(ctx context.Context) (sess *session, err error) {
	sess = &session{}
	// Whatever was opened before a failure is closed here rather than left to the process.
	defer func() {
		if err != nil {
			sess.close()
			sess = nil
		}
	}()

	sess.sourceDB, err = openMySQL(ctx, s.cfg.Source.DSN)
	if err != nil {
		return nil, fmt.Errorf("supervisor: connect to source: %w", err)
	}
	sess.schemas = schema.NewStore(schema.DBLoader{DB: sess.sourceDB})

	sess.metaDB, err = openMySQL(ctx, s.cfg.Checkpoint.DSN)
	if err != nil {
		return nil, fmt.Errorf("supervisor: connect to checkpoint store: %w", err)
	}
	if sess.store, err = checkpoint.NewMySQLStore(sess.metaDB, s.cfg.Checkpoint.Table); err != nil {
		return nil, err
	}
	if err = sess.store.EnsureSchema(ctx); err != nil {
		return nil, err
	}
	return sess, nil
}

func (sess *session) close() {
	if sess.sourceDB != nil {
		sess.sourceDB.Close()
	}
	if sess.metaDB != nil {
		sess.metaDB.Close()
	}
}

// serveTelemetry starts the metrics and health endpoint and returns the function that stops
// it. Nothing is served when metrics are turned off.
func (s *Supervisor) serveTelemetry(ctx context.Context) func() {
	if !s.cfg.Runtime.MetricsEnabled() {
		return func() {}
	}

	server := telemetry.NewServer(s.cfg.Runtime.MetricsAddr, s.registry, telemetry.Health{Ready: s.ready})
	go func() {
		if err := server.Start(ctx); err != nil {
			// Losing observability is serious but not a reason to stop replicating.
			s.log.Error("metrics endpoint stopped", "error", err)
		}
	}()
	s.log.Info("serving metrics and health", "address", s.cfg.Runtime.MetricsAddr)
	return func() {
		if err := server.Shutdown(); err != nil {
			s.log.Warn("could not stop the metrics endpoint", "error", err)
		}
	}
}

// checkSource refuses to replicate from a server that cannot support it.
func (s *Supervisor) checkSource(ctx context.Context, sourceDB *sql.DB) error {
	report, err := preflight.Run(ctx, preflight.DBReader{DB: sourceDB})
	if err != nil {
		return fmt.Errorf("supervisor: check source: %w", err)
	}
	if !report.OK() {
		for _, c := range report.Failures() {
			s.log.Error("source is not configured for change data capture",
				"check", c.Name, "want", c.Want, "got", c.Got, "why", c.Why)
		}
		return fmt.Errorf("supervisor: source failed %d required check(s)", len(report.Failures()))
	}
	for _, c := range report.Warnings() {
		s.log.Warn("source configuration warning", "check", c.Name, "want", c.Want, "got", c.Got, "why", c.Why)
	}

	// A replica claiming the source's own id is rejected by the server, and the resulting
	// error names neither side.
	if sourceID, ok := report.Get("server_id"); ok && sourceID == strconv.FormatUint(uint64(s.cfg.Source.ServerID), 10) {
		return fmt.Errorf("supervisor: source.server_id %d is the source's own id; replication needs a distinct one", s.cfg.Source.ServerID)
	}
	return nil
}

// setRunning publishes the prepared streams to health reporting.
func (s *Supervisor) setRunning(runtimes []*streamRuntime) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = runtimes
}

// recordError keeps the reason a stream stopped in the checkpoint row, where `changeflow
// status` and the PHP package read it. A nil error clears it.
//
// Detached from the given context, since this runs while something is shutting down, and
// best effort: failing to record why a stream stopped must not replace the reason.
func (s *Supervisor) recordError(ctx context.Context, store *checkpoint.MySQLStore, stream string, cause error) {
	reason := ""
	if cause != nil {
		reason = cause.Error()
	}

	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	if err := store.RecordError(writeCtx, stream, reason); err != nil {
		s.log.Warn("could not record the stream's state", "stream", stream, "error", err)
	}
}
