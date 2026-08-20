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
	"github.com/ErfanMomeniii/changeflow/internal/source/binlog"
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

// New prepares a supervisor.
//
// Naming no stream runs every configured one, which is the normal case. Naming one runs it
// alone, which is how a noisy table is isolated from its siblings and how a stream that has
// fallen behind is caught up before rejoining them.
func New(cfg *config.Config, streamName, dlqDir string, log *slog.Logger) (*Supervisor, error) {
	if log == nil {
		log = slog.Default()
	}
	if dlqDir == "" {
		return nil, errors.New("supervisor: a dead letter directory is required, since a refused document must be recorded before its position advances")
	}

	var streams []*config.Stream
	if streamName != "" {
		stream, err := cfg.Stream(streamName)
		if err != nil {
			return nil, err
		}
		streams = []*config.Stream{stream}
	} else {
		// Sorted, so startup order and logs do not vary between runs.
		for _, name := range cfg.StreamNames() {
			streams = append(streams, cfg.Streams[name])
		}
	}
	if len(streams) == 0 {
		return nil, errors.New("supervisor: no streams are configured")
	}

	return &Supervisor{cfg: cfg, streams: streams, dlqDir: dlqDir, log: log}, nil
}

// Run starts every stream and blocks until the context ends or something fails.
func (s *Supervisor) Run(ctx context.Context) error {
	s.registry = prometheus.NewRegistry()
	// Memory and file descriptor figures are the first things asked about when a container
	// keeps restarting.
	s.registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	s.registry.MustRegister(collectors.NewGoCollector())

	if s.cfg.Runtime.MetricsEnabled() {
		server := telemetry.NewServer(s.cfg.Runtime.MetricsAddr, s.registry, telemetry.Health{Ready: s.ready})
		go func() {
			if err := server.Start(ctx); err != nil {
				// Losing observability is serious but not a reason to stop replicating.
				s.log.Error("metrics endpoint stopped", "error", err)
			}
		}()
		defer server.Shutdown()
		s.log.Info("serving metrics and health", "address", s.cfg.Runtime.MetricsAddr)
	}

	sourceDB, err := openMySQL(ctx, s.cfg.Source.DSN)
	if err != nil {
		return fmt.Errorf("supervisor: connect to source: %w", err)
	}
	defer sourceDB.Close()

	if err := s.checkSource(ctx, sourceDB); err != nil {
		return err
	}

	metaDB, err := openMySQL(ctx, s.cfg.Checkpoint.DSN)
	if err != nil {
		return fmt.Errorf("supervisor: connect to checkpoint store: %w", err)
	}
	defer metaDB.Close()

	store, err := checkpoint.NewMySQLStore(metaDB, s.cfg.Checkpoint.Table)
	if err != nil {
		return err
	}
	if err := store.EnsureSchema(ctx); err != nil {
		return err
	}

	schemas := schema.NewStore(schema.DBLoader{DB: sourceDB})

	runtimes, err := s.prepare(ctx, store, schemas)
	defer s.release(ctx, runtimes)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.running = runtimes
	s.mu.Unlock()

	// Scans run before streaming, one stream at a time. Reading two tables at once would
	// double the load a backfill puts on the source without finishing any sooner.
	positions := make(map[string]string, len(runtimes))
	for _, rt := range runtimes {
		position, err := s.snapshotIfNeeded(ctx, store, sourceDB, rt)
		if err != nil {
			return err
		}
		positions[rt.cfg.Name] = position
	}

	return s.stream(ctx, store, schemas, runtimes, positions)
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

// prepare builds each stream's parts, stopping at the first that cannot be built.
//
// Whatever was already created is returned even on failure, so locks and files are released
// rather than held until the process exits.
func (s *Supervisor) prepare(ctx context.Context, store *checkpoint.MySQLStore, schemas *schema.Store) ([]*streamRuntime, error) {
	zone, err := time.LoadLocation(s.cfg.Source.TimeZone)
	if err != nil {
		return nil, fmt.Errorf("supervisor: source.time_zone %q: %w", s.cfg.Source.TimeZone, err)
	}

	var runtimes []*streamRuntime
	for _, stream := range s.streams {
		rt := &streamRuntime{cfg: stream, metrics: telemetry.New(s.registry, stream.Name)}
		runtimes = append(runtimes, rt)

		// Two processes replicating one stream would double-write and fight over a server
		// id, and the resulting errors name neither culprit.
		lock, err := store.Lock(ctx, stream.Name)
		if err != nil {
			return runtimes, fmt.Errorf("supervisor: %w", err)
		}
		rt.lock = lock

		meta, err := schemas.Table(ctx, stream.Schema(), stream.TableName())
		if err != nil {
			return runtimes, fmt.Errorf("supervisor: %w", err)
		}
		rt.meta = meta

		dialect, err := dialectFor(stream.Sink.Type)
		if err != nil {
			return runtimes, err
		}

		// Compiling now surfaces an unmappable column or an impossible key before any
		// document is written.
		plan, err := pipeline.Compile(meta, stream.Mapping, dialect, zone, stream.Mapping.OnZeroDate)
		if err != nil {
			return runtimes, fmt.Errorf("supervisor: %w", err)
		}
		rt.plan = plan

		destination, err := buildSink(stream)
		if err != nil {
			return runtimes, err
		}
		rt.sink = destination

		// A field declared as the wrong type does not fail the write: it changes the
		// value, and correcting it later means rebuilding everything written meanwhile.
		if err := s.validateDestination(ctx, stream, destination, meta); err != nil {
			return runtimes, err
		}

		deadLetters, err := dlq.New(dlq.Options{Dir: s.dlqDir, Stream: stream.Name})
		if err != nil {
			return runtimes, err
		}
		rt.dlq = deadLetters

		alloc, err := checkpoint.NewAllocator(ctx, store, stream.Name, seqBlockSize, time.Now)
		if err != nil {
			return runtimes, err
		}
		rt.alloc = alloc

		runner, err := pipeline.NewRunner(pipeline.RunnerOptions{
			Stream: stream.Name,
			Plan:   plan,
			Sink:   destination,
			DLQ:    deadLetters,
			Store:  store,
			Limits: pipeline.Limits{
				MaxRows:       stream.Batch.MaxRows,
				MaxBytes:      stream.Batch.MaxBytes.Bytes(),
				FlushInterval: stream.Batch.FlushInterval.Duration(),
			},
			ShutdownGrace: s.cfg.Runtime.ShutdownGrace.Duration(),
			Observer:      &streamObserver{metrics: rt.metrics, state: &rt.state},
			Logger:        s.log,
		})
		if err != nil {
			return runtimes, err
		}
		rt.runner = runner
		rt.events = make(chan cdc.ChangeEvent, s.cfg.Runtime.BufferSize)
	}

	return runtimes, nil
}

// release gives up locks and closes destinations.
func (s *Supervisor) release(ctx context.Context, runtimes []*streamRuntime) {
	// Detached from the caller's context, which is usually already cancelled by the time
	// this runs.
	ctx = context.WithoutCancel(ctx)

	for _, rt := range runtimes {
		if rt.sink != nil {
			rt.sink.Close()
		}
		if rt.dlq != nil {
			rt.dlq.Close()
		}
		if rt.lock != nil {
			if err := rt.lock.Release(ctx); err != nil {
				s.log.Warn("could not release the stream lock", "stream", rt.cfg.Name, "error", err)
			}
		}
	}
}

// stream reads the source once and fans each change out to the streams that want it.
func (s *Supervisor) stream(
	ctx context.Context,
	store *checkpoint.MySQLStore,
	schemas *schema.Store,
	runtimes []*streamRuntime,
	positions map[string]string,
) error {
	// The shared position must be one no stream has passed, or a stream behind the others
	// would never receive the changes it still needs.
	startGTID, err := sharedStartPosition(positions)
	if err != nil {
		return err
	}

	router := NewRouter()
	for _, rt := range runtimes {
		router.Add(rt.cfg.Table, rt.events)
	}

	host, port, err := addressOf(s.cfg.Source.DSN)
	if err != nil {
		return err
	}

	// One reader, so one sequencer. Versions need only increase within a key, so streams
	// drawing from a shared sequence is not a problem.
	streamer, err := binlog.New(binlog.Options{
		Host:            host,
		Port:            port,
		User:            usernameOf(s.cfg.Source.DSN),
		Password:        passwordOf(s.cfg.Source.DSN),
		ServerID:        s.cfg.Source.ServerID,
		StartGTID:       startGTID,
		Tables:          router.Tables(),
		Schemas:         schemas,
		Sequencer:       runtimes[0].alloc,
		HeartbeatPeriod: s.cfg.Source.HeartbeatPeriod.Duration(),
		ReadTimeout:     s.cfg.Source.ReadTimeout.Duration(),
		Buffer:          s.cfg.Runtime.BufferSize,
		Logger:          s.log,
	})
	if err != nil {
		return err
	}
	defer streamer.Close()

	names := make([]string, 0, len(runtimes))
	for _, rt := range runtimes {
		names = append(names, rt.cfg.Name)
	}
	s.log.Info("streaming started",
		"streams", names, "tables", router.Tables(), "from", startGTID, "server_id", s.cfg.Source.ServerID)

	group, groupCtx := newGroup(ctx)

	// A pipeline per stream, each consuming its own queue.
	for _, rt := range runtimes {
		rt := rt
		rt.state.set(true, nil)
		// A previous run's failure is no longer the current state, and leaving it would
		// have status reporting an error that has been resolved.
		s.recordError(ctx, store, rt.cfg.Name, nil)
		group.run(func() error {
			err := rt.runner.Run(groupCtx, rt.events)
			rt.state.set(false, err)
			if err != nil && !errors.Is(err, context.Canceled) {
				s.recordError(ctx, store, rt.cfg.Name, err)
				return fmt.Errorf("stream %s: %w", rt.cfg.Name, err)
			}
			return nil
		})
		go s.reportQueueDepth(groupCtx, rt)
	}

	// The reader, fanning out to those pipelines.
	group.run(func() error {
		// Closing the queues is how each pipeline learns the source has ended and flushes
		// what it holds.
		defer router.Close()

		for ev := range streamer.Events(groupCtx) {
			if err := router.Route(groupCtx, ev); err != nil {
				return err
			}
		}
		if err := streamer.Err(); err != nil {
			// The reader serves every stream, so its failure is recorded against all of
			// them: each one has stopped, and each will be looked at on its own.
			for _, rt := range runtimes {
				s.recordError(ctx, store, rt.cfg.Name, err)
			}
			return fmt.Errorf("supervisor: source stopped: %w", err)
		}
		return nil
	})

	err = group.wait()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
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
