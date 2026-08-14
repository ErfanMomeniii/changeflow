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
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/checkpoint"
	"github.com/ErfanMomeniii/changeflow/internal/config"
	"github.com/ErfanMomeniii/changeflow/internal/pipeline"
	"github.com/ErfanMomeniii/changeflow/internal/preflight"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
	"github.com/ErfanMomeniii/changeflow/internal/sink"
	"github.com/ErfanMomeniii/changeflow/internal/sink/clickhouse"
	"github.com/ErfanMomeniii/changeflow/internal/sink/dlq"
	"github.com/ErfanMomeniii/changeflow/internal/sink/elasticsearch"
	"github.com/ErfanMomeniii/changeflow/internal/source/binlog"
	"github.com/ErfanMomeniii/changeflow/internal/source/snapshot"
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

// snapshotIfNeeded runs a stream's table scan when one is outstanding, and returns the
// position its streaming should begin from.
//
// The order is what makes a lock-free scan correct. The position is captured before the scan
// starts, so every change made during the scan is still in the binlog afterwards, and each
// such change carries a version above the one stamped on scanned rows. A row read by the
// scan and modified concurrently is therefore overwritten by the modification, never the
// other way round.
func (s *Supervisor) snapshotIfNeeded(
	ctx context.Context,
	store *checkpoint.MySQLStore,
	sourceDB *sql.DB,
	rt *streamRuntime,
) (string, error) {
	stream := rt.cfg

	cp, err := store.Load(ctx, stream.Name)
	if err != nil && !errors.Is(err, checkpoint.ErrNotFound) {
		// A position that cannot be read must never be guessed at: streaming from the
		// wrong place silently skips or duplicates data.
		return "", fmt.Errorf("supervisor: read checkpoint: %w", err)
	}

	// Already streaming: resume where the destination was last acknowledged.
	if cp.SnapshotDone && cp.GTIDSet != "" {
		return cp.GTIDSet, nil
	}

	if !stream.Snapshot.Enabled {
		if cp.GTIDSet != "" {
			return cp.GTIDSet, nil
		}
		position, err := currentPosition(ctx, sourceDB)
		if err != nil {
			return "", err
		}
		s.log.Warn("snapshots are disabled, so rows written before now will not appear in the destination",
			"stream", stream.Name, "from", position)
		cp.Stream, cp.SnapshotDone, cp.GTIDSet = stream.Name, true, position
		if err := store.Save(ctx, cp); err != nil {
			return "", fmt.Errorf("supervisor: record start position: %w", err)
		}
		return position, nil
	}

	// A first attempt captures the position and the version to stamp on scanned rows. A
	// resumed attempt keeps both, or the guarantee above would not hold.
	if cp.SnapshotStartGTID == "" {
		position, err := currentPosition(ctx, sourceDB)
		if err != nil {
			return "", err
		}
		baseSeq, err := rt.alloc.Next(ctx)
		if err != nil {
			return "", fmt.Errorf("supervisor: allocate the snapshot version: %w", err)
		}

		cp.Stream = stream.Name
		cp.SnapshotStartGTID = position
		cp.SnapshotBaseSeq = baseSeq
		cp.SnapshotRowsTotal = estimateRows(ctx, sourceDB, rt.meta)
		if err := store.Save(ctx, cp); err != nil {
			return "", fmt.Errorf("supervisor: record the snapshot start: %w", err)
		}
		s.log.Info("starting a table scan",
			"stream", stream.Name, "table", stream.Table,
			"from_position", position, "estimated_rows", cp.SnapshotRowsTotal)
	} else {
		s.log.Info("resuming a table scan",
			"stream", stream.Name, "rows_done", cp.SnapshotRowsDone, "estimated_rows", cp.SnapshotRowsTotal)
	}

	scanDB := sourceDB
	if s.cfg.Source.SnapshotDSN != "" {
		// A scan is the only part of changeflow that can slow the source down, so it can
		// be pointed at a replica instead.
		scanDB, err = openMySQL(ctx, s.cfg.Source.SnapshotDSN)
		if err != nil {
			return "", fmt.Errorf("supervisor: connect for the table scan: %w", err)
		}
		defer scanDB.Close()
	}

	key, err := rt.meta.ResolveKey(stream.Mapping.Key)
	if err != nil {
		return "", fmt.Errorf("supervisor: %w", err)
	}

	estimated := cp.SnapshotRowsTotal
	scanner, err := snapshot.New(snapshot.Options{
		DB:            scanDB,
		Meta:          rt.meta,
		Key:           key,
		ChunkSize:     stream.Snapshot.ChunkSize,
		MaxRowsPerSec: stream.Snapshot.MaxRateRowsPerSec,
		Cursor:        cp.SnapshotCursor,
		BaseSeq:       cp.SnapshotBaseSeq,
		Observe: func(rows uint64) {
			rt.metrics.SnapshotProgress(rows, estimated)
			s.log.Info("table scan progress", "stream", stream.Name, "rows", rows, "estimated_total", estimated)
		},
		Logger: s.log,
	})
	if err != nil {
		return "", err
	}

	rt.metrics.SnapshotRunning(true)
	defer rt.metrics.SnapshotRunning(false)

	if err := s.scan(ctx, rt, scanner); err != nil {
		return "", err
	}
	if !scanner.Done() {
		// Stopping short is not a failure, but the scan must not be recorded as complete,
		// or the rows it never reached would never be written.
		return "", ctx.Err()
	}

	cp, err = store.Load(ctx, stream.Name)
	if err != nil {
		return "", fmt.Errorf("supervisor: read checkpoint after the scan: %w", err)
	}
	cp.Stream = stream.Name
	cp.SnapshotDone = true
	cp.SnapshotRowsTotal = cp.SnapshotRowsDone
	if err := store.Save(ctx, cp); err != nil {
		return "", fmt.Errorf("supervisor: record the completed scan: %w", err)
	}
	s.log.Info("table scan complete", "stream", stream.Name, "rows", cp.SnapshotRowsDone)

	if err := s.promoteAlias(ctx, rt); err != nil {
		return "", err
	}

	// Streaming begins where the scan began, so every change made during it is applied on
	// top.
	return cp.SnapshotStartGTID, nil
}

// recordError keeps the reason a stream stopped where an operator will look for it: the
// checkpoint row, which `changeflow status` and the PHP package read.
//
// A nil error clears it. Detached from the given context because this runs precisely when
// something is shutting down, and best effort because failing to record why a stream
// stopped must not replace the reason it stopped.
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

// scan fills the destination from the table, with refreshing turned off for the duration
// where that is safe.
func (s *Supervisor) scan(ctx context.Context, rt *streamRuntime, scanner *snapshot.Snapshotter) error {
	load, err := s.beginBulkLoad(ctx, rt)
	if err != nil {
		// A scan that cannot change the destination's settings is slower, not wrong.
		s.log.Warn("could not prepare the destination for a bulk load",
			"stream", rt.cfg.Name, "error", err)
	}
	// Restored even when the scan stops early, so an interrupted rebuild does not leave
	// behind an index that never refreshes. Detached from the cancelled context, since
	// shutdown is exactly when this has to still happen.
	defer func() {
		if !load.Applied() {
			return
		}
		restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if err := rt.sink.(*elasticsearch.Sink).EndBulkLoad(restoreCtx, load); err != nil {
			s.log.Warn("could not restore the destination's settings after the scan",
				"stream", rt.cfg.Name, "index", rt.cfg.Sink.Index, "error", err)
		}
	}()

	if err := rt.runner.Run(ctx, scanner.Events(ctx)); err != nil {
		return err
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("supervisor: table scan: %w", err)
	}
	if !scanner.Done() {
		return nil
	}

	// Merged once the index is full, because a scan leaves far more segments behind than
	// an index that grew gradually, and search cost scales with their number.
	if load.Applied() {
		if err := rt.sink.(*elasticsearch.Sink).ForceMerge(ctx); err != nil {
			s.log.Warn("could not merge the scanned index's segments, which only makes it slower to search",
				"stream", rt.cfg.Name, "index", rt.cfg.Sink.Index, "error", err)
		}
	}
	return nil
}

// beginBulkLoad relaxes the destination's settings for a scan, but only when readers are
// not using the index being filled.
//
// An index with refreshing off answers searches with nothing, so this is only for an
// index being built behind an alias. Without an alias the index being filled is the one
// being read, and it is left alone.
func (s *Supervisor) beginBulkLoad(ctx context.Context, rt *streamRuntime) (elasticsearch.LoadSettings, error) {
	var none elasticsearch.LoadSettings

	es, ok := rt.sink.(*elasticsearch.Sink)
	if !ok || rt.cfg.Sink.Alias == "" {
		return none, nil
	}

	targets, err := es.AliasTargets(ctx)
	if err != nil {
		return none, err
	}
	for _, index := range targets {
		if index == rt.cfg.Sink.Index {
			return none, nil
		}
	}

	s.log.Info("filling a new index with refreshing off for the scan",
		"stream", rt.cfg.Name, "index", rt.cfg.Sink.Index, "readers_on", targets)
	return es.BeginBulkLoad(ctx)
}

// promoteAlias moves readers to the index a scan has just filled.
//
// Done after the scan rather than at startup, because until it finishes the index is
// incomplete and pointing readers at it would show them a half-built table.
func (s *Supervisor) promoteAlias(ctx context.Context, rt *streamRuntime) error {
	es, ok := rt.sink.(*elasticsearch.Sink)
	if !ok || rt.cfg.Sink.Alias == "" {
		return nil
	}

	before, err := es.AliasTargets(ctx)
	if err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	if err := es.PromoteAlias(ctx); err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	if len(before) != 1 || before[0] != rt.cfg.Sink.Index {
		s.log.Info("read alias moved to the newly scanned index",
			"stream", rt.cfg.Name, "alias", rt.cfg.Sink.Alias,
			"from", before, "to", rt.cfg.Sink.Index)
	}
	return nil
}

// streamObserver adapts one stream's metrics to what the pipeline reports, and keeps the
// state readiness is judged on up to date.
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

// validateDestination compares a destination's schema against what its stream will write.
func (s *Supervisor) validateDestination(ctx context.Context, stream *config.Stream, destination sink.Sink, meta *schema.TableMeta) error {
	es, ok := destination.(*elasticsearch.Sink)
	if !ok {
		// Nothing to check for a destination whose schema changeflow cannot read.
		return nil
	}

	key, err := meta.ResolveKey(stream.Mapping.Key)
	if err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	expected, err := schema.ExpectedElasticsearchFields(meta,
		stream.Mapping.Include, stream.Mapping.Exclude, key, stream.Mapping.Rename)
	if err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}

	if err := es.ValidateMapping(ctx, expected); err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	s.log.Info("destination schema matches the stream",
		"stream", stream.Name, "index", stream.Sink.Index, "fields", len(expected))
	return nil
}

func buildSink(stream *config.Stream) (sink.Sink, error) {
	switch stream.Sink.Type {
	case config.SinkElasticsearch:
		return elasticsearch.New(elasticsearch.Options{
			Addresses: stream.Sink.Addresses,
			Index:     stream.Sink.Index,
			Alias:     stream.Sink.Alias,
			Workers:   stream.Sink.Workers,
			Compress:  true,
		})

	case config.SinkClickHouse:
		return clickhouse.New(clickhouse.Options{
			DSN:      stream.Sink.DSN,
			Table:    stream.Sink.Table,
			Workers:  stream.Sink.Workers,
			Compress: true,
		})

	default:
		return nil, fmt.Errorf("supervisor: sink type %q is not implemented", stream.Sink.Type)
	}
}

func dialectFor(sinkType string) (pipeline.Dialect, error) {
	switch sinkType {
	case config.SinkElasticsearch:
		return pipeline.DialectElasticsearch, nil
	case config.SinkClickHouse:
		return pipeline.DialectClickHouse, nil
	default:
		return 0, fmt.Errorf("supervisor: no encoding known for sink type %q", sinkType)
	}
}

func currentPosition(ctx context.Context, db *sql.DB) (string, error) {
	var gtid string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&gtid); err != nil {
		return "", fmt.Errorf("supervisor: read source position: %w", err)
	}
	gtid = strings.ReplaceAll(gtid, "\n", "")
	if strings.TrimSpace(gtid) == "" {
		return "", errors.New("supervisor: the source has logged no transactions, so there is no position to start from")
	}
	return gtid, nil
}

// estimateRows reads the optimiser's row estimate, which drives a progress percentage only.
// It is approximate by nature, and nothing depends on its accuracy.
func estimateRows(ctx context.Context, db *sql.DB, meta *schema.TableMeta) uint64 {
	var rows sql.NullInt64
	err := db.QueryRowContext(ctx,
		"SELECT table_rows FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		meta.Schema, meta.Table).Scan(&rows)
	if err != nil || !rows.Valid || rows.Int64 < 0 {
		return 0
	}
	return uint64(rows.Int64)
}

func openMySQL(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	// One connection per stream for locks, plus a few for schema and checkpoint reads.
	db.SetMaxOpenConns(16)
	db.SetConnMaxLifetime(time.Hour)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// addressOf extracts the replication endpoint from a DSN. Replication uses its own
// connection rather than the SQL driver, so it needs the parts rather than the DSN.
func addressOf(dsn string) (string, uint16, error) {
	cfg, err := driver.ParseDSN(dsn)
	if err != nil {
		return "", 0, fmt.Errorf("supervisor: parse dsn: %w", err)
	}
	if cfg.Net != "tcp" {
		return "", 0, fmt.Errorf("supervisor: replication needs a tcp dsn, got %q", cfg.Net)
	}

	host, portStr, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return strings.Trim(cfg.Addr, "[]"), 3306, nil
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return "", 0, fmt.Errorf("supervisor: parse port in %q: %w", cfg.Addr, err)
	}
	return host, uint16(port), nil
}

func usernameOf(dsn string) string {
	if cfg, err := driver.ParseDSN(dsn); err == nil {
		return cfg.User
	}
	return ""
}

func passwordOf(dsn string) string {
	if cfg, err := driver.ParseDSN(dsn); err == nil {
		return cfg.Passwd
	}
	return ""
}
