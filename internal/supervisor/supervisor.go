// Package supervisor assembles a stream's parts from configuration and runs them.
//
// Everything here is wiring. The decisions live in the units it connects, and the
// one rule it enforces itself is the startup order: refuse to run before the source
// is known to be usable, the stream is known not to be running elsewhere, and the
// mapping is known to fit the destination.
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

// seqBlockSize is how many versions are reserved per durable write. Large enough
// that reserving costs nothing measurable, small enough that a crash wastes a
// negligible slice of the sequence space.
const seqBlockSize = 10_000

// Supervisor runs one stream.
type Supervisor struct {
	cfg    *config.Config
	stream *config.Stream
	log    *slog.Logger

	dlqDir string

	metrics *telemetry.Metrics
	// state is what readiness is judged on. A stream that is behind is still alive,
	// so this affects readiness only, never liveness.
	state streamState
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

// New prepares a supervisor for one configured stream.
func New(cfg *config.Config, streamName, dlqDir string, log *slog.Logger) (*Supervisor, error) {
	stream, err := cfg.Stream(streamName)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	if dlqDir == "" {
		return nil, errors.New("supervisor: a dead letter directory is required, since a refused document must be recorded before its position advances")
	}
	return &Supervisor{cfg: cfg, stream: stream, dlqDir: dlqDir, log: log}, nil
}

// Run starts the stream and blocks until the context ends or something fails.
func (s *Supervisor) Run(ctx context.Context) error {
	registry := prometheus.NewRegistry()
	// The process collector gives memory and file descriptor figures, which are the
	// first things asked about when a container is restarting.
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registry.MustRegister(collectors.NewGoCollector())
	s.metrics = telemetry.New(registry, s.stream.Name)

	if s.cfg.Runtime.MetricsAddr != "" {
		server := telemetry.NewServer(s.cfg.Runtime.MetricsAddr, registry, telemetry.Health{Ready: s.ready})
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

	// Refuse to start against a server that cannot support correct replication.
	// Discovering this later means data already written from bad decoding.
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
	if sourceID, ok := report.Get("server_id"); ok && sourceID == strconv.FormatUint(uint64(s.cfg.Source.ServerID), 10) {
		return fmt.Errorf("supervisor: source.server_id %d is the source's own id; replication needs a distinct one", s.cfg.Source.ServerID)
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

	// Two processes replicating one stream would double-write and fight over a
	// server id, and the resulting errors name neither culprit.
	lock, err := store.Lock(ctx, s.stream.Name)
	if err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	defer func() {
		if err := lock.Release(context.WithoutCancel(ctx)); err != nil {
			s.log.Warn("could not release the stream lock", "error", err)
		}
	}()

	schemas := schema.NewStore(schema.DBLoader{DB: sourceDB})
	meta, err := schemas.Table(ctx, s.stream.Schema(), s.stream.TableName())
	if err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}

	zone, err := time.LoadLocation(s.cfg.Source.TimeZone)
	if err != nil {
		return fmt.Errorf("supervisor: source.time_zone %q: %w", s.cfg.Source.TimeZone, err)
	}

	dialect, err := dialectFor(s.stream.Sink.Type)
	if err != nil {
		return err
	}

	// Compiling now surfaces an unmappable column or an impossible key before any
	// document is written.
	plan, err := pipeline.Compile(meta, s.stream.Mapping, dialect, zone, s.stream.Mapping.OnZeroDate)
	if err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}

	destination, err := s.buildSink()
	if err != nil {
		return err
	}
	defer destination.Close()

	// Checked before a single document is written. A field declared as the wrong type
	// does not fail the write: it changes the value, and correcting it afterwards means
	// rebuilding everything written in the meantime.
	if err := s.validateDestination(ctx, destination, meta); err != nil {
		return err
	}

	deadLetters, err := dlq.New(dlq.Options{Dir: s.dlqDir, Stream: s.stream.Name})
	if err != nil {
		return err
	}
	defer deadLetters.Close()

	allocator, err := checkpoint.NewAllocator(ctx, store, s.stream.Name, seqBlockSize, time.Now)
	if err != nil {
		return err
	}

	runner, err := pipeline.NewRunner(pipeline.RunnerOptions{
		Stream: s.stream.Name,
		Plan:   plan,
		Sink:   destination,
		DLQ:    deadLetters,
		Store:  store,
		Limits: pipeline.Limits{
			MaxRows:       s.stream.Batch.MaxRows,
			MaxBytes:      s.stream.Batch.MaxBytes.Bytes(),
			FlushInterval: s.stream.Batch.FlushInterval.Duration(),
		},
		ShutdownGrace: s.cfg.Runtime.ShutdownGrace.Duration(),
		Observer:      s.observer(),
		Logger:        s.log,
	})
	if err != nil {
		return err
	}

	// The scan runs first when a stream has never completed one. Rows written before
	// the stream existed produce no binlog events at all, so this is the only way
	// they reach the destination.
	startGTID, err := s.snapshotIfNeeded(ctx, store, sourceDB, allocator, runner, meta)
	if err != nil {
		return err
	}

	host, port, err := addressOf(s.cfg.Source.DSN)
	if err != nil {
		return err
	}

	streamer, err := binlog.New(binlog.Options{
		Host:            host,
		Port:            port,
		User:            usernameOf(s.cfg.Source.DSN),
		Password:        passwordOf(s.cfg.Source.DSN),
		ServerID:        s.cfg.Source.ServerID,
		StartGTID:       startGTID,
		Tables:          []string{s.stream.Table},
		Schemas:         schemas,
		Sequencer:       allocator,
		HeartbeatPeriod: s.cfg.Source.HeartbeatPeriod.Duration(),
		ReadTimeout:     s.cfg.Source.ReadTimeout.Duration(),
		Buffer:          s.cfg.Runtime.BufferSize,
		Logger:          s.log,
	})
	if err != nil {
		return err
	}
	defer streamer.Close()

	s.log.Info("stream started",
		"stream", s.stream.Name, "table", s.stream.Table,
		"sink", s.stream.Sink.Type, "from", startGTID)

	events := streamer.Events(ctx)
	s.state.set(true, nil)
	go s.reportQueueDepth(ctx, events)

	runErr := runner.Run(ctx, events)
	s.state.set(false, runErr)

	// A reader failure is the more useful diagnosis: the runner usually just sees
	// its input end.
	if readErr := streamer.Err(); readErr != nil {
		return fmt.Errorf("supervisor: source stopped: %w", readErr)
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return runErr
	}
	return nil
}

// snapshotIfNeeded runs the table scan when one is outstanding, and returns the
// position streaming should begin from.
//
// The order is what makes a lock-free scan correct. The position is captured before
// the scan starts, so every change made during the scan is still in the binlog
// afterwards, and each such change carries a version above the one stamped on
// scanned rows. A row read by the scan and modified concurrently is therefore
// overwritten by the modification, never the other way round.
func (s *Supervisor) snapshotIfNeeded(
	ctx context.Context,
	store *checkpoint.MySQLStore,
	sourceDB *sql.DB,
	allocator *checkpoint.Allocator,
	runner *pipeline.Runner,
	meta *schema.TableMeta,
) (string, error) {
	cp, err := store.Load(ctx, s.stream.Name)
	if err != nil && !errors.Is(err, checkpoint.ErrNotFound) {
		// A position that cannot be read must never be guessed at: streaming from the
		// wrong place silently skips or duplicates data.
		return "", fmt.Errorf("supervisor: read checkpoint: %w", err)
	}

	// Already streaming: resume where the destination was last acknowledged.
	if cp.SnapshotDone && cp.GTIDSet != "" {
		return cp.GTIDSet, nil
	}

	if !s.stream.Snapshot.Enabled {
		if cp.GTIDSet != "" {
			return cp.GTIDSet, nil
		}
		position, err := currentPosition(ctx, sourceDB)
		if err != nil {
			return "", err
		}
		s.log.Warn("snapshots are disabled, so rows written before now will not appear in the destination",
			"stream", s.stream.Name, "from", position)
		cp.Stream, cp.SnapshotDone, cp.GTIDSet = s.stream.Name, true, position
		if err := store.Save(ctx, cp); err != nil {
			return "", fmt.Errorf("supervisor: record start position: %w", err)
		}
		return position, nil
	}

	// A first attempt captures the position and the version to stamp on scanned
	// rows. A resumed attempt keeps both, or the guarantee above would not hold.
	if cp.SnapshotStartGTID == "" {
		position, err := currentPosition(ctx, sourceDB)
		if err != nil {
			return "", err
		}
		baseSeq, err := allocator.Next(ctx)
		if err != nil {
			return "", fmt.Errorf("supervisor: allocate the snapshot version: %w", err)
		}

		cp.Stream = s.stream.Name
		cp.SnapshotStartGTID = position
		cp.SnapshotBaseSeq = baseSeq
		cp.SnapshotRowsTotal = estimateRows(ctx, sourceDB, meta)
		if err := store.Save(ctx, cp); err != nil {
			return "", fmt.Errorf("supervisor: record the snapshot start: %w", err)
		}
		s.log.Info("starting a table scan",
			"stream", s.stream.Name, "table", s.stream.Table,
			"from_position", position, "estimated_rows", cp.SnapshotRowsTotal)
	} else {
		s.log.Info("resuming a table scan",
			"stream", s.stream.Name, "rows_done", cp.SnapshotRowsDone,
			"estimated_rows", cp.SnapshotRowsTotal)
	}

	scanDB := sourceDB
	if s.cfg.Source.SnapshotDSN != "" {
		// A scan is the only part of changeflow that can slow the source down, so it
		// can be pointed at a replica instead.
		scanDB, err = openMySQL(ctx, s.cfg.Source.SnapshotDSN)
		if err != nil {
			return "", fmt.Errorf("supervisor: connect for the table scan: %w", err)
		}
		defer scanDB.Close()
	}

	key, err := meta.ResolveKey(s.stream.Mapping.Key)
	if err != nil {
		return "", fmt.Errorf("supervisor: %w", err)
	}

	scanner, err := snapshot.New(snapshot.Options{
		DB:            scanDB,
		Meta:          meta,
		Key:           key,
		ChunkSize:     s.stream.Snapshot.ChunkSize,
		MaxRowsPerSec: s.stream.Snapshot.MaxRateRowsPerSec,
		Cursor:        cp.SnapshotCursor,
		BaseSeq:       cp.SnapshotBaseSeq,
		Observe: func(rows uint64) {
			s.metrics.SnapshotProgress(rows, cp.SnapshotRowsTotal)
			s.log.Info("table scan progress",
				"stream", s.stream.Name, "rows", rows, "estimated_total", cp.SnapshotRowsTotal)
		},
		Logger: s.log,
	})
	if err != nil {
		return "", err
	}

	s.metrics.SnapshotRunning(true)
	defer s.metrics.SnapshotRunning(false)

	if err := runner.Run(ctx, scanner.Events(ctx)); err != nil {
		return "", err
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("supervisor: table scan: %w", err)
	}
	if !scanner.Done() {
		// Stopping short is not a failure, but the scan must not be recorded as
		// complete, or the rows it never reached would never be written.
		return "", ctx.Err()
	}

	cp, err = store.Load(ctx, s.stream.Name)
	if err != nil {
		return "", fmt.Errorf("supervisor: read checkpoint after the scan: %w", err)
	}
	cp.Stream = s.stream.Name
	cp.SnapshotDone = true
	cp.SnapshotRowsTotal = cp.SnapshotRowsDone
	if err := store.Save(ctx, cp); err != nil {
		return "", fmt.Errorf("supervisor: record the completed scan: %w", err)
	}
	s.log.Info("table scan complete", "stream", s.stream.Name, "rows", cp.SnapshotRowsDone)

	// Streaming begins where the scan began, so every change made during it is
	// applied on top.
	return cp.SnapshotStartGTID, nil
}

// observer adapts the metrics recorder to what the pipeline reports, and keeps the
// state readiness is judged on up to date.
func (s *Supervisor) observer() pipeline.Observer {
	return &supervisorObserver{metrics: s.metrics, state: &s.state}
}

type supervisorObserver struct {
	metrics *telemetry.Metrics
	state   *streamState
}

func (o *supervisorObserver) Event(op cdc.Op) { o.metrics.Event(op) }

func (o *supervisorObserver) Lag(seconds float64) {
	o.metrics.Lag(seconds)
	o.state.observed(time.Now())
}

func (o *supervisorObserver) Batch(rows int) { o.metrics.Batch(rows) }

func (o *supervisorObserver) Write(applied, stale, rejected int, elapsed time.Duration, failed bool) {
	o.metrics.Write(applied, stale, rejected, elapsed, failed)
}

func (o *supervisorObserver) DeadLettered(n int) { o.metrics.DeadLettered(n) }

// ready reports whether the stream is fit to serve traffic.
//
// A stream that is merely behind is still alive, so this is readiness rather than
// liveness: restarting a lagging stream would only push it further behind.
func (s *Supervisor) ready() error {
	streaming, lastEvent, lastErr := s.state.snapshot()
	if lastErr != nil {
		return lastErr
	}
	if !streaming {
		return errors.New("not streaming yet")
	}
	// A quiet table produces no events, so silence alone cannot mean unhealthy. Only
	// an event older than the threshold does, which means changes exist and are not
	// being applied.
	if !lastEvent.IsZero() {
		if behind := time.Since(lastEvent); behind > readinessLagLimit {
			return fmt.Errorf("last change applied %s ago", behind.Round(time.Second))
		}
	}
	return nil
}

// readinessLagLimit is how stale the last applied change may be before a stream stops
// reporting itself ready.
const readinessLagLimit = 5 * time.Minute

// reportQueueDepth samples how many events are waiting.
//
// Pinned at the buffer size means the destination is setting the pace, which is the
// difference between a slow source and a slow sink.
func (s *Supervisor) reportQueueDepth(ctx context.Context, events <-chan cdc.ChangeEvent) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.metrics.QueueDepth(len(events))
		}
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

// estimateRows reads the optimiser's row estimate, which drives a progress
// percentage only. It is approximate by nature, and nothing depends on its accuracy.
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

// validateDestination compares the destination's schema against what this stream will
// write.
func (s *Supervisor) validateDestination(ctx context.Context, destination sink.Sink, meta *schema.TableMeta) error {
	es, ok := destination.(*elasticsearch.Sink)
	if !ok {
		// Nothing to check for a destination whose schema changeflow cannot read.
		return nil
	}

	key, err := meta.ResolveKey(s.stream.Mapping.Key)
	if err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	expected, err := schema.ExpectedElasticsearchFields(meta,
		s.stream.Mapping.Include, s.stream.Mapping.Exclude, key, s.stream.Mapping.Rename)
	if err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}

	if err := es.ValidateMapping(ctx, expected); err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	s.log.Info("destination schema matches the stream",
		"stream", s.stream.Name, "index", s.stream.Sink.Index, "fields", len(expected))
	return nil
}

func (s *Supervisor) buildSink() (sink.Sink, error) {
	switch s.stream.Sink.Type {
	case config.SinkElasticsearch:
		return elasticsearch.New(elasticsearch.Options{
			Addresses: s.stream.Sink.Addresses,
			Index:     s.stream.Sink.Index,
			Alias:     s.stream.Sink.Alias,
			Workers:   s.stream.Sink.Workers,
			Compress:  true,
		})
	case config.SinkClickHouse:
		return clickhouse.New(clickhouse.Options{
			DSN:      s.stream.Sink.DSN,
			Table:    s.stream.Sink.Table,
			Workers:  s.stream.Sink.Workers,
			Compress: true,
		})

	default:
		return nil, fmt.Errorf("supervisor: sink type %q is not implemented", s.stream.Sink.Type)
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

func openMySQL(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
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
