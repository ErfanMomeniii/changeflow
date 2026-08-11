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
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/ErfanMomeniii/changeflow/internal/checkpoint"
	"github.com/ErfanMomeniii/changeflow/internal/config"
	"github.com/ErfanMomeniii/changeflow/internal/pipeline"
	"github.com/ErfanMomeniii/changeflow/internal/preflight"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
	"github.com/ErfanMomeniii/changeflow/internal/sink"
	"github.com/ErfanMomeniii/changeflow/internal/sink/dlq"
	"github.com/ErfanMomeniii/changeflow/internal/sink/elasticsearch"
	"github.com/ErfanMomeniii/changeflow/internal/source/binlog"
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
		Logger:        s.log,
	})
	if err != nil {
		return err
	}

	startGTID, err := s.startPosition(ctx, store, sourceDB)
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
	runErr := runner.Run(ctx, events)

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

// startPosition resumes from the recorded position, or begins at the source's
// current position for a stream that has never run.
//
// Starting at "now" means rows written earlier are not in the stream at all. Only a
// table scan can supply those, which is what the snapshot phase is for.
func (s *Supervisor) startPosition(ctx context.Context, store checkpoint.Store, sourceDB *sql.DB) (string, error) {
	cp, err := store.Load(ctx, s.stream.Name)
	switch {
	case err == nil && cp.GTIDSet != "":
		return cp.GTIDSet, nil
	case err != nil && !errors.Is(err, checkpoint.ErrNotFound):
		// A position that cannot be read must never be guessed at: streaming from
		// the wrong place silently skips or duplicates data.
		return "", fmt.Errorf("supervisor: read checkpoint: %w", err)
	}

	var gtid string
	if err := sourceDB.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&gtid); err != nil {
		return "", fmt.Errorf("supervisor: read source position: %w", err)
	}
	gtid = strings.ReplaceAll(gtid, "\n", "")
	if strings.TrimSpace(gtid) == "" {
		return "", errors.New("supervisor: the source has logged no transactions, so there is no position to start from")
	}

	s.log.Warn("no recorded position, starting from the source's current position; rows written earlier will not appear until a snapshot runs",
		"stream", s.stream.Name, "from", gtid)
	return gtid, nil
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
	default:
		return nil, fmt.Errorf("supervisor: sink type %q is not implemented yet", s.stream.Sink.Type)
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
