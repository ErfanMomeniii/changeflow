// Package binlog reads MySQL's replication stream and emits change events.
//
// It is one implementation of a source: the other is a table scan, and everything
// downstream is written against the events rather than against either producer.
// That is what lets a backfill and a live stream share the same transform, batching,
// sink, and tests.
package binlog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
)

// Sequencer hands out the versions stamped on events. It is an interface so the
// reader does not depend on where positions are stored.
type Sequencer interface {
	Next(ctx context.Context) (uint64, error)
}

// Options configures a reader.
type Options struct {
	Host            string
	Port            uint16
	User            string
	Password        string
	ServerID        uint32
	StartGTID       string
	Tables          []string
	Schemas         *schema.Store
	Sequencer       Sequencer
	HeartbeatPeriod time.Duration
	ReadTimeout     time.Duration
	Buffer          int
	Logger          *slog.Logger
}

// Streamer reads the replication stream.
type Streamer struct {
	opts    Options
	watched map[string]bool
	log     *slog.Logger
	syncer  *replication.BinlogSyncer
	mu      sync.Mutex
	err     error
	gtid    string
}

// New validates options and prepares a reader. Nothing connects until Events is
// called.
func New(opts Options) (*Streamer, error) {
	switch {
	case opts.Host == "":
		return nil, errors.New("binlog: a host is required")
	case opts.ServerID == 0:
		return nil, errors.New("binlog: a non-zero server_id is required, and it must differ from the source's own")
	case opts.StartGTID == "":
		return nil, errors.New("binlog: a start position is required; an empty GTID set would stream the whole retained binlog")
	case opts.Schemas == nil:
		return nil, errors.New("binlog: a schema store is required")
	case opts.Sequencer == nil:
		return nil, errors.New("binlog: a sequencer is required to version events")
	}

	if opts.HeartbeatPeriod <= 0 {
		opts.HeartbeatPeriod = 5 * time.Second
	}
	if opts.ReadTimeout <= 0 {
		opts.ReadTimeout = 90 * time.Second
	}
	if opts.Buffer <= 0 {
		opts.Buffer = 1024
	}
	if opts.Port == 0 {
		opts.Port = 3306
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}

	watched := make(map[string]bool, len(opts.Tables))
	for _, t := range opts.Tables {
		if !strings.Contains(t, ".") {
			return nil, fmt.Errorf("binlog: table %q must be written as database.table", t)
		}
		watched[strings.ToLower(t)] = true
	}

	return &Streamer{opts: opts, watched: watched, log: log}, nil
}

// Events starts reading and returns the stream. The channel closes when the
// context ends or reading fails; Err reports why.
func (s *Streamer) Events(ctx context.Context) <-chan cdc.ChangeEvent {
	out := make(chan cdc.ChangeEvent, s.opts.Buffer)

	go func() {
		defer close(out)
		if err := s.run(ctx, out); err != nil && !errors.Is(err, context.Canceled) {
			s.setErr(err)
		}
	}()

	return out
}

// Err returns why the stream ended, or nil for a clean stop.
func (s *Streamer) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *Streamer) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

// Position returns the last transaction identifier seen, for reporting.
func (s *Streamer) Position() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gtid
}

func (s *Streamer) run(ctx context.Context, out chan<- cdc.ChangeEvent) error {
	s.syncer = replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:                s.opts.ServerID,
		Flavor:                  "mysql",
		Host:                    s.opts.Host,
		Port:                    s.opts.Port,
		User:                    s.opts.User,
		Password:                s.opts.Password,
		UseDecimal:              true,
		ParseTime:               false,
		TimestampStringLocation: time.UTC,
		HeartbeatPeriod:         s.opts.HeartbeatPeriod,
		ReadTimeout:             s.opts.ReadTimeout,
		Logger:                  s.log,
	})
	defer s.syncer.Close()

	gtidSet, err := mysql.ParseMysqlGTIDSet(s.opts.StartGTID)
	if err != nil {
		return fmt.Errorf("binlog: parse start position %q: %w", s.opts.StartGTID, err)
	}

	streamer, err := s.syncer.StartSyncGTID(gtidSet)
	if err != nil {
		return fmt.Errorf("binlog: start replication from %s: %w", s.opts.StartGTID, err)
	}
	s.log.Info("replication started", "from", s.opts.StartGTID, "server_id", s.opts.ServerID)

	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			return err
		}
		if err := s.handle(ctx, ev, out); err != nil {
			return err
		}
	}
}

func (s *Streamer) handle(ctx context.Context, ev *replication.BinlogEvent, out chan<- cdc.ChangeEvent) error {
	switch e := ev.Event.(type) {
	case *replication.GTIDEvent:
		if set, err := e.GTIDNext(); err == nil {
			s.mu.Lock()
			s.gtid = set.String()
			s.mu.Unlock()
		}
		return nil

	case *replication.QueryEvent:
		// DDL changes a table's shape, so any cached definition is now wrong and
		// would decode later rows against the wrong columns.
		s.invalidate(string(e.Schema), string(e.Query))
		return nil

	case *replication.RowsEvent:
		return s.emitRows(ctx, ev, e, out)
	}
	return nil
}

// invalidate drops cached definitions a DDL statement may have changed. The
// statement is not parsed properly: matching a watched table's name is enough to
// decide, and anything ambiguous drops everything, since a stale definition is far
// worse than a redundant reload.
func (s *Streamer) invalidate(dbName, query string) {
	lowered := strings.ToLower(query)
	if !strings.Contains(lowered, "alter") && !strings.Contains(lowered, "create") &&
		!strings.Contains(lowered, "drop") && !strings.Contains(lowered, "rename") &&
		!strings.Contains(lowered, "truncate") {
		return
	}

	for watched := range s.watched {
		schemaName, table, _ := strings.Cut(watched, ".")
		if strings.Contains(lowered, strings.ToLower(table)) {
			s.log.Info("reloading table definition after DDL", "table", watched, "statement", collapse(query))
			s.opts.Schemas.Invalidate(schemaName, table)
			return
		}
	}

	// The statement mentions no watched table by name, but a rename or a database
	// level change could still affect one.
	if strings.Contains(lowered, "rename") || dbName == "" {
		s.opts.Schemas.InvalidateAll()
	}
}

func (s *Streamer) emitRows(ctx context.Context, raw *replication.BinlogEvent, e *replication.RowsEvent, out chan<- cdc.ChangeEvent) error {
	schemaName, table := string(e.Table.Schema), string(e.Table.Table)
	if len(s.watched) > 0 && !s.watched[strings.ToLower(schemaName+"."+table)] {
		return nil
	}

	meta, err := s.opts.Schemas.Table(ctx, schemaName, table)
	if err != nil {
		return fmt.Errorf("binlog: %w", err)
	}

	// A row narrower than the definition means the definition is stale, most likely
	// because DDL arrived without a statement we recognised. Reload once before
	// giving up, rather than decoding values against the wrong columns.
	if len(e.Rows) > 0 && len(e.Rows[0]) != len(meta.Columns) {
		s.opts.Schemas.Invalidate(schemaName, table)
		if meta, err = s.opts.Schemas.Table(ctx, schemaName, table); err != nil {
			return fmt.Errorf("binlog: reload %s.%s after a row width mismatch: %w", schemaName, table, err)
		}
		if len(e.Rows[0]) != len(meta.Columns) {
			return fmt.Errorf("binlog: %s.%s row carries %d values but the table defines %d columns; decoding would attribute values to the wrong fields",
				schemaName, table, len(e.Rows[0]), len(meta.Columns))
		}
	}

	timestamp := time.Unix(int64(raw.Header.Timestamp), 0)
	op, isUpdate, recognised := opFor(raw.Header.EventType)
	if !recognised {
		return nil
	}

	s.mu.Lock()
	gtid := s.gtid
	s.mu.Unlock()

	if isUpdate {
		// Update rows arrive as before/after pairs.
		for i := 0; i+1 < len(e.Rows); i += 2 {
			ev, err := s.build(ctx, meta, cdc.OpUpdate, e.Rows[i], e.Rows[i+1], timestamp, gtid)
			if err != nil {
				return err
			}
			if err := send(ctx, out, ev); err != nil {
				return err
			}
		}
		return nil
	}

	for _, row := range e.Rows {
		var before, after cdc.Row
		if op == cdc.OpDelete {
			before = cdc.Row(row)
		} else {
			after = cdc.Row(row)
		}
		ev, err := s.build(ctx, meta, op, before, after, timestamp, gtid)
		if err != nil {
			return err
		}
		if err := send(ctx, out, ev); err != nil {
			return err
		}
	}
	return nil
}

func (s *Streamer) build(ctx context.Context, meta *schema.TableMeta, op cdc.Op, before, after cdc.Row, ts time.Time, gtid string) (cdc.ChangeEvent, error) {
	seq, err := s.opts.Sequencer.Next(ctx)
	if err != nil {
		return cdc.ChangeEvent{}, fmt.Errorf("binlog: allocate version: %w", err)
	}
	return cdc.ChangeEvent{
		Meta:      meta,
		Op:        op,
		Before:    before,
		After:     after,
		Timestamp: ts,
		GTID:      gtid,
		Seq:       seq,
	}, nil
}

// send blocks until the pipeline takes the event, which is how back pressure
// reaches the reader.
func send(ctx context.Context, out chan<- cdc.ChangeEvent, ev cdc.ChangeEvent) error {
	select {
	case out <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// opFor maps an event type onto an operation.
//
// The third result reports whether the type was recognised at all. It cannot be
// inferred from the operation, because OpInsert is the zero value: using that as a
// sentinel silently discards every insert.
func opFor(t replication.EventType) (op cdc.Op, isUpdate, recognised bool) {
	switch t {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return cdc.OpInsert, false, true
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return cdc.OpDelete, false, true
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return cdc.OpUpdate, true, true
	default:
		return 0, false, false
	}
}

// Close stops replication.
func (s *Streamer) Close() error {
	if s.syncer != nil {
		s.syncer.Close()
	}
	return nil
}

func collapse(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
