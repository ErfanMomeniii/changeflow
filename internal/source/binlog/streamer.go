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

// ErrPositionPurged means the source no longer holds the transactions a stream needs.
//
// It is the one failure a restart cannot fix, so it is worth telling apart from a
// connection problem: the data between the recorded position and the oldest retained
// transaction exists nowhere any more, and only reading the table again can reconcile it.
var ErrPositionPurged = errors.New("binlog: the recorded position is no longer in the source's binlog")

const errBinlogReadFailure = 1236

func explain(err error) error {
	var serverErr *mysql.MyError
	if errors.As(err, &serverErr) && serverErr.Code == errBinlogReadFailure {
		return fmt.Errorf("%w: %s; rescan the table with `changeflow resnapshot --stream <name> --confirm` "+
			"and start the stream again, or lengthen binlog_expire_logs_seconds so an outage this long is survivable",
			ErrPositionPurged, serverErr.Message)
	}
	return err
}

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
	opts      Options
	watched   map[string]bool
	log       *slog.Logger
	syncer    *replication.BinlogSyncer
	mu        sync.Mutex
	err       error
	executed  mysql.GTIDSet
	currentTx string
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

// Position returns the cumulative set of applied transactions, for reporting.
func (s *Streamer) Position() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.executed == nil {
		return ""
	}
	return s.executed.String()
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
	s.mu.Lock()
	s.executed = gtidSet.Clone()
	s.mu.Unlock()
	streamer, err := s.syncer.StartSyncGTID(gtidSet)
	if err != nil {
		return fmt.Errorf("binlog: start replication from %s: %w", s.opts.StartGTID, explain(err))
	}
	s.log.Info("replication started", "from", s.opts.StartGTID, "server_id", s.opts.ServerID)
	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			return explain(err)
		}
		if err := s.handle(ctx, ev, out); err != nil {
			return err
		}
	}
}

func (s *Streamer) handle(ctx context.Context, ev *replication.BinlogEvent, out chan<- cdc.ChangeEvent) error {
	switch e := ev.Event.(type) {
	case *replication.GTIDEvent:
		set, err := e.GTIDNext()
		if err != nil {
			return fmt.Errorf("binlog: read transaction identifier: %w", err)
		}
		s.mu.Lock()
		s.currentTx = set.String()
		s.mu.Unlock()
		return nil
	case *replication.XIDEvent:
		s.mu.Lock()
		if s.currentTx != "" && s.executed != nil {
			if err := s.executed.Update(s.currentTx); err != nil {
				s.mu.Unlock()
				return fmt.Errorf("binlog: fold transaction %s into the position: %w", s.currentTx, err)
			}
			s.currentTx = ""
		}
		s.mu.Unlock()
		return nil
	case *replication.QueryEvent:
		s.invalidate(string(e.Schema), string(e.Query))
		return nil
	case *replication.RowsEvent:
		return s.emitRows(ctx, ev, e, out)
	}
	return nil
}

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
	if strings.Contains(lowered, "rename") || dbName == "" {
		s.opts.Schemas.InvalidateAll()
	}
}

func (s *Streamer) emitRows(ctx context.Context, raw *replication.BinlogEvent, e *replication.RowsEvent, out chan<- cdc.ChangeEvent) error {
	operation, isUpdate, recognised := operationFor(raw.Header.EventType)
	if !recognised {
		return nil
	}
	schemaName, table := string(e.Table.Schema), string(e.Table.Table)
	if len(s.watched) > 0 && !s.watched[strings.ToLower(schemaName+"."+table)] {
		return nil
	}
	meta, err := s.opts.Schemas.Table(ctx, schemaName, table)
	if err != nil {
		return fmt.Errorf("binlog: %w", err)
	}
	align, err := s.alignment(ctx, e, meta)
	if err != nil {
		return err
	}
	em := emission{
		align:     align,
		timestamp: time.Unix(int64(raw.Header.Timestamp), 0),
		gtid:      s.positionForRows(),
	}
	if isUpdate {
		return s.emitUpdates(ctx, em, e.Rows, out)
	}
	return s.emitEach(ctx, em, operation, e.Rows, out)
}

type emission struct {
	align     alignment
	timestamp time.Time
	gtid      string
}

func (s *Streamer) positionForRows() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.executed == nil {
		return ""
	}
	return s.executed.String()
}

func (s *Streamer) emitUpdates(ctx context.Context, em emission, rows [][]any, out chan<- cdc.ChangeEvent) error {
	for i := 0; i+1 < len(rows); i += 2 {
		ev, err := s.build(ctx, em.align.meta, cdc.OperationUpdate,
			em.align.row(rows[i]), em.align.row(rows[i+1]), em.timestamp, em.gtid)
		if err != nil {
			return err
		}
		if err := send(ctx, out, ev); err != nil {
			return err
		}
	}
	return nil
}

func (s *Streamer) emitEach(ctx context.Context, em emission, operation cdc.Operation, rows [][]any, out chan<- cdc.ChangeEvent) error {
	for _, row := range rows {
		var before, after cdc.Row
		if operation == cdc.OperationDelete {
			before = em.align.row(row)
		} else {
			after = em.align.row(row)
		}
		ev, err := s.build(ctx, em.align.meta, operation, before, after, em.timestamp, em.gtid)
		if err != nil {
			return err
		}
		if err := send(ctx, out, ev); err != nil {
			return err
		}
	}
	return nil
}

type alignment struct {
	meta      *schema.TableMeta
	positions []int
	direct    bool
}

func (a alignment) row(values []any) cdc.Row {
	if a.direct {
		return cdc.Row(values)
	}
	out := make(cdc.Row, len(a.meta.Columns))
	for i, v := range values {
		if i >= len(a.positions) {
			break
		}
		if pos := a.positions[i]; pos >= 0 {
			out[pos] = v
		}
	}
	return out
}

func (s *Streamer) alignment(ctx context.Context, e *replication.RowsEvent, meta *schema.TableMeta) (alignment, error) {
	if len(e.Rows) == 0 {
		return alignment{meta: meta, direct: true}, nil
	}
	width := len(e.Rows[0])
	if width == len(meta.Columns) {
		return alignment{meta: meta, direct: true}, nil
	}
	schemaName, table := string(e.Table.Schema), string(e.Table.Table)
	s.opts.Schemas.Invalidate(schemaName, table)
	reloaded, err := s.opts.Schemas.Table(ctx, schemaName, table)
	if err != nil {
		return alignment{}, fmt.Errorf("binlog: reload %s.%s after a row width mismatch: %w", schemaName, table, err)
	}
	if width == len(reloaded.Columns) {
		return alignment{meta: reloaded, direct: true}, nil
	}
	names := e.Table.ColumnNameString()
	if len(names) != width {
		return alignment{}, fmt.Errorf("binlog: %s.%s row carries %d values while the table defines %d columns, and the binlog names only %d of them; set binlog_row_metadata=FULL so historical rows can be aligned by name",
			schemaName, table, width, len(reloaded.Columns), len(names))
	}
	positions := make([]int, width)
	var dropped, missing []string
	for i, name := range names {
		if c, ok := reloaded.Column(name); ok {
			positions[i] = c.Position
			continue
		}
		positions[i] = -1
		dropped = append(dropped, name)
	}
	present := make(map[string]bool, len(names))
	for _, n := range names {
		present[strings.ToLower(n)] = true
	}
	for _, c := range reloaded.Columns {
		if !present[strings.ToLower(c.Name)] {
			missing = append(missing, c.Name)
		}
	}
	s.log.Warn("aligning rows written before a schema change",
		"table", reloaded.Name(),
		"row_columns", width,
		"table_columns", len(reloaded.Columns),
		"not_in_table", dropped,
		"absent_from_row", missing)
	return alignment{meta: reloaded, positions: positions}, nil
}

func (s *Streamer) build(ctx context.Context, meta *schema.TableMeta, operation cdc.Operation, before, after cdc.Row, ts time.Time, gtid string) (cdc.ChangeEvent, error) {
	seq, err := s.opts.Sequencer.Next(ctx)
	if err != nil {
		return cdc.ChangeEvent{}, fmt.Errorf("binlog: allocate version: %w", err)
	}
	return cdc.ChangeEvent{
		Meta:      meta,
		Operation: operation,
		Before:    before,
		After:     after,
		Timestamp: ts,
		GTID:      gtid,
		Seq:       seq,
	}, nil
}

func send(ctx context.Context, out chan<- cdc.ChangeEvent, ev cdc.ChangeEvent) error {
	select {
	case out <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func operationFor(t replication.EventType) (operation cdc.Operation, isUpdate, recognised bool) {
	switch t {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return cdc.OperationInsert, false, true
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return cdc.OperationDelete, false, true
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return cdc.OperationUpdate, true, true
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
