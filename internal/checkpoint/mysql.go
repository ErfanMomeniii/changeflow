package checkpoint

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	driver "github.com/go-sql-driver/mysql"
)

// Schema is the checkpoint table definition.
//
// Columns are added over time but never repurposed, because other tools read this
// table directly to report progress. schema_version lets such a reader refuse to
// interpret a row written by a newer changeflow rather than guess.
const Schema = `
CREATE TABLE IF NOT EXISTS %s (
    stream              VARCHAR(64)      NOT NULL PRIMARY KEY,
    gtid_set            TEXT             NOT NULL,
    snapshot_done       TINYINT(1)       NOT NULL DEFAULT 0,
    snapshot_start_gtid TEXT                 NULL,
    snapshot_cursor     VARBINARY(255)       NULL,
    snapshot_base_seq   BIGINT UNSIGNED  NOT NULL DEFAULT 0,
    snapshot_rows_done  BIGINT UNSIGNED  NOT NULL DEFAULT 0,
    snapshot_rows_total BIGINT UNSIGNED  NOT NULL DEFAULT 0,
    seq_watermark       BIGINT UNSIGNED  NOT NULL DEFAULT 0,
    last_event_ts_ms    BIGINT           NOT NULL DEFAULT 0,
    last_error          TEXT                 NULL,
    schema_version      INT              NOT NULL DEFAULT 1,
    updated_at          TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB`

// maxStreamNameLen matches the stream column and leaves room for the lock name
// prefix, since MySQL lock names are limited to 64 characters.
const maxStreamNameLen = 48

// MySQLStore keeps checkpoints in a MySQL table. Putting them in a replicated,
// backed-up database rather than on local disk is what makes a changeflow process
// stateless and freely rescheduled.
type MySQLStore struct {
	db          *sql.DB
	table       string
	LockTimeout time.Duration
}

// NewMySQLStore returns a store writing to the named table. The table name is
// validated rather than escaped because it is interpolated into DDL and DML:
// identifiers cannot be bound as parameters.
func NewMySQLStore(db *sql.DB, table string) (*MySQLStore, error) {
	if db == nil {
		return nil, errors.New("checkpoint: mysql store needs a database")
	}
	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	return &MySQLStore{db: db, table: table, LockTimeout: 10 * time.Second}, nil
}

// validateIdentifier permits only a plain or qualified identifier, so nothing a
// caller supplies can alter the shape of a statement.
func validateIdentifier(name string) error {
	if name == "" {
		return errors.New("checkpoint: table name is empty")
	}
	for _, part := range strings.Split(name, ".") {
		if part == "" {
			return fmt.Errorf("checkpoint: malformed table name %q", name)
		}
		for _, r := range part {
			if r != '_' && r != '$' &&
				(r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
				return fmt.Errorf("checkpoint: table name %q may contain only letters, digits, underscore, and a database prefix", name)
			}
		}
	}
	return nil
}

// EnsureSchema creates the checkpoint table if it does not exist.
//
// This needs CREATE, which a runtime user should not hold. In production, apply
// the DDL with a migration tool and give the running service only DML rights.
func (s *MySQLStore) EnsureSchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(Schema, s.table)); err != nil {
		return fmt.Errorf("checkpoint: create table %s: %w", s.table, err)
	}
	return nil
}

// ErrNotInitialized reports that the checkpoint table does not exist. It is
// distinct from a missing row, because reporting status before anything has ever
// run is a normal thing to want, while a vanished table is not.
var ErrNotInitialized = errors.New("checkpoint table does not exist")

// missingTableCode is MySQL's ER_NO_SUCH_TABLE.
const missingTableCode = 1146

func asMissingTable(err error) error {
	var mysqlErr *driver.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == missingTableCode {
		return fmt.Errorf("%w: run changeflow once to create it, or apply the migration", ErrNotInitialized)
	}
	return nil
}

// Load returns a stream's checkpoint, or ErrNotFound.
func (s *MySQLStore) Load(ctx context.Context, stream string) (Checkpoint, error) {
	q := fmt.Sprintf(`
		SELECT stream, gtid_set, snapshot_done, COALESCE(snapshot_start_gtid,''),
		       snapshot_cursor, snapshot_base_seq, snapshot_rows_done,
		       snapshot_rows_total, seq_watermark, last_event_ts_ms,
		       COALESCE(last_error,''), schema_version, updated_at
		FROM %s WHERE stream = ?`, s.table)

	var (
		cp        Checkpoint
		updatedAt any
	)
	err := s.db.QueryRowContext(ctx, q, stream).Scan(
		&cp.Stream, &cp.GTIDSet, &cp.SnapshotDone, &cp.SnapshotStartGTID,
		&cp.SnapshotCursor, &cp.SnapshotBaseSeq, &cp.SnapshotRowsDone,
		&cp.SnapshotRowsTotal, &cp.SeqWatermark, &cp.LastEventTsMs,
		&cp.LastError, &cp.SchemaVersion, &updatedAt,
	)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Checkpoint{}, ErrNotFound
	case err != nil:
		if missing := asMissingTable(err); missing != nil {
			return Checkpoint{}, missing
		}
		return Checkpoint{}, fmt.Errorf("checkpoint: load %s: %w", stream, err)
	}
	if ts, err := parseTimestamp(updatedAt); err != nil {
		return Checkpoint{}, fmt.Errorf("checkpoint: stream %s has an unreadable updated_at: %w", stream, err)
	} else {
		cp.UpdatedAt = ts
	}

	// A row written by a newer changeflow may use fields this build does not know
	// about, so refuse it instead of acting on a partial understanding.
	if cp.SchemaVersion > SchemaVersion {
		return Checkpoint{}, fmt.Errorf("checkpoint: stream %s was written with schema version %d, this build understands %d", stream, cp.SchemaVersion, SchemaVersion)
	}
	return cp, nil
}

// parseTimestamp accepts either form the driver may produce for a TIMESTAMP.
func parseTimestamp(v any) (time.Time, error) {
	switch x := v.(type) {
	case nil:
		return time.Time{}, nil
	case time.Time:
		return x, nil
	case []byte:
		return parseTimestampText(string(x))
	case string:
		return parseTimestampText(x)
	default:
		return time.Time{}, fmt.Errorf("unexpected type %T", v)
	}
}

func parseTimestampText(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" || strings.HasPrefix(s, "0000-00-00") {
		return time.Time{}, nil
	}
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
	} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse %q as a timestamp", s)
}

// Save writes the checkpoint, inserting it when absent. The upsert is a single
// statement so a concurrent reader never sees a half-written position.
func (s *MySQLStore) Save(ctx context.Context, cp Checkpoint) error {
	if err := validateStream(cp.Stream); err != nil {
		return err
	}
	if cp.SchemaVersion == 0 {
		cp.SchemaVersion = SchemaVersion
	}

	q := fmt.Sprintf(`
		INSERT INTO %s (stream, gtid_set, snapshot_done, snapshot_start_gtid,
		                snapshot_cursor, snapshot_base_seq, snapshot_rows_done,
		                snapshot_rows_total, seq_watermark, last_event_ts_ms,
		                last_error, schema_version)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
		ON DUPLICATE KEY UPDATE
		    gtid_set = VALUES(gtid_set),
		    snapshot_done = VALUES(snapshot_done),
		    snapshot_start_gtid = VALUES(snapshot_start_gtid),
		    snapshot_cursor = VALUES(snapshot_cursor),
		    snapshot_base_seq = VALUES(snapshot_base_seq),
		    snapshot_rows_done = VALUES(snapshot_rows_done),
		    snapshot_rows_total = VALUES(snapshot_rows_total),
		    seq_watermark = VALUES(seq_watermark),
		    last_event_ts_ms = VALUES(last_event_ts_ms),
		    last_error = VALUES(last_error),
		    schema_version = VALUES(schema_version)`, s.table)

	if _, err := s.db.ExecContext(ctx, q,
		cp.Stream, cp.GTIDSet, cp.SnapshotDone, cp.SnapshotStartGTID,
		cp.SnapshotCursor, cp.SnapshotBaseSeq, cp.SnapshotRowsDone,
		cp.SnapshotRowsTotal, cp.SeqWatermark, cp.LastEventTsMs,
		cp.LastError, cp.SchemaVersion,
	); err != nil {
		return fmt.Errorf("checkpoint: save %s: %w", cp.Stream, err)
	}
	return nil
}

// List returns every checkpoint, for status reporting.
func (s *MySQLStore) List(ctx context.Context) ([]Checkpoint, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("SELECT stream FROM %s ORDER BY stream", s.table))
	if err != nil {
		return nil, fmt.Errorf("checkpoint: list: %w", err)
	}
	defer rows.Close()

	var streams []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("checkpoint: scan stream: %w", err)
		}
		streams = append(streams, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]Checkpoint, 0, len(streams))
	for _, name := range streams {
		cp, err := s.Load(ctx, name)
		if err != nil {
			return nil, err
		}
		out = append(out, cp)
	}
	return out, nil
}

func validateStream(stream string) error {
	if stream == "" {
		return errors.New("checkpoint: stream name is empty")
	}
	if len(stream) > maxStreamNameLen {
		return fmt.Errorf("checkpoint: stream name %q is longer than %d characters", stream, maxStreamNameLen)
	}
	return nil
}

// StreamLock is exclusive ownership of one stream, held for as long as its
// connection lives.
type StreamLock struct {
	conn   *sql.Conn
	name   string
	stream string
}

// ErrStreamLocked reports that another session owns the stream.
var ErrStreamLocked = errors.New("stream is already being processed elsewhere")

// Lock claims exclusive ownership of a stream, so two processes cannot replicate
// it at once. Running a stream twice would double-write and produce two replicas
// contending for one server_id, and the resulting errors name neither culprit.
//
// The lock lives on its own pinned connection: MySQL advisory locks are
// session-scoped, so taking one from a pooled connection would release it as soon
// as that connection returned to the pool.
func (s *MySQLStore) Lock(ctx context.Context, stream string) (*StreamLock, error) {
	if err := validateStream(stream); err != nil {
		return nil, err
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: pin connection for stream lock: %w", err)
	}

	name := "changeflow:" + stream
	waitSeconds := int(s.LockTimeout.Round(time.Second) / time.Second)
	if waitSeconds < 0 {
		waitSeconds = 0
	}

	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", name, waitSeconds).Scan(&got); err != nil {
		conn.Close()
		return nil, fmt.Errorf("checkpoint: acquire lock for %s: %w", stream, err)
	}
	if !got.Valid || got.Int64 != 1 {
		holder := "another session"
		var id sql.NullInt64
		if err := conn.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", name).Scan(&id); err == nil && id.Valid {
			holder = fmt.Sprintf("connection id %d", id.Int64)
		}
		conn.Close()
		return nil, fmt.Errorf("%w: %s still holds %s after waiting %s", ErrStreamLocked, holder, stream, s.LockTimeout)
	}

	return &StreamLock{conn: conn, name: name, stream: stream}, nil
}

// Release gives up the lock and returns its connection.
func (l *StreamLock) Release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}
	_, err := l.conn.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", l.name)
	closeErr := l.conn.Close()
	l.conn = nil
	if err != nil {
		return fmt.Errorf("checkpoint: release lock for %s: %w", l.stream, err)
	}
	return closeErr
}

// Held reports whether this lock is still held, which distinguishes a released
// lock from one whose connection died.
func (l *StreamLock) Held(ctx context.Context) bool {
	if l == nil || l.conn == nil {
		return false
	}
	var mine sql.NullInt64
	if err := l.conn.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", l.name).Scan(&mine); err != nil {
		return false
	}
	return mine.Valid
}

// Ping verifies the store is reachable, so a misconfigured checkpoint database
// fails at startup rather than at the first batch acknowledgement.
func (s *MySQLStore) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("checkpoint: ping: %w", err)
	}
	return nil
}
