// Package snapshot reads rows that already exist in a table.
//
// The binlog only carries changes, so a row written before a stream existed and
// never touched since produces no event at all. This is the only way those rows
// reach a destination.
//
// It is the second implementation of a source, and it emits the same events the
// binlog reader does, in the same value shapes, so everything downstream is unaware
// of which one produced a row.
package snapshot

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shopspring/decimal"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
)

// Options configures a scan.
type Options struct {
	DB   *sql.DB
	Meta *schema.TableMeta
	// Key columns, in order. The scan walks the table in this order and resumes by
	// comparing against it, so it must be unique.
	Key []string

	// ChunkSize bounds one query's result, which bounds how much memory a scan holds
	// and how long any single statement runs on the source.
	ChunkSize int
	// MaxRowsPerSec throttles reading, since a backfill is the only part of
	// changeflow that can degrade the source.
	MaxRowsPerSec int

	// Cursor resumes an interrupted scan. Empty starts from the beginning.
	Cursor []byte
	// BaseSeq is stamped on every row this scan emits.
	//
	// One version for all of them is deliberate: any change replayed from the
	// position captured before the scan carries a higher version and therefore wins,
	// which is what makes scanning without a lock safe.
	BaseSeq uint64

	// Observe is called after each chunk, for logging and metrics only. Durability
	// is not its job: the cursor is carried on the last event of the chunk and
	// recorded by whatever stage sees the write acknowledged.
	Observe func(rowsRead uint64)

	Logger *slog.Logger
}

// Snapshotter scans a table in key order.
type Snapshotter struct {
	opts    Options
	keyCols []schema.Column
	// scanCols is every column the scan reads, which is all of them: the transform
	// decides what to keep, exactly as it does for a binlog row.
	scanCols []schema.Column
	log      *slog.Logger

	rowsRead atomic.Uint64
	mu       sync.Mutex
	err      error
	done     bool
}

// New validates options and prepares a scan. Nothing runs until Events is called.
func New(opts Options) (*Snapshotter, error) {
	switch {
	case opts.DB == nil:
		return nil, errors.New("snapshot: a database is required")
	case opts.Meta == nil:
		return nil, errors.New("snapshot: a table definition is required")
	case len(opts.Key) == 0:
		return nil, errors.New("snapshot: a key is required to scan in a resumable order")
	case opts.ChunkSize < 1:
		return nil, errors.New("snapshot: chunk size must be at least 1")
	case opts.BaseSeq == 0:
		return nil, errors.New("snapshot: a base version is required, or snapshot rows could overwrite newer changes")
	}

	s := &Snapshotter{opts: opts, log: opts.Logger}
	if s.log == nil {
		s.log = slog.Default()
	}

	for _, name := range opts.Key {
		c, ok := opts.Meta.Column(name)
		if !ok {
			return nil, fmt.Errorf("snapshot: key column %q is not in table %s", name, opts.Meta.Name())
		}
		s.keyCols = append(s.keyCols, c)
	}

	for _, c := range opts.Meta.Columns {
		// A generated column is absent from a binlog row image, so including it here
		// would make snapshot rows a different shape from streamed ones.
		if !c.Generated {
			s.scanCols = append(s.scanCols, c)
		}
	}

	return s, nil
}

// Events runs the scan and returns its rows. The channel closes when the scan
// finishes or fails; Err reports why, and Done reports whether it completed.
func (s *Snapshotter) Events(ctx context.Context) <-chan cdc.ChangeEvent {
	out := make(chan cdc.ChangeEvent, s.opts.ChunkSize)

	go func() {
		defer close(out)
		if err := s.run(ctx, out); err != nil {
			if !errors.Is(err, context.Canceled) {
				s.setErr(err)
			}
			return
		}
		s.mu.Lock()
		s.done = true
		s.mu.Unlock()
	}()

	return out
}

// Err returns why the scan stopped, or nil if it completed or was cancelled.
func (s *Snapshotter) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Done reports whether the scan reached the end of the table. Only then may a
// stream record its snapshot as complete.
func (s *Snapshotter) Done() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

// RowsRead reports progress, for a status display.
func (s *Snapshotter) RowsRead() uint64 { return s.rowsRead.Load() }

func (s *Snapshotter) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

func (s *Snapshotter) run(ctx context.Context, out chan<- cdc.ChangeEvent) error {
	conn, err := s.scanConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	cursor, err := decodeCursor(s.opts.Cursor, len(s.keyCols))
	if err != nil {
		return err
	}
	if cursor != nil {
		s.log.Info("resuming an interrupted scan", "table", s.opts.Meta.Name(), "after", cursor)
	}

	limiter := newLimiter(s.opts.MaxRowsPerSec)

	for {
		rows, next, err := s.readChunk(ctx, conn, cursor)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}

		if err := s.emitChunk(ctx, out, rows, next); err != nil {
			return err
		}
		cursor = next

		if s.opts.Observe != nil {
			s.opts.Observe(s.rowsRead.Load())
		}

		if err := limiter.wait(ctx, len(rows)); err != nil {
			return err
		}
		if len(rows) < s.opts.ChunkSize {
			// A short chunk means the end of the table.
			return nil
		}
	}
}

// scanConn takes a dedicated connection, so the session settings applied here cover every
// query the scan makes and nothing else in the pool inherits them.
func (s *Snapshotter) scanConn(ctx context.Context) (*sql.Conn, error) {
	conn, err := s.opts.DB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: acquire connection: %w", err)
	}

	// A TIMESTAMP is rendered in the session's zone. Without this it would arrive in
	// whatever zone the server happens to use, while the binlog reader always sees
	// UTC, and the same row would land in the destination with two different times
	// depending on which source read it.
	if _, err := conn.ExecContext(ctx, "SET SESSION time_zone = '+00:00'"); err != nil {
		conn.Close()
		return nil, fmt.Errorf("snapshot: set session time zone: %w", err)
	}
	return conn, nil
}

// emitChunk sends one chunk's rows downstream and records the progress they represent.
func (s *Snapshotter) emitChunk(ctx context.Context, out chan<- cdc.ChangeEvent, rows []cdc.Row, next []any) error {
	encodedCursor, err := encodeCursor(next)
	if err != nil {
		return err
	}
	rowsAfterChunk := s.rowsRead.Load() + uint64(len(rows))

	for i, row := range rows {
		ev := cdc.ChangeEvent{
			Meta:      s.opts.Meta,
			Operation: cdc.OperationSnapshot,
			// A scanned row has no prior state and no transaction: it is simply
			// what the table holds now.
			After: row,
			Seq:   s.opts.BaseSeq,
		}
		// Only the last row of a chunk carries the cursor. Attaching one to every
		// row would encode a position per row for no benefit, and a batch cut short
		// mid-chunk simply resumes from the previous boundary and re-reads a chunk,
		// which the destination absorbs.
		if i == len(rows)-1 {
			ev.Cursor = encodedCursor
			ev.RowsScanned = rowsAfterChunk
		}

		select {
		case out <- ev:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	s.rowsRead.Store(rowsAfterChunk)
	return nil
}

// readChunk reads the next page and returns the rows plus the cursor that follows
// them.
func (s *Snapshotter) readChunk(ctx context.Context, conn *sql.Conn, cursor []any) (rows []cdc.Row, next []any, err error) {
	query, args := s.chunkQuery(cursor)

	result, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("snapshot: scan %s: %w", s.opts.Meta.Name(), err)
	}
	defer result.Close()

	for result.Next() {
		row, keyValues, err := s.scanRow(result)
		if err != nil {
			return nil, nil, err
		}
		rows = append(rows, row)
		next = keyValues
	}
	if err := result.Err(); err != nil {
		return nil, nil, fmt.Errorf("snapshot: read %s: %w", s.opts.Meta.Name(), err)
	}
	return rows, next, nil
}

// chunkQuery builds a keyset-paginated read.
//
// Keyset rather than OFFSET: an offset makes the server walk and discard every
// earlier row, so a scan of a large table becomes quadratic. A row-value comparison
// handles composite keys in one predicate.
func (s *Snapshotter) chunkQuery(cursor []any) (string, []any) {
	var b strings.Builder
	b.WriteString("SELECT ")
	for i, c := range s.scanCols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteIdentifier(c.Name))
	}
	b.WriteString(" FROM ")
	b.WriteString(quoteIdentifier(s.opts.Meta.Schema))
	b.WriteByte('.')
	b.WriteString(quoteIdentifier(s.opts.Meta.Table))

	var args []any
	if cursor != nil {
		b.WriteString(" WHERE (")
		for i, c := range s.keyCols {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(quoteIdentifier(c.Name))
		}
		b.WriteString(") > (")
		for i := range s.keyCols {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("?")
		}
		b.WriteString(")")
		args = append(args, cursor...)
	}

	b.WriteString(" ORDER BY ")
	for i, c := range s.keyCols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteIdentifier(c.Name))
	}
	b.WriteString(" LIMIT ?")
	args = append(args, s.opts.ChunkSize)

	return b.String(), args
}

// scanRow reads one row into the same value shapes a binlog row carries, and
// returns the key values for the cursor.
func (s *Snapshotter) scanRow(result *sql.Rows) (cdc.Row, []any, error) {
	raw := make([]any, len(s.scanCols))
	dest := make([]any, len(s.scanCols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	if err := result.Scan(dest...); err != nil {
		return nil, nil, fmt.Errorf("snapshot: scan row of %s: %w", s.opts.Meta.Name(), err)
	}

	// The row is laid out by ordinal position, matching the table definition, because
	// that is how the transform indexes into it.
	row := make(cdc.Row, len(s.opts.Meta.Columns))
	for i, c := range s.scanCols {
		value, err := toEventValue(c, raw[i])
		if err != nil {
			return nil, nil, fmt.Errorf("snapshot: %s.%s: %w", s.opts.Meta.Name(), c.Name, err)
		}
		row[c.Position] = value
	}

	keyValues := make([]any, len(s.keyCols))
	for i, c := range s.keyCols {
		// The cursor uses the value as the server returned it, so the comparison in the
		// next query is against the column's own type rather than a converted form.
		keyValues[i] = rawKeyValue(raw[indexOf(s.scanCols, c.Name)])
	}

	return row, keyValues, nil
}

func indexOf(cols []schema.Column, name string) int {
	for i, c := range cols {
		if strings.EqualFold(c.Name, name) {
			return i
		}
	}
	return -1
}

func rawKeyValue(v any) any {
	if b, ok := v.([]byte); ok {
		// A []byte from the driver aliases its buffer, which is reused.
		return string(b)
	}
	return v
}

// toEventValue converts a scanned value into the shape the binlog reader produces for the
// same column: a SELECT returns an ENUM's label and a SET's members as text, where the
// binlog carries a member number and a bitmask. One canonical shape, one contract for the
// transform to satisfy.
func toEventValue(c schema.Column, v any) (any, error) {
	if v == nil {
		return nil, nil
	}

	switch strings.ToLower(c.DataType) {
	case "enum":
		return enumMember(c, v)

	case "set":
		return setBitmask(c, v)

	case "decimal", "numeric":
		text, err := asString(v)
		if err != nil {
			return nil, err
		}
		// Parsed rather than passed through, so the value is exact and identical to
		// what the binlog reader produces.
		d, err := decimal.NewFromString(text)
		if err != nil {
			return nil, fmt.Errorf("decimal value %q: %w", text, err)
		}
		return d, nil

	case "date", "datetime", "timestamp", "time":
		// Text, as the binlog reader also produces, so the transform interprets the
		// zone in one place.
		return asString(v)

	case "json", "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		b, err := asBytes(v)
		if err != nil {
			return nil, err
		}
		return b, nil

	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		return asString(v)

	case "bigint", "int", "integer", "mediumint", "smallint", "tinyint", "year", "bit":
		return asInteger(c, v)

	case "float", "double", "double precision", "real":
		return asFloat(v)

	default:
		return nil, fmt.Errorf("type %s is not supported by the scanner", c.DataType)
	}
}

// enumMember turns a label into the member number the binlog would have carried.
func enumMember(c schema.Column, v any) (any, error) {
	label, err := asString(v)
	if err != nil {
		return nil, err
	}
	if label == "" {
		// MySQL's marker for a value that failed validation, which the binlog
		// reports as member zero.
		return int64(0), nil
	}
	for i, member := range c.EnumValues {
		if member == label {
			return int64(i + 1), nil
		}
	}
	return nil, fmt.Errorf("enum value %q is not one of the declared members %v", label, c.EnumValues)
}

// setBitmask turns a comma-separated member list into the bitmask the binlog would have
// carried.
func setBitmask(c schema.Column, v any) (any, error) {
	joined, err := asString(v)
	if err != nil {
		return nil, err
	}
	if joined == "" {
		return int64(0), nil
	}

	var bits int64
	for _, member := range strings.Split(joined, ",") {
		index := indexOfString(c.SetValues, member)
		if index < 0 {
			return nil, fmt.Errorf("set value %q is not one of the declared members %v", member, c.SetValues)
		}
		bits |= 1 << uint(index)
	}
	return bits, nil
}

func indexOfString(values []string, want string) int {
	for i, v := range values {
		if v == want {
			return i
		}
	}
	return -1
}

func asString(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	case time.Time:
		return x.Format("2006-01-02 15:04:05.999999999"), nil
	default:
		return fmt.Sprint(x), nil
	}
}

func asBytes(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		// Copied: the driver reuses its buffer once the row is advanced.
		return append([]byte(nil), x...), nil
	case string:
		return []byte(x), nil
	default:
		return nil, fmt.Errorf("expected bytes, got %T", v)
	}
}

// asInteger keeps unsigned columns unsigned, so a value above 2^63 stays exact
// rather than reading as negative.
func asInteger(c schema.Column, v any) (any, error) {
	text, err := asString(v)
	if err != nil {
		return nil, err
	}

	if c.Unsigned {
		n, err := parseUint(text)
		if err != nil {
			return nil, fmt.Errorf("unsigned value %q: %w", text, err)
		}
		return n, nil
	}
	n, err := parseInt(text)
	if err != nil {
		return nil, fmt.Errorf("value %q: %w", text, err)
	}
	return n, nil
}

func asFloat(v any) (any, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return x, nil
	default:
		text, err := asString(v)
		if err != nil {
			return nil, err
		}
		f, err := parseFloat(text)
		if err != nil {
			return nil, fmt.Errorf("float value %q: %w", text, err)
		}
		return f, nil
	}
}

// encodeCursor stores the key values a scan resumes after.
//
// Values are held as text so the encoding does not depend on the driver's Go types,
// and the whole thing is JSON so a composite key needs no separator that a value
// could contain.
func encodeCursor(values []any) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}
	parts := make([]string, len(values))
	for i, v := range values {
		text, err := asString(v)
		if err != nil {
			return nil, err
		}
		parts[i] = text
	}
	encoded, err := json.Marshal(parts)
	if err != nil {
		return nil, fmt.Errorf("snapshot: encode cursor: %w", err)
	}
	return encoded, nil
}

func decodeCursor(encoded []byte, keyLen int) ([]any, error) {
	if len(encoded) == 0 {
		return nil, nil
	}
	var parts []string
	if err := json.Unmarshal(encoded, &parts); err != nil {
		return nil, fmt.Errorf("snapshot: cursor is unreadable, so resuming would skip or repeat rows: %w", err)
	}
	if len(parts) != keyLen {
		return nil, fmt.Errorf("snapshot: cursor has %d values but the key has %d columns", len(parts), keyLen)
	}
	out := make([]any, len(parts))
	for i, p := range parts {
		out[i] = p
	}
	return out, nil
}

// quoteIdentifier wraps a name in backticks, doubling any it contains. Identifiers
// cannot be bound as parameters, so they are quoted rather than interpolated raw.
func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// limiter paces a scan, since a backfill is the only part of changeflow that can
// slow the source down.
type limiter struct {
	rowsPerSec int
	last       time.Time
}

func newLimiter(rowsPerSec int) *limiter {
	return &limiter{rowsPerSec: rowsPerSec, last: time.Now()}
}

func (l *limiter) wait(ctx context.Context, rows int) error {
	if l.rowsPerSec <= 0 {
		return nil
	}

	want := time.Duration(float64(rows) / float64(l.rowsPerSec) * float64(time.Second))
	elapsed := time.Since(l.last)
	l.last = time.Now()
	if elapsed >= want {
		return nil
	}

	timer := time.NewTimer(want - elapsed)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		l.last = time.Now()
		return nil
	}
}

func parseUint(text string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSpace(text), 10, 64)
}

func parseInt(text string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(text), 10, 64)
}

func parseFloat(text string) (float64, error) {
	return strconv.ParseFloat(strings.TrimSpace(text), 64)
}
