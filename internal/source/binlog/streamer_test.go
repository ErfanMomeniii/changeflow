package binlog

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
)

type counterSeq struct{ n uint64 }

func (c *counterSeq) Next(context.Context) (uint64, error) {
	c.n++
	return c.n, nil
}

var nextServerID atomic.Uint32

type idRange struct{ from, to uint64 }

type liveHarness struct {
	ids    idRange
	db     *sql.DB
	writer *sql.DB
	stream *Streamer
	events <-chan cdc.ChangeEvent
	cancel context.CancelFunc
}

func newLiveHarness(t *testing.T, tables ...string) *liveHarness {
	t.Helper()
	return newLiveHarnessForIDs(t, idRange{}, tables...)
}

func newLiveHarnessForIDs(t *testing.T, ids idRange, tables ...string) *liveHarness {
	t.Helper()
	dsn := os.Getenv("CHANGEFLOW_TEST_DSN")
	if dsn == "" {
		t.Skip("set CHANGEFLOW_TEST_DSN to run binlog tests against MySQL")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.PingContext(t.Context()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	writeDSN := os.Getenv("CHANGEFLOW_TEST_WRITE_DSN")
	if writeDSN == "" {
		t.Skip("set CHANGEFLOW_TEST_WRITE_DSN to a user allowed to modify the source table")
	}
	writer, writerErr := sql.Open("mysql", writeDSN)
	if writerErr != nil {
		t.Fatalf("open writer: %v", writerErr)
	}
	t.Cleanup(func() { writer.Close() })
	if err := writer.PingContext(t.Context()); err != nil {
		t.Fatalf("connect writer: %v", err)
	}
	var gtid string
	if err := db.QueryRowContext(t.Context(), "SELECT @@GLOBAL.gtid_executed").Scan(&gtid); err != nil {
		t.Fatalf("read position: %v", err)
	}
	gtid = collapse(gtid)
	if gtid == "" {
		t.Skip("server has logged no transactions, so there is no position to stream from")
	}
	host, port := "127.0.0.1", uint16(13306)
	if h := os.Getenv("CHANGEFLOW_TEST_HOST"); h != "" {
		host = h
	}
	s, err := New(Options{
		Host:      host,
		Port:      port,
		User:      "cdc",
		Password:  "cdc",
		ServerID:  4100 + nextServerID.Add(1),
		StartGTID: gtid,
		Tables:    tables,
		Schemas:   schema.NewStore(schema.DBLoader{DB: db}),
		Sequencer: &counterSeq{},
	})
	if err != nil {
		t.Fatalf("new streamer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	events := s.Events(ctx)
	t.Cleanup(func() { s.Close() })
	time.Sleep(1500 * time.Millisecond)
	return &liveHarness{ids: ids, db: db, writer: writer, stream: s, events: events, cancel: cancel}
}

func (h *liveHarness) next(t *testing.T) cdc.ChangeEvent {
	t.Helper()
	deadline := time.After(15 * time.Second)
	for {
		select {
		case ev, ok := <-h.events:
			if !ok {
				t.Fatalf("stream closed: %v", h.stream.Err())
			}
			if h.owns(ev) {
				return ev
			}
		case <-deadline:
			t.Fatalf("no event for this test arrived; streamer error: %v", h.stream.Err())
			return cdc.ChangeEvent{}
		}
	}
}

func (h *liveHarness) owns(ev cdc.ChangeEvent) bool {
	if h.ids == (idRange{}) {
		return true
	}
	values := ev.Values()
	if len(values) == 0 {
		return false
	}
	id, ok := values[0].(uint64)
	if !ok {
		return false
	}
	return id >= h.ids.from && id <= h.ids.to
}

func (h *liveHarness) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := h.writer.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func TestStreamerEmitsInsertUpdateDelete(t *testing.T) {
	h := newLiveHarnessForIDs(t, idRange{9001, 9001}, "shop.orders")
	h.exec(t, "INSERT INTO orders (id,user_id,status,total_amount) VALUES (9001,5,'paid',12.34)")
	h.exec(t, "UPDATE orders SET status='shipped' WHERE id=9001")
	h.exec(t, "DELETE FROM orders WHERE id=9001")
	t.Cleanup(func() { h.writer.Exec("DELETE FROM orders WHERE id=9001") })
	insert := h.next(t)
	if insert.Operation != cdc.OperationInsert {
		t.Fatalf("first event operation = %s, want insert", insert.Operation)
	}
	if insert.Meta.Name() != "shop.orders" {
		t.Errorf("table = %s", insert.Meta.Name())
	}
	if insert.GTID == "" {
		t.Error("event carries no position, so nothing could be checkpointed")
	}
	if insert.Seq == 0 {
		t.Error("event carries no version")
	}
	if insert.Timestamp.IsZero() {
		t.Error("event carries no timestamp, so lag could not be measured")
	}
	update := h.next(t)
	if update.Operation != cdc.OperationUpdate {
		t.Fatalf("second event operation = %s, want update", update.Operation)
	}
	if update.Before == nil || update.After == nil {
		t.Fatal("an update must carry both before and after values")
	}
	del := h.next(t)
	if del.Operation != cdc.OperationDelete {
		t.Fatalf("third event operation = %s, want delete", del.Operation)
	}
	if del.Before == nil {
		t.Fatal("a delete must carry the prior values, which is the only place its key still exists")
	}
	if del.After != nil {
		t.Error("a delete has no new values")
	}
}

// Versions must increase, since they are what decides which of two writes to one
// key wins.
func TestStreamerVersionsIncrease(t *testing.T) {
	h := newLiveHarnessForIDs(t, idRange{9100, 9199}, "shop.orders")
	for i := 0; i < 3; i++ {
		h.exec(t, "INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,5,'paid',1.00)", 9100+i)
	}
	t.Cleanup(func() { h.writer.Exec("DELETE FROM orders WHERE id BETWEEN 9100 AND 9200") })
	var prev uint64
	for i := 0; i < 3; i++ {
		ev := h.next(t)
		if ev.Seq <= prev {
			t.Fatalf("version %d did not increase past %d", ev.Seq, prev)
		}
		prev = ev.Seq
	}
}

// The values must arrive in the shapes the transform expects, which is the contract
// between decoding and encoding.
func TestStreamerDecodesValuesForTheTransform(t *testing.T) {
	h := newLiveHarnessForIDs(t, idRange{9300, 9300}, "shop.orders")
	h.exec(t, `INSERT INTO orders (id,user_id,status,channels,total_amount,is_gift,placed_at)
	           VALUES (9300, 18446744073709551000, 'shipped', 'web,pos', 19.90, 1, '2026-08-11 10:00:00.000')`)
	t.Cleanup(func() { h.writer.Exec("DELETE FROM orders WHERE id=9300") })
	ev := h.next(t)
	row := ev.After
	col := func(name string) any {
		c, ok := ev.Meta.Column(name)
		if !ok {
			t.Fatalf("column %s missing", name)
		}
		return row[c.Position]
	}
	if v, ok := col("user_id").(uint64); !ok || v != 18446744073709551000 {
		t.Errorf("user_id = %#v, want uint64(18446744073709551000)", col("user_id"))
	}
	if v, ok := col("total_amount").(decimal.Decimal); !ok {
		t.Errorf("total_amount = %#v, want a decimal", col("total_amount"))
	} else if v.String() != "19.9" && v.StringFixed(2) != "19.90" {
		t.Errorf("total_amount = %s, want 19.90", v.StringFixed(2))
	}
	if _, ok := asInt(col("status")); !ok {
		t.Errorf("status = %#v, want an enum member number", col("status"))
	}
	if _, ok := asInt(col("channels")); !ok {
		t.Errorf("channels = %#v, want a set bitmask", col("channels"))
	}
	if _, ok := col("placed_at").(string); !ok {
		t.Errorf("placed_at = %#v, want the server's text form", col("placed_at"))
	}
}

func asInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case uint64:
		return int64(x), true
	default:
		return 0, false
	}
}

// Tables outside the configured set must not produce events, so an unrelated write
// cannot consume versions or reach a sink.
func TestStreamerIgnoresUnwatchedTables(t *testing.T) {
	h := newLiveHarness(t, "shop.order_items")
	h.exec(t, "INSERT INTO orders (id,user_id,status,total_amount) VALUES (9400,5,'paid',1.00)")
	h.exec(t, "INSERT INTO order_items (order_id,sku,qty,unit_price) VALUES (9400,'SKU-X',1,1.00)")
	t.Cleanup(func() {
		h.writer.Exec("DELETE FROM order_items WHERE order_id=9400")
		h.writer.Exec("DELETE FROM orders WHERE id=9400")
	})
	ev := h.next(t)
	if ev.Meta.Name() != "shop.order_items" {
		t.Fatalf("received an event for %s, which is not watched", ev.Meta.Name())
	}
}

// Adding a column changes the row width, and a stale definition would attribute
// values to the wrong fields.
func TestStreamerReloadsDefinitionAfterAlter(t *testing.T) {
	h := newLiveHarnessForIDs(t, idRange{9500, 9599}, "shop.orders")
	t.Cleanup(func() {
		h.writer.Exec("ALTER TABLE orders DROP COLUMN temp_note")
		h.writer.Exec("DELETE FROM orders WHERE id IN (9500,9501)")
	})
	h.exec(t, "INSERT INTO orders (id,user_id,status,total_amount) VALUES (9500,5,'paid',1.00)")
	first := h.next(t)
	widthBefore := len(first.Meta.Columns)
	h.exec(t, "ALTER TABLE orders ADD COLUMN temp_note VARCHAR(16) NULL")
	h.exec(t, "INSERT INTO orders (id,user_id,status,total_amount,temp_note) VALUES (9501,5,'paid',1.00,'hi')")
	second := h.next(t)
	if len(second.Meta.Columns) != widthBefore+1 {
		t.Fatalf("definition still has %d columns after adding one to %d", len(second.Meta.Columns), widthBefore)
	}
	if len(second.After) != len(second.Meta.Columns) {
		t.Fatalf("row carries %d values against %d columns", len(second.After), len(second.Meta.Columns))
	}
	if _, ok := second.Meta.Column("temp_note"); !ok {
		t.Error("the new column is missing from the reloaded definition")
	}
}

// A position must be the cumulative set of executed transactions, not the last
// transaction's identifier.
//
// Resuming from a single identifier tells the server we have executed only that
// transaction, so it replays everything else it retains. That resurrects rows whose
// deletes have since been purged, and re-does the entire binlog on every restart.
func TestEventsCarryTheCumulativeExecutedSet(t *testing.T) {
	h := newLiveHarnessForIDs(t, idRange{9600, 9699}, "shop.orders")
	t.Cleanup(func() { h.writer.Exec("DELETE FROM orders WHERE id BETWEEN 9600 AND 9700") })
	h.exec(t, "INSERT INTO orders (id,user_id,status,total_amount) VALUES (9600,5,'paid',1.00)")
	h.exec(t, "INSERT INTO orders (id,user_id,status,total_amount) VALUES (9601,5,'paid',1.00)")
	first := h.next(t)
	second := h.next(t)
	if !strings.Contains(first.GTID, "-") {
		t.Errorf("position %q looks like one transaction rather than a cumulative set", first.GTID)
	}
	firstSet, err := mysql.ParseMysqlGTIDSet(first.GTID)
	if err != nil {
		t.Fatalf("parse %q: %v", first.GTID, err)
	}
	secondSet, err := mysql.ParseMysqlGTIDSet(second.GTID)
	if err != nil {
		t.Fatalf("parse %q: %v", second.GTID, err)
	}
	if !secondSet.Contain(firstSet) {
		t.Errorf("position went backwards: %q does not contain %q", second.GTID, first.GTID)
	}
	if secondSet.Equal(firstSet) {
		t.Errorf("position did not advance after a committed transaction: still %q", second.GTID)
	}
}

// Resuming from a reported position must not re-deliver rows from transactions
// already folded into it.
//
// The transaction an event belongs to is deliberately excluded from its position,
// since it may be only partly written, so that one transaction does replay. Earlier
// ones must not: before the position became a cumulative set, resuming replayed the
// entire retained binlog.
func TestResumingFromAPositionDoesNotReplayEarlierTransactions(t *testing.T) {
	h := newLiveHarnessForIDs(t, idRange{9700, 9799}, "shop.orders")
	t.Cleanup(func() { h.writer.Exec("DELETE FROM orders WHERE id BETWEEN 9700 AND 9800") })
	h.exec(t, "INSERT INTO orders (id,user_id,status,total_amount) VALUES (9700,5,'paid',1.00)")
	h.exec(t, "INSERT INTO orders (id,user_id,status,total_amount) VALUES (9701,5,'paid',1.00)")
	h.next(t)
	second := h.next(t)
	resumeFrom := second.GTID
	h.stream.Close()
	resumed, err := New(Options{
		Host: "127.0.0.1", Port: 13306, User: "cdc", Password: "cdc",
		ServerID:  4100 + nextServerID.Add(1),
		StartGTID: resumeFrom,
		Tables:    []string{"shop.orders"},
		Schemas:   schema.NewStore(schema.DBLoader{DB: h.db}),
		Sequencer: &counterSeq{},
	})
	if err != nil {
		t.Fatalf("new streamer: %v", err)
	}
	defer resumed.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	events := resumed.Events(ctx)
	var replayed []uint64
	deadline := time.After(3 * time.Second)
collect:
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				break collect
			}
			if id, isUint := ev.Values()[0].(uint64); isUint {
				replayed = append(replayed, id)
			}
		case <-deadline:
			break collect
		}
	}
	for _, id := range replayed {
		if id == 9700 {
			t.Fatalf("resuming from %q replayed row 9700, whose transaction the position already covers (replayed: %v)",
				resumeFrom, replayed)
		}
	}
	for _, id := range replayed {
		if id == 1 || id == 2 {
			t.Fatalf("resuming replayed seed row %d, so the position is not being honoured (replayed: %v)", id, replayed)
		}
	}
}

// Replaying history across a schema change is normal, and a row written before a
// column was dropped must still decode: refusing would leave the stream permanently
// stuck at that position.
func TestRowsWrittenBeforeAColumnWasDroppedStillDecode(t *testing.T) {
	h := newLiveHarnessForIDs(t, idRange{9900, 9999}, "shop.orders")
	t.Cleanup(func() {
		h.writer.Exec("ALTER TABLE orders DROP COLUMN scratch")
		h.writer.Exec("DELETE FROM orders WHERE id BETWEEN 9900 AND 9999")
	})
	h.exec(t, "ALTER TABLE orders ADD COLUMN scratch VARCHAR(16) NULL")
	h.exec(t, "INSERT INTO orders (id,user_id,status,total_amount,scratch) VALUES (9900,5,'paid',1.00,'x')")
	h.exec(t, "ALTER TABLE orders DROP COLUMN scratch")
	ev := h.next(t)
	if ev.Operation != cdc.OperationInsert {
		t.Fatalf("operation = %s, want insert", ev.Operation)
	}
	if len(ev.After) != len(ev.Meta.Columns) {
		t.Fatalf("row has %d values against %d columns", len(ev.After), len(ev.Meta.Columns))
	}
	for _, tc := range []struct {
		column string
		want   any
	}{
		{"id", uint64(9900)},
		{"user_id", uint64(5)},
	} {
		c, ok := ev.Meta.Column(tc.column)
		if !ok {
			t.Fatalf("column %s missing", tc.column)
		}
		if got := ev.After[c.Position]; got != tc.want {
			t.Errorf("%s = %#v, want %#v", tc.column, got, tc.want)
		}
	}
}

// A column added after a row was written has no value in it, so it decodes as null
// rather than shifting every later value along by one.
func TestRowsWrittenBeforeAColumnWasAddedStillDecode(t *testing.T) {
	h := newLiveHarnessForIDs(t, idRange{9800, 9899}, "shop.orders")
	t.Cleanup(func() {
		h.writer.Exec("ALTER TABLE orders DROP COLUMN added_later")
		h.writer.Exec("DELETE FROM orders WHERE id BETWEEN 9800 AND 9899")
	})
	h.exec(t, "INSERT INTO orders (id,user_id,status,total_amount) VALUES (9800,5,'paid',1.00)")
	h.exec(t, "ALTER TABLE orders ADD COLUMN added_later VARCHAR(16) NULL")
	h.exec(t, "INSERT INTO orders (id,user_id,status,total_amount) VALUES (9801,5,'paid',1.00)")
	first := h.next(t)
	if len(first.After) != len(first.Meta.Columns) {
		t.Fatalf("row has %d values against %d columns", len(first.After), len(first.Meta.Columns))
	}
	c, ok := first.Meta.Column("id")
	if !ok {
		t.Fatal("id column missing")
	}
	if got := first.After[c.Position]; got != uint64(9800) {
		t.Errorf("id = %#v, want 9800; values may have shifted", got)
	}
	second := h.next(t)
	if got := second.After[c.Position]; got != uint64(9801) {
		t.Errorf("id = %#v, want 9801", got)
	}
}

func TestNewRejectsIncompleteOptions(t *testing.T) {
	store := schema.NewStore(schema.DBLoader{})
	valid := Options{
		Host: "127.0.0.1", ServerID: 1, StartGTID: "uuid:1-2",
		Schemas: store, Sequencer: &counterSeq{},
	}
	for _, tc := range []struct {
		name  string
		spoil func(*Options)
	}{
		{"no host", func(o *Options) { o.Host = "" }},
		{"no server id", func(o *Options) { o.ServerID = 0 }},
		{"no start position", func(o *Options) { o.StartGTID = "" }},
		{"no schema store", func(o *Options) { o.Schemas = nil }},
		{"no sequencer", func(o *Options) { o.Sequencer = nil }},
		{"table without a database", func(o *Options) { o.Tables = []string{"orders"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := valid
			tc.spoil(&opts)
			if _, err := New(opts); err == nil {
				t.Fatal("expected the reader to refuse these options")
			}
		})
	}
}

// The one failure a restart cannot fix has to say so. Without this it arrives as a bare
// server error, and the actual remedy — a rescan — is left for someone to work out during
// an incident.
func TestAPurgedPositionSaysWhatToDoAboutIt(t *testing.T) {
	err := explain(&mysql.MyError{
		Code:    errBinlogReadFailure,
		Message: "The replica is connecting using CHANGE REPLICATION SOURCE TO MASTER_AUTO_POSITION = 1, but the source has purged binary logs containing GTIDs that the replica requires",
	})
	if !errors.Is(err, ErrPositionPurged) {
		t.Fatalf("error = %v, want it to be recognisable as a purged position", err)
	}
	if !strings.Contains(err.Error(), "resnapshot") {
		t.Errorf("the error should name the remedy, got %v", err)
	}
	if !strings.Contains(err.Error(), "purged binary logs") {
		t.Errorf("the server's own words should survive, got %v", err)
	}
}

// Everything else is a connection or protocol problem a restart may well fix, and
// dressing it up as a lost position would send someone to rescan a table for nothing.
func TestOtherServerErrorsArePassedThrough(t *testing.T) {
	original := &mysql.MyError{Code: 1045, Message: "Access denied for user 'cdc'@'%'"}
	err := explain(original)
	if errors.Is(err, ErrPositionPurged) {
		t.Error("an access denial was reported as a purged position")
	}
	if err != original {
		t.Errorf("error = %v, want it returned unchanged", err)
	}
}
