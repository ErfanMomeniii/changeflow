package binlog

import (
	"context"
	"database/sql"
	"os"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
)

// counterSeq hands out increasing versions without a store, since these tests are
// about decoding rather than persistence.
type counterSeq struct{ n uint64 }

func (c *counterSeq) Next(context.Context) (uint64, error) {
	c.n++
	return c.n, nil
}

// nextServerID keeps each harness on its own replica id. Reusing one would make
// the master evict the previous connection, and the test would then hang waiting
// for events on a stream that was killed.
var nextServerID atomic.Uint32

type liveHarness struct {
	db     *sql.DB
	writer *sql.DB
	stream *Streamer
	events <-chan cdc.ChangeEvent
	cancel context.CancelFunc
}

// newLiveHarness connects to the MySQL named by CHANGEFLOW_TEST_DSN, starts
// replication from the current position, and returns the event stream.
func newLiveHarness(t *testing.T, tables ...string) *liveHarness {
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

	// Give replication a moment to register before the test writes.
	time.Sleep(1500 * time.Millisecond)

	return &liveHarness{db: db, writer: writer, stream: s, events: events, cancel: cancel}
}

// next waits for one event.
func (h *liveHarness) next(t *testing.T) cdc.ChangeEvent {
	t.Helper()
	select {
	case ev, ok := <-h.events:
		if !ok {
			t.Fatalf("stream closed: %v", h.stream.Err())
		}
		return ev
	case <-time.After(10 * time.Second):
		t.Fatalf("no event arrived; streamer error: %v", h.stream.Err())
		return cdc.ChangeEvent{}
	}
}

func (h *liveHarness) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := h.writer.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func TestStreamerEmitsInsertUpdateDelete(t *testing.T) {
	h := newLiveHarness(t, "shop.orders")

	h.exec(t, "INSERT INTO orders (id,user_id,status,total_amount) VALUES (9001,5,'paid',12.34)")
	h.exec(t, "UPDATE orders SET status='shipped' WHERE id=9001")
	h.exec(t, "DELETE FROM orders WHERE id=9001")
	t.Cleanup(func() { h.writer.Exec("DELETE FROM orders WHERE id=9001") })

	insert := h.next(t)
	if insert.Op != cdc.OpInsert {
		t.Fatalf("first event op = %s, want insert", insert.Op)
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
	if update.Op != cdc.OpUpdate {
		t.Fatalf("second event op = %s, want update", update.Op)
	}
	// An update needs both images: the prior one to find the old key, the new one to
	// write.
	if update.Before == nil || update.After == nil {
		t.Fatal("an update must carry both before and after values")
	}

	del := h.next(t)
	if del.Op != cdc.OpDelete {
		t.Fatalf("third event op = %s, want delete", del.Op)
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
	h := newLiveHarness(t, "shop.orders")

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
	h := newLiveHarness(t, "shop.orders")

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

	// An unsigned column must arrive unsigned, or values above 2^63 read negative.
	if v, ok := col("user_id").(uint64); !ok || v != 18446744073709551000 {
		t.Errorf("user_id = %#v, want uint64(18446744073709551000)", col("user_id"))
	}
	// An exact decimal, not a float.
	if v, ok := col("total_amount").(decimal.Decimal); !ok {
		t.Errorf("total_amount = %#v, want a decimal", col("total_amount"))
	} else if v.String() != "19.9" && v.StringFixed(2) != "19.90" {
		t.Errorf("total_amount = %s, want 19.90", v.StringFixed(2))
	}
	// ENUM and SET arrive as numbers; resolving them to labels needs the definition,
	// which is why the event carries it.
	if _, ok := asInt(col("status")); !ok {
		t.Errorf("status = %#v, want an enum member number", col("status"))
	}
	if _, ok := asInt(col("channels")); !ok {
		t.Errorf("channels = %#v, want a set bitmask", col("channels"))
	}
	// A DATETIME arrives as text, so the transform can interpret it in the source's
	// zone rather than the reader's.
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
	h := newLiveHarness(t, "shop.orders")

	// Registered before anything can fail, so a mid-test failure still tidies up.
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
