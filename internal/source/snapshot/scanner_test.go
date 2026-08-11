package snapshot

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
)

type harness struct {
	db     *sql.DB
	writer *sql.DB
	meta   *schema.TableMeta
}

// newHarness connects as the replication user for reads and a privileged user for
// the fixtures, keeping the read-only grant honest.
func newHarness(t *testing.T, table string) *harness {
	t.Helper()

	dsn := os.Getenv("CHANGEFLOW_TEST_DSN")
	writeDSN := os.Getenv("CHANGEFLOW_TEST_WRITE_DSN")
	if dsn == "" || writeDSN == "" {
		t.Skip("set CHANGEFLOW_TEST_DSN and CHANGEFLOW_TEST_WRITE_DSN to run snapshot tests against MySQL")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.PingContext(t.Context()); err != nil {
		t.Fatalf("connect: %v", err)
	}

	writer, err := sql.Open("mysql", writeDSN)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	t.Cleanup(func() { writer.Close() })

	meta, err := schema.DBLoader{DB: db}.Load(t.Context(), "shop", table)
	if err != nil {
		t.Fatalf("load table definition: %v", err)
	}

	return &harness{db: db, writer: writer, meta: meta}
}

func (h *harness) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := h.writer.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// collect drains a scan.
func collect(t *testing.T, s *Snapshotter) []cdc.ChangeEvent {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	var events []cdc.ChangeEvent
	for ev := range s.Events(ctx) {
		events = append(events, ev)
	}
	if err := s.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return events
}

func (h *harness) scanner(t *testing.T, tune func(*Options)) *Snapshotter {
	t.Helper()

	opts := Options{
		DB:        h.db,
		Meta:      h.meta,
		Key:       h.meta.PrimaryKey,
		ChunkSize: 2,
		BaseSeq:   1_000_000,
	}
	if tune != nil {
		tune(&opts)
	}

	s, err := New(opts)
	if err != nil {
		t.Fatalf("new scanner: %v", err)
	}
	return s
}

// value reads a column from an event by name.
func value(t *testing.T, ev cdc.ChangeEvent, column string) any {
	t.Helper()
	c, ok := ev.Meta.Column(column)
	if !ok {
		t.Fatalf("column %s missing", column)
	}
	return ev.After[c.Position]
}

func TestScanReadsEveryRowInKeyOrder(t *testing.T) {
	h := newHarness(t, "orders")

	t.Cleanup(func() { h.writer.Exec("DELETE FROM orders WHERE id BETWEEN 30000 AND 30099") })
	for i := 0; i < 7; i++ {
		h.exec(t, "INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,5,'paid',1.00)", 30000+i)
	}

	// Chunked deliberately smaller than the data, so paging is exercised.
	events := collect(t, h.scanner(t, func(o *Options) { o.ChunkSize = 2 }))

	var seen []uint64
	for _, ev := range events {
		if id, ok := value(t, ev, "id").(uint64); ok && id >= 30000 && id <= 30099 {
			seen = append(seen, id)
		}
	}
	if len(seen) != 7 {
		t.Fatalf("scanned %d of the 7 inserted rows", len(seen))
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] <= seen[i-1] {
			t.Fatalf("rows were not returned in key order: %v", seen)
		}
	}
}

func TestScanMarksEventsAsSnapshotRows(t *testing.T) {
	h := newHarness(t, "orders")
	events := collect(t, h.scanner(t, nil))

	if len(events) == 0 {
		t.Fatal("expected the seeded rows to be scanned")
	}
	for _, ev := range events {
		if ev.Op != cdc.OpSnapshot {
			t.Fatalf("op = %s, want snapshot", ev.Op)
		}
		if ev.Before != nil {
			t.Error("a scanned row has no prior state")
		}
		if ev.GTID != "" {
			t.Error("a scanned row belongs to no transaction, so it carries no position")
		}
		// One version for every scanned row is what lets a concurrent change win.
		if ev.Seq != 1_000_000 {
			t.Fatalf("version = %d, want the base version for every row", ev.Seq)
		}
	}
}

// The two sources must produce the same value shapes, or the transform would need
// to know which one it is reading from.
func TestScannedValuesMatchTheBinlogShapes(t *testing.T) {
	h := newHarness(t, "orders")

	t.Cleanup(func() { h.writer.Exec("DELETE FROM orders WHERE id = 30200") })
	h.exec(t, `INSERT INTO orders (id,user_id,status,channels,total_amount,is_gift,note_latin1,metadata,placed_at)
	           VALUES (30200, 18446744073709551001, 'shipped', 'web,pos', 19.90, 1, 'x', '{"a":1}', '2026-08-11 10:00:00.000')`)

	var found *cdc.ChangeEvent
	for _, ev := range collect(t, h.scanner(t, func(o *Options) { o.ChunkSize = 100 })) {
		if id, ok := value(t, ev, "id").(uint64); ok && id == 30200 {
			found = &ev
			break
		}
	}
	if found == nil {
		t.Fatal("the inserted row was not scanned")
	}

	// Unsigned stays unsigned, so a value above 2^63 is exact rather than negative.
	if v, ok := value(t, *found, "user_id").(uint64); !ok || v != 18446744073709551001 {
		t.Errorf("user_id = %#v, want uint64(18446744073709551001)", value(t, *found, "user_id"))
	}
	// An ENUM arrives as a member number, as the binlog carries it, not as its label.
	if v, ok := value(t, *found, "status").(int64); !ok || v != 3 {
		t.Errorf("status = %#v, want the member number 3 for 'shipped'", value(t, *found, "status"))
	}
	// A SET arrives as a bitmask: web is bit 0 and pos is bit 3.
	if v, ok := value(t, *found, "channels").(int64); !ok || v != 0b1001 {
		t.Errorf("channels = %#v, want the bitmask 0b1001", value(t, *found, "channels"))
	}
	// An exact decimal, not a float or a string.
	if v, ok := value(t, *found, "total_amount").(decimal.Decimal); !ok {
		t.Errorf("total_amount = %#v, want a decimal", value(t, *found, "total_amount"))
	} else if v.StringFixed(2) != "19.90" {
		t.Errorf("total_amount = %s, want 19.90", v.StringFixed(2))
	}
	// A DATETIME arrives as text for the transform to interpret.
	if _, ok := value(t, *found, "placed_at").(string); !ok {
		t.Errorf("placed_at = %#v, want text", value(t, *found, "placed_at"))
	}
	// JSON is bytes, passed through untouched.
	if _, ok := value(t, *found, "metadata").([]byte); !ok {
		t.Errorf("metadata = %#v, want bytes", value(t, *found, "metadata"))
	}
}

// A generated column is absent from a binlog row image, so a scan must leave it out
// too or the two sources would disagree on the row's shape.
func TestScanOmitsGeneratedColumns(t *testing.T) {
	h := newHarness(t, "orders")

	events := collect(t, h.scanner(t, func(o *Options) { o.ChunkSize = 100 }))
	if len(events) == 0 {
		t.Fatal("expected rows")
	}

	c, ok := h.meta.Column("total_with_tax")
	if !ok {
		t.Skip("the development schema has no generated column")
	}
	if v := events[0].After[c.Position]; v != nil {
		t.Fatalf("generated column was populated with %#v; a binlog row would leave it absent", v)
	}
}

// An interrupted scan must resume rather than restart, or a large table could never
// finish on an unreliable connection.
func TestScanResumesFromACursor(t *testing.T) {
	h := newHarness(t, "orders")

	t.Cleanup(func() { h.writer.Exec("DELETE FROM orders WHERE id BETWEEN 30300 AND 30399") })
	for i := 0; i < 6; i++ {
		h.exec(t, "INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,5,'paid',1.00)", 30300+i)
	}

	// First pass: stop after one chunk by refusing to record further progress.
	var cursorAfterFirstChunk []byte
	chunks := 0
	first := h.scanner(t, func(o *Options) {
		o.ChunkSize = 2
		o.Progress = func(_ context.Context, _ uint64, cursor []byte) error {
			chunks++
			cursorAfterFirstChunk = append([]byte(nil), cursor...)
			if chunks == 1 {
				return context.Canceled // stand in for a crash
			}
			return nil
		}
	})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	var firstPass []cdc.ChangeEvent
	for ev := range first.Events(ctx) {
		firstPass = append(firstPass, ev)
	}
	if len(cursorAfterFirstChunk) == 0 {
		t.Fatal("no cursor was recorded")
	}
	if first.Done() {
		t.Fatal("an interrupted scan must not report itself as complete")
	}

	// Second pass: resume from the recorded cursor.
	second := h.scanner(t, func(o *Options) {
		o.ChunkSize = 2
		o.Cursor = cursorAfterFirstChunk
	})
	secondPass := collect(t, second)

	if !second.Done() {
		t.Error("a scan that reached the end must report itself as complete")
	}

	// No row appears in both passes: the cursor resumes strictly after the last row
	// handed on.
	seen := map[uint64]int{}
	for _, ev := range append(firstPass, secondPass...) {
		if id, ok := value(t, ev, "id").(uint64); ok {
			seen[id]++
		}
	}
	for id, count := range seen {
		if count > 1 {
			t.Errorf("row %d was scanned %d times across the resume", id, count)
		}
	}
}

func TestScanHandlesCompositeKeys(t *testing.T) {
	h := newHarness(t, "order_items")

	t.Cleanup(func() { h.writer.Exec("DELETE FROM order_items WHERE order_id BETWEEN 30400 AND 30499") })
	for i := 0; i < 5; i++ {
		h.exec(t, "INSERT INTO order_items (order_id,sku,qty,unit_price) VALUES (?,?,1,1.00)", 30400, "SKU-"+string(rune('a'+i)))
	}

	// A chunk smaller than the data forces the row-value comparison to page correctly
	// within one order_id.
	events := collect(t, h.scanner(t, func(o *Options) { o.ChunkSize = 2 }))

	var skus []string
	for _, ev := range events {
		if id, ok := value(t, ev, "order_id").(uint64); ok && id == 30400 {
			skus = append(skus, value(t, ev, "sku").(string))
		}
	}
	if len(skus) != 5 {
		t.Fatalf("scanned %d of 5 rows sharing an order_id: %v", len(skus), skus)
	}
}

func TestScanReportsProgress(t *testing.T) {
	h := newHarness(t, "orders")

	t.Cleanup(func() { h.writer.Exec("DELETE FROM orders WHERE id BETWEEN 30500 AND 30599") })
	for i := 0; i < 5; i++ {
		h.exec(t, "INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,5,'paid',1.00)", 30500+i)
	}

	var reports []uint64
	s := h.scanner(t, func(o *Options) {
		o.ChunkSize = 2
		o.Progress = func(_ context.Context, rowsRead uint64, _ []byte) error {
			reports = append(reports, rowsRead)
			return nil
		}
	})
	events := collect(t, s)

	if len(reports) == 0 {
		t.Fatal("progress was never reported, so an interrupted scan could not resume")
	}
	for i := 1; i < len(reports); i++ {
		if reports[i] <= reports[i-1] {
			t.Fatalf("progress did not increase: %v", reports)
		}
	}
	if got := s.RowsRead(); got != uint64(len(events)) {
		t.Errorf("RowsRead() = %d, but %d events were emitted", got, len(events))
	}
}

// A scan is the only part of changeflow that can slow the source down, so the
// throttle has to work.
func TestScanRespectsTheRateLimit(t *testing.T) {
	h := newHarness(t, "orders")

	t.Cleanup(func() { h.writer.Exec("DELETE FROM orders WHERE id BETWEEN 30600 AND 30699") })
	for i := 0; i < 8; i++ {
		h.exec(t, "INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,5,'paid',1.00)", 30600+i)
	}

	start := time.Now()
	events := collect(t, h.scanner(t, func(o *Options) {
		o.ChunkSize = 2
		o.MaxRowsPerSec = 8 // roughly a quarter second per two-row chunk
	}))
	elapsed := time.Since(start)

	if len(events) < 8 {
		t.Fatalf("scanned %d rows, expected at least the 8 inserted", len(events))
	}
	// Four chunks of the inserted rows alone should take around a second; allow ample
	// slack so a fast machine does not make this flaky, while still catching a
	// throttle that does nothing.
	if elapsed < 500*time.Millisecond {
		t.Errorf("scan of %d rows at 8 rows per second took only %v", len(events), elapsed)
	}
}

func TestScanCanBeCancelled(t *testing.T) {
	h := newHarness(t, "orders")

	ctx, cancel := context.WithCancel(t.Context())
	s := h.scanner(t, func(o *Options) { o.ChunkSize = 1 })

	events := s.Events(ctx)
	<-events // take one row, leaving the scan mid-table
	cancel()

	// The channel must close rather than block forever.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-events:
			if !ok {
				if s.Done() {
					t.Error("a cancelled scan must not report itself as complete")
				}
				return
			}
		case <-deadline:
			t.Fatal("the scan did not stop after cancellation")
		}
	}
}

func TestNewRejectsIncompleteOptions(t *testing.T) {
	meta := &schema.TableMeta{
		Schema: "shop", Table: "orders",
		Columns:    []schema.Column{{Name: "id", Position: 0, DataType: "bigint", ColumnType: "bigint unsigned", Unsigned: true}},
		PrimaryKey: []string{"id"},
	}
	db, err := sql.Open("mysql", "user:pass@tcp(127.0.0.1:1)/x")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	valid := Options{DB: db, Meta: meta, Key: []string{"id"}, ChunkSize: 10, BaseSeq: 1}

	for _, tc := range []struct {
		name  string
		spoil func(*Options)
	}{
		{"no database", func(o *Options) { o.DB = nil }},
		{"no table definition", func(o *Options) { o.Meta = nil }},
		{"no key", func(o *Options) { o.Key = nil }},
		{"unknown key column", func(o *Options) { o.Key = []string{"nope"} }},
		{"zero chunk size", func(o *Options) { o.ChunkSize = 0 }},
		// Without a base version, snapshot rows would carry version zero and could
		// overwrite a newer change.
		{"no base version", func(o *Options) { o.BaseSeq = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := valid
			tc.spoil(&opts)
			if _, err := New(opts); err == nil {
				t.Fatal("expected the scanner to refuse these options")
			}
		})
	}
}

// A cursor that cannot be read must stop the scan: guessing would either skip rows
// or scan some twice.
func TestUnreadableCursorIsRefused(t *testing.T) {
	h := newHarness(t, "orders")

	s := h.scanner(t, func(o *Options) { o.Cursor = []byte("not json") })
	for range s.Events(t.Context()) {
		t.Fatal("expected no rows from a scan with an unreadable cursor")
	}
	if s.Err() == nil {
		t.Fatal("expected an error for an unreadable cursor")
	}
	if s.Done() {
		t.Fatal("a scan that never ran must not report itself as complete")
	}
}
