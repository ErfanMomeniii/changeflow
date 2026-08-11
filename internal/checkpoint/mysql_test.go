package checkpoint

import (
	"database/sql"
	"errors"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

// testStore connects to the MySQL named by CHANGEFLOW_TEST_DSN and returns a
// store on a table unique to the calling test, dropped afterwards. Tests skip
// when no DSN is set, so the default suite needs no containers.
func testStore(t *testing.T) (*MySQLStore, *sql.DB) {
	t.Helper()

	// A distinct variable from the source database: the checkpoint store lives in
	// its own schema with its own grants, and these tests create and drop tables in
	// it. Sharing one variable would point them at a database where that is rightly
	// forbidden.
	dsn := os.Getenv("CHANGEFLOW_TEST_META_DSN")
	if dsn == "" {
		t.Skip("set CHANGEFLOW_TEST_META_DSN to run checkpoint store tests against MySQL")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if err := db.PingContext(t.Context()); err != nil {
		t.Fatalf("connect: %v", err)
	}

	table := "cp_test_" + sanitizeForTable(t.Name())
	store, err := NewMySQLStore(db, table)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.EnsureSchema(t.Context()); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
	})

	return store, db
}

func sanitizeForTable(name string) string {
	out := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

func TestMySQLStoreEnsureSchemaIsRepeatable(t *testing.T) {
	store, _ := testStore(t)

	// Startup runs this every time, so a second call must be a no-op rather than
	// an error.
	if err := store.EnsureSchema(t.Context()); err != nil {
		t.Fatalf("second ensure schema: %v", err)
	}
}

func TestMySQLStoreReportsMissingStream(t *testing.T) {
	store, _ := testStore(t)

	if _, err := store.Load(t.Context(), "never-seen"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestMySQLStoreRoundTripsEveryField(t *testing.T) {
	store, _ := testStore(t)

	want := Checkpoint{
		Stream:            "orders_to_es",
		GTIDSet:           "ac8fec9f-9576-11f1-810c-16613dc98230:1-4211",
		SnapshotDone:      true,
		SnapshotStartGTID: "ac8fec9f-9576-11f1-810c-16613dc98230:1-9000",
		SnapshotCursor:    []byte("18446744073709551001"),
		SnapshotBaseSeq:   1 << 50,
		SnapshotRowsDone:  340_000,
		SnapshotRowsTotal: 1_000_000,
		// Above 2^63 to prove the column is unsigned end to end.
		SeqWatermark:  18_446_744_073_709_551_000,
		LastEventTsMs: 1_786_000_000_000,
		LastError:     "elasticsearch: 429 rejected",
		SchemaVersion: SchemaVersion,
	}

	if err := store.Save(t.Context(), want); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := store.Load(t.Context(), want.Stream)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if got.GTIDSet != want.GTIDSet ||
		got.SnapshotDone != want.SnapshotDone ||
		got.SnapshotStartGTID != want.SnapshotStartGTID ||
		string(got.SnapshotCursor) != string(want.SnapshotCursor) ||
		got.SnapshotBaseSeq != want.SnapshotBaseSeq ||
		got.SnapshotRowsDone != want.SnapshotRowsDone ||
		got.SnapshotRowsTotal != want.SnapshotRowsTotal ||
		got.SeqWatermark != want.SeqWatermark ||
		got.LastEventTsMs != want.LastEventTsMs ||
		got.LastError != want.LastError {
		t.Fatalf("round trip changed the checkpoint:\n got %+v\nwant %+v", got, want)
	}
	if got.UpdatedAt.IsZero() {
		t.Error("expected updated_at to be set by the store")
	}
}

func TestMySQLStoreUpsertsExistingStream(t *testing.T) {
	store, _ := testStore(t)
	ctx := t.Context()

	first := Checkpoint{Stream: "orders_to_es", GTIDSet: "uuid:1-10", SeqWatermark: 100}
	if err := store.Save(ctx, first); err != nil {
		t.Fatalf("save: %v", err)
	}
	second := Checkpoint{Stream: "orders_to_es", GTIDSet: "uuid:1-20", SeqWatermark: 200, SnapshotDone: true}
	if err := store.Save(ctx, second); err != nil {
		t.Fatalf("resave: %v", err)
	}

	got, err := store.Load(ctx, "orders_to_es")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.GTIDSet != "uuid:1-20" || got.SeqWatermark != 200 || !got.SnapshotDone {
		t.Fatalf("expected the second save to win, got %+v", got)
	}

	all, err := store.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected one row after an upsert, got %d", len(all))
	}
}

// A binary cursor must survive unchanged: a primary key can hold arbitrary bytes,
// and a mangled cursor would resume a snapshot in the wrong place.
func TestMySQLStorePreservesBinaryCursor(t *testing.T) {
	store, _ := testStore(t)

	cursor := []byte{0x00, 0xff, 0x41, 0x00, 0xfe}
	if err := store.Save(t.Context(), Checkpoint{Stream: "s", SnapshotCursor: cursor}); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := store.Load(t.Context(), "s")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if string(got.SnapshotCursor) != string(cursor) {
		t.Fatalf("cursor changed: got %x, want %x", got.SnapshotCursor, cursor)
	}
}

func TestMySQLStoreRefusesNewerSchemaVersion(t *testing.T) {
	store, db := testStore(t)

	if err := store.Save(t.Context(), Checkpoint{Stream: "s", GTIDSet: "uuid:1-1"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	// Simulate a newer changeflow having written this row.
	if _, err := db.ExecContext(t.Context(),
		"UPDATE "+store.table+" SET schema_version = ? WHERE stream = ?", SchemaVersion+1, "s"); err != nil {
		t.Fatalf("bump version: %v", err)
	}

	_, err := store.Load(t.Context(), "s")
	if err == nil {
		t.Fatal("expected a refusal to interpret a newer schema version")
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatal("a newer row exists; reporting it as missing would trigger a needless re-snapshot")
	}
}

func TestMySQLStoreRejectsOversizedStreamName(t *testing.T) {
	store, _ := testStore(t)

	long := ""
	for len(long) <= maxStreamNameLen {
		long += "x"
	}

	if err := store.Save(t.Context(), Checkpoint{Stream: long}); err == nil {
		t.Fatal("expected a name longer than the column to be rejected before reaching MySQL")
	}
}

func TestStreamLockIsExclusive(t *testing.T) {
	store, _ := testStore(t)
	ctx := t.Context()

	held, err := store.Lock(ctx, "orders_to_es")
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	defer held.Release(ctx)

	if _, err := store.Lock(ctx, "orders_to_es"); !errors.Is(err, ErrStreamLocked) {
		t.Fatalf("expected ErrStreamLocked for a second holder, got %v", err)
	}

	// A different stream is unaffected.
	other, err := store.Lock(ctx, "orders_to_clickhouse")
	if err != nil {
		t.Fatalf("expected an unrelated stream to be lockable: %v", err)
	}
	if err := other.Release(ctx); err != nil {
		t.Fatalf("release other: %v", err)
	}
}

func TestStreamLockIsReacquirableAfterRelease(t *testing.T) {
	store, _ := testStore(t)
	ctx := t.Context()

	first, err := store.Lock(ctx, "orders_to_es")
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	second, err := store.Lock(ctx, "orders_to_es")
	if err != nil {
		t.Fatalf("expected the lock to be free after release: %v", err)
	}
	if err := second.Release(ctx); err != nil {
		t.Fatalf("release second: %v", err)
	}
}

// MySQL advisory locks belong to a session. If the lock were taken on a pooled
// connection, other queries returning that connection to the pool would drop it
// silently, and two processes could then replicate one stream.
func TestStreamLockSurvivesPoolActivity(t *testing.T) {
	store, db := testStore(t)
	ctx := t.Context()

	held, err := store.Lock(ctx, "orders_to_es")
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	defer held.Release(ctx)

	// Churn the pool well past its connection limit.
	for i := 0; i < 40; i++ {
		var one int
		if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
			t.Fatalf("pool query %d: %v", i, err)
		}
		if err := store.Save(ctx, Checkpoint{Stream: "orders_to_es", SeqWatermark: uint64(i)}); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}

	if _, err := store.Lock(ctx, "orders_to_es"); !errors.Is(err, ErrStreamLocked) {
		t.Fatalf("lock was lost during pool activity: second acquire returned %v", err)
	}
	if !held.Held(ctx) {
		t.Fatal("expected the original lock to still report itself as held")
	}
}

// The allocator's guarantees have to hold against the real store, not only the
// in-memory one, since this is where a lost or truncated watermark would appear.
func TestAllocatorAgainstMySQLStoreNeverRegresses(t *testing.T) {
	store, _ := testStore(t)
	ctx := t.Context()
	clock := fixedClock(testClockMs)

	first, err := NewAllocator(ctx, store, "orders_to_es", 20, clock)
	if err != nil {
		t.Fatalf("new allocator: %v", err)
	}

	var highest uint64
	for i := 0; i < 25; i++ { // crosses a block boundary
		v, err := first.Next(ctx)
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if v <= highest {
			t.Fatalf("value %d did not increase past %d", v, highest)
		}
		highest = v
	}

	// Restart against the same table.
	second, err := NewAllocator(ctx, store, "orders_to_es", 20, clock)
	if err != nil {
		t.Fatalf("new allocator after restart: %v", err)
	}
	next, err := second.Next(ctx)
	if err != nil {
		t.Fatalf("next after restart: %v", err)
	}
	if next <= highest {
		t.Fatalf("restart issued %d, which does not exceed the previous highest %d", next, highest)
	}

	// Wiping the row is the state-loss case: the wall-clock floor, not the
	// watermark, is what keeps versions moving forward.
	wiped, err := NewAllocator(ctx, NewMemoryStore(), "orders_to_es", 20, clock)
	if err != nil {
		t.Fatalf("new allocator on empty store: %v", err)
	}
	afterLoss, err := wiped.Next(ctx)
	if err != nil {
		t.Fatalf("next after state loss: %v", err)
	}
	if afterLoss < uint64(testFloor) {
		t.Fatalf("after losing state, got %d, expected at least the clock floor %d", afterLoss, testFloor)
	}
}

func TestNewMySQLStoreRejectsUnsafeTableNames(t *testing.T) {
	db, err := sql.Open("mysql", "user:pass@tcp(127.0.0.1:1)/x")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for _, name := range []string{
		"",
		"checkpoints; DROP TABLE users",
		"check points",
		"`checkpoints`",
		"meta..checkpoints",
	} {
		if _, err := NewMySQLStore(db, name); err == nil {
			t.Errorf("expected table name %q to be rejected", name)
		}
	}
}

func TestNewMySQLStoreAcceptsQualifiedName(t *testing.T) {
	db, err := sql.Open("mysql", "user:pass@tcp(127.0.0.1:1)/x")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := NewMySQLStore(db, "changeflow_meta.changeflow_checkpoints"); err != nil {
		t.Fatalf("expected a database-qualified table name to be accepted: %v", err)
	}
}
