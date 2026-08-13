package checkpoint

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

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
	// Contention is the behaviour under test, so do not wait for it to clear.
	store.LockTimeout = 0
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

// The driver returns a TIMESTAMP as time.Time or as raw bytes depending on the
// DSN's parseTime parameter. Depending on one spelling made every checkpoint load
// fail against a connection string written the obvious way.
func TestMySQLStoreReadsTimestampsWithoutParseTime(t *testing.T) {
	dsn := os.Getenv("CHANGEFLOW_TEST_META_DSN")
	if dsn == "" {
		t.Skip("set CHANGEFLOW_TEST_META_DSN to run checkpoint store tests against MySQL")
	}
	// Strip the parameter if the environment happened to supply it.
	if i := strings.IndexByte(dsn, '?'); i >= 0 {
		dsn = dsn[:i]
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	table := "cp_test_" + sanitizeForTable(t.Name())
	store, err := NewMySQLStore(db, table)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.EnsureSchema(t.Context()); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS " + table) })

	if err := store.Save(t.Context(), Checkpoint{Stream: "s", GTIDSet: "uuid:1-5"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	cp, err := store.Load(t.Context(), "s")
	if err != nil {
		t.Fatalf("load without parseTime: %v", err)
	}
	if cp.UpdatedAt.IsZero() {
		t.Error("updated_at was not parsed from the driver's byte form")
	}
	if cp.GTIDSet != "uuid:1-5" {
		t.Errorf("gtid = %q", cp.GTIDSet)
	}
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

// A restart moments after an abrupt kill must be able to take the lock once the
// server reaps the dead session, rather than refusing to start.
func TestStreamLockWaitsBrieflyForAContendedLock(t *testing.T) {
	store, _ := testStore(t)
	store.LockTimeout = 2 * time.Second
	ctx := t.Context()

	held, err := store.Lock(ctx, "orders_to_es")
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}

	// Release from another goroutine while the second acquire is waiting.
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = held.Release(context.Background())
	}()

	start := time.Now()
	second, err := store.Lock(ctx, "orders_to_es")
	if err != nil {
		t.Fatalf("expected the wait to outlast the holder: %v", err)
	}
	defer second.Release(ctx)

	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Errorf("acquired in %v, which suggests it did not actually wait", elapsed)
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

// A stopped stream's reason belongs where an operator looks for it, and it has to survive
// the process that failed.
func TestRecordErrorKeepsTheReasonAStreamStopped(t *testing.T) {
	store, _ := testStore(t)

	if err := store.Save(t.Context(), Checkpoint{Stream: "orders", GTIDSet: "uuid:1-5"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := store.RecordError(t.Context(), "orders", "sink refused the batch"); err != nil {
		t.Fatalf("record: %v", err)
	}

	cp, err := store.Load(t.Context(), "orders")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cp.LastError != "sink refused the batch" {
		t.Errorf("last error = %q, want the recorded reason", cp.LastError)
	}
	// Recording a failure must not disturb the position, or a restart would replay from
	// somewhere other than where the stream got to.
	if cp.GTIDSet != "uuid:1-5" {
		t.Errorf("position = %q, want it untouched", cp.GTIDSet)
	}
}

func TestRecordErrorClearsAResolvedFailure(t *testing.T) {
	store, _ := testStore(t)

	if err := store.Save(t.Context(), Checkpoint{Stream: "orders", LastError: "old news"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := store.RecordError(t.Context(), "orders", ""); err != nil {
		t.Fatalf("clear: %v", err)
	}

	cp, err := store.Load(t.Context(), "orders")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cp.LastError != "" {
		t.Errorf("last error = %q, want it cleared once the stream is running again", cp.LastError)
	}
}

// A driver error can carry a whole request body, which must not fill the column and fail
// the write that is trying to explain a failure.
func TestRecordErrorTruncatesAnOverlongReason(t *testing.T) {
	store, _ := testStore(t)

	if err := store.Save(t.Context(), Checkpoint{Stream: "orders"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := store.RecordError(t.Context(), "orders", strings.Repeat("x", 5000)); err != nil {
		t.Fatalf("record: %v", err)
	}

	cp, err := store.Load(t.Context(), "orders")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cp.LastError) > maxRecordedErrorLen+4 {
		t.Errorf("stored %d bytes, want it bounded near %d", len(cp.LastError), maxRecordedErrorLen)
	}
	if !strings.HasSuffix(cp.LastError, "…") {
		t.Error("a truncated reason should say that it was truncated")
	}
}

// Nothing has failed about a stream that has never run, so there is no row to update and
// that is not an error.
func TestRecordErrorOnAnUnknownStreamIsNotAnError(t *testing.T) {
	store, _ := testStore(t)

	if err := store.RecordError(t.Context(), "never_ran", "something"); err != nil {
		t.Errorf("record: %v", err)
	}
}

// A running service should hold no DDL rights, and MySQL refuses CREATE TABLE IF NOT
// EXISTS without CREATE even when the table already exists. A stream that cannot start
// under the grants it is documented to need would be found in production, not here.
func TestEnsureSchemaWorksWithDMLRightsOnly(t *testing.T) {
	adminDSN := os.Getenv("CHANGEFLOW_TEST_WRITE_DSN")
	if adminDSN == "" {
		t.Skip("set CHANGEFLOW_TEST_WRITE_DSN to a user that may grant, to run this")
	}
	store, _ := testStore(t)

	admin, err := sql.Open("mysql", adminDSN)
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}
	defer admin.Close()

	// A user with exactly the rights the README asks for, and nothing more.
	const user = "cf_dml_only"
	for _, statement := range []string{
		"DROP USER IF EXISTS '" + user + "'@'%'",
		"CREATE USER '" + user + "'@'%' IDENTIFIED BY 'secret'",
		"GRANT SELECT, INSERT, UPDATE, DELETE ON `changeflow_meta`.* TO '" + user + "'@'%'",
	} {
		if _, err := admin.ExecContext(t.Context(), statement); err != nil {
			t.Skipf("cannot prepare a restricted user (%v); this needs a user that may grant", err)
		}
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP USER IF EXISTS '" + user + "'@'%'")
	})

	restricted, err := sql.Open("mysql", "cf_dml_only:secret@tcp("+hostOf(adminDSN)+")/changeflow_meta")
	if err != nil {
		t.Fatalf("open restricted: %v", err)
	}
	defer restricted.Close()

	limited, err := NewMySQLStore(restricted, store.table)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	// The table exists, applied by whoever holds DDL rights.
	if err := limited.EnsureSchema(t.Context()); err != nil {
		t.Fatalf("a service with DML rights only could not start: %v", err)
	}
	// And it can do the work it exists to do.
	if err := limited.Save(t.Context(), Checkpoint{Stream: "orders", GTIDSet: "uuid:1-9"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if cp, err := limited.Load(t.Context(), "orders"); err != nil || cp.GTIDSet != "uuid:1-9" {
		t.Fatalf("load = %+v, %v", cp, err)
	}
}

// hostOf pulls the address out of a DSN, so a restricted user can be pointed at the same
// server without a second variable to keep in step.
func hostOf(dsn string) string {
	start := strings.Index(dsn, "tcp(")
	if start < 0 {
		return "127.0.0.1:3306"
	}
	rest := dsn[start+len("tcp("):]
	end := strings.Index(rest, ")")
	if end < 0 {
		return "127.0.0.1:3306"
	}
	return rest[:end]
}
