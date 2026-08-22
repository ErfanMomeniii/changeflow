package schema

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func liveLoader(t *testing.T) DBLoader {
	t.Helper()
	dsn := os.Getenv("CHANGEFLOW_TEST_DSN")
	if dsn == "" {
		t.Skip("set CHANGEFLOW_TEST_DSN to run schema tests against MySQL")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.PingContext(t.Context()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	return DBLoader{DB: db}
}

// The seed table is built from the awkward corners of the type map, so reading it
// exercises what a hand-written fixture would only assume.
func TestLoadOrdersTableFromInformationSchema(t *testing.T) {
	meta, err := liveLoader(t).Load(t.Context(), "shop", "orders")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if meta.Name() != "shop.orders" {
		t.Errorf("name = %q", meta.Name())
	}
	if len(meta.PrimaryKey) != 1 || meta.PrimaryKey[0] != "id" {
		t.Errorf("primary key = %v, want [id]", meta.PrimaryKey)
	}
	first, ok := meta.Column("id")
	if !ok {
		t.Fatal("id column missing")
	}
	if first.Position != 0 {
		t.Errorf("id position = %d, want 0", first.Position)
	}
	for _, tc := range []struct {
		column     string
		wantUnsign bool
		wantNull   bool
		wantKindES string
	}{
		{"id", true, false, "unsigned_long"},
		{"user_id", true, false, "unsigned_long"},
		{"status", false, false, "keyword"},
		{"channels", false, false, "keyword"},
		{"total_amount", false, false, "keyword"},
		{"is_gift", false, false, "boolean"},
		{"note_latin1", false, true, "keyword"},
		{"metadata", false, true, "object"},
		{"placed_at", false, true, "date"},
		{"updated_at", false, false, "date"},
	} {
		t.Run(tc.column, func(t *testing.T) {
			c, ok := meta.Column(tc.column)
			if !ok {
				t.Fatalf("column %s missing", tc.column)
			}
			if c.Unsigned != tc.wantUnsign {
				t.Errorf("unsigned = %v, want %v", c.Unsigned, tc.wantUnsign)
			}
			if c.Nullable != tc.wantNull {
				t.Errorf("nullable = %v, want %v", c.Nullable, tc.wantNull)
			}
			mapped, err := Map(c)
			if err != nil {
				t.Fatalf("map: %v", err)
			}
			if mapped.Elasticsearch != tc.wantKindES {
				t.Errorf("elasticsearch type = %q, want %q", mapped.Elasticsearch, tc.wantKindES)
			}
		})
	}
}

// Labels have to come from the server: the binlog carries only member numbers.
func TestLoadResolvesEnumAndSetLabels(t *testing.T) {
	meta, err := liveLoader(t).Load(t.Context(), "shop", "orders")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	status, _ := meta.Column("status")
	if want := []string{"draft", "paid", "shipped", "cancelled"}; strings.Join(status.EnumValues, ",") != strings.Join(want, ",") {
		t.Errorf("enum labels = %v, want %v", status.EnumValues, want)
	}
	channels, _ := meta.Column("channels")
	if want := []string{"web", "ios", "android", "pos"}; strings.Join(channels.SetValues, ",") != strings.Join(want, ",") {
		t.Errorf("set labels = %v, want %v", channels.SetValues, want)
	}
}

func TestLoadReadsDecimalPrecisionAndDateTimePrecision(t *testing.T) {
	meta, err := liveLoader(t).Load(t.Context(), "shop", "orders")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	total, _ := meta.Column("total_amount")
	if total.NumericPrecision != 18 || total.NumericScale != 2 {
		t.Errorf("decimal precision/scale = %d/%d, want 18/2", total.NumericPrecision, total.NumericScale)
	}
	mapped, err := Map(total)
	if err != nil {
		t.Fatalf("map: %v", err)
	}
	if mapped.ClickHouse != "Decimal(18, 2)" {
		t.Errorf("clickhouse type = %q, want Decimal(18, 2)", mapped.ClickHouse)
	}
	placed, _ := meta.Column("placed_at")
	if placed.DateTimePrecision != 3 {
		t.Errorf("datetime precision = %d, want 3", placed.DateTimePrecision)
	}
}

// A latin1 column has to be identifiable, since its bytes are not UTF-8 and
// converting them needs the charset.
func TestLoadReportsColumnCharset(t *testing.T) {
	meta, err := liveLoader(t).Load(t.Context(), "shop", "orders")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	note, _ := meta.Column("note_latin1")
	if !strings.EqualFold(note.CharacterSet, "latin1") {
		t.Errorf("character set = %q, want latin1", note.CharacterSet)
	}
	status, _ := meta.Column("status")
	if strings.EqualFold(status.CharacterSet, "latin1") {
		t.Errorf("expected the enum column not to be latin1, got %q", status.CharacterSet)
	}
}

func TestLoadCompositePrimaryKeyInOrder(t *testing.T) {
	meta, err := liveLoader(t).Load(t.Context(), "shop", "order_items")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if want := []string{"order_id", "sku"}; strings.Join(meta.PrimaryKey, ",") != strings.Join(want, ",") {
		t.Fatalf("primary key = %v, want %v", meta.PrimaryKey, want)
	}
}

func TestLoadReportsTableWithoutPrimaryKey(t *testing.T) {
	meta, err := liveLoader(t).Load(t.Context(), "shop", "audit_log")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(meta.PrimaryKey) != 0 {
		t.Fatalf("expected no primary key, got %v", meta.PrimaryKey)
	}
	if _, err := meta.ResolveKey(nil); err == nil {
		t.Fatal("expected resolving a key to fail for a table with none")
	}
}

func TestLoadNamesMissingTable(t *testing.T) {
	_, err := liveLoader(t).Load(t.Context(), "shop", "no_such_table")
	if err == nil {
		t.Fatal("expected an error for a table that does not exist")
	}
	if !strings.Contains(err.Error(), "no_such_table") {
		t.Fatalf("error should name the table, got: %v", err)
	}
}

func TestStoreAgainstLiveLoader(t *testing.T) {
	store := NewStore(liveLoader(t))
	first, err := store.Table(t.Context(), "shop", "orders")
	if err != nil {
		t.Fatalf("table: %v", err)
	}
	second, err := store.Table(t.Context(), "shop", "orders")
	if err != nil {
		t.Fatalf("table: %v", err)
	}
	if first != second {
		t.Fatal("expected the cached definition to be the same instance")
	}
}
