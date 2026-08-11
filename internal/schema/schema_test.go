package schema

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func ordersMeta() *TableMeta {
	m := &TableMeta{
		Schema: "shop",
		Table:  "orders",
		Columns: []Column{
			{Name: "id", Position: 0, DataType: "bigint", ColumnType: "bigint unsigned", Unsigned: true},
			{Name: "user_id", Position: 1, DataType: "bigint", ColumnType: "bigint unsigned", Unsigned: true},
			{Name: "status", Position: 2, DataType: "enum", ColumnType: "enum('draft','paid')", EnumValues: []string{"draft", "paid"}},
			{Name: "note", Position: 3, DataType: "varchar", ColumnType: "varchar(64)", Nullable: true},
			{Name: "total_with_tax", Position: 4, DataType: "decimal", ColumnType: "decimal(18,2)", Generated: true},
		},
		PrimaryKey: []string{"id"},
	}
	m.index()
	return m
}

func names(cols []Column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}

func TestColumnLookupIgnoresCase(t *testing.T) {
	m := ordersMeta()

	if _, ok := m.Column("STATUS"); !ok {
		t.Error("expected a case-insensitive lookup to succeed")
	}
	if _, ok := m.Column("nonexistent"); ok {
		t.Error("expected a missing column to report absent")
	}
}

func TestPrimaryKeyPositions(t *testing.T) {
	got, err := ordersMeta().PrimaryKeyPositions()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("got %v, want [0]", got)
	}
}

// A table with no primary key cannot be replicated idempotently, and the message
// has to say so rather than failing later on a duplicate.
func TestPrimaryKeyPositionsExplainsMissingKey(t *testing.T) {
	m := &TableMeta{Schema: "shop", Table: "audit_log", Columns: []Column{{Name: "actor"}}}

	_, err := m.PrimaryKeyPositions()
	if err == nil {
		t.Fatal("expected an error for a table with no primary key")
	}
	if !strings.Contains(err.Error(), "idempotent") {
		t.Fatalf("error should explain the consequence, got: %v", err)
	}
}

func TestResolveKeyFallsBackToPrimaryKey(t *testing.T) {
	got, err := ordersMeta().ResolveKey(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || got[0] != "id" {
		t.Fatalf("got %v, want [id]", got)
	}
}

func TestResolveKeyRejectsUnusableKeys(t *testing.T) {
	m := ordersMeta()

	for _, tc := range []struct {
		name string
		key  []string
		why  string
	}{
		{"missing column", []string{"nope"}, "does not exist"},
		{"duplicate column", []string{"id", "id"}, "twice"},
		// A generated column is absent from row images, so it can never key a row.
		{"generated column", []string{"total_with_tax"}, "generated"},
		// A null cannot identify anything.
		{"nullable column", []string{"note"}, "null"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := m.ResolveKey(tc.key)
			if err == nil {
				t.Fatalf("expected key %v to be rejected", tc.key)
			}
			if !strings.Contains(err.Error(), tc.why) {
				t.Fatalf("error should mention %q, got: %v", tc.why, err)
			}
		})
	}
}

func TestResolveKeyAcceptsCompositeKey(t *testing.T) {
	m := &TableMeta{
		Schema: "shop", Table: "order_items",
		Columns: []Column{
			{Name: "order_id", Position: 0, DataType: "bigint", ColumnType: "bigint unsigned", Unsigned: true},
			{Name: "sku", Position: 1, DataType: "varchar", ColumnType: "varchar(64)"},
		},
		PrimaryKey: []string{"order_id", "sku"},
	}
	m.index()

	got, err := m.ResolveKey(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 || got[0] != "order_id" || got[1] != "sku" {
		t.Fatalf("got %v, want [order_id sku]", got)
	}
}

// Generated columns are dropped by default, since a row image never carries them
// and including one would write a null over real data.
func TestSelectColumnsSkipsGeneratedByDefault(t *testing.T) {
	got, err := ordersMeta().SelectColumns(nil, nil, []string{"id"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, c := range got {
		if c.Name == "total_with_tax" {
			t.Fatal("a generated column must not be selected by default")
		}
	}
	if len(got) != 4 {
		t.Fatalf("got %v, want the four real columns", names(got))
	}
}

func TestSelectColumnsHonoursInclude(t *testing.T) {
	got, err := ordersMeta().SelectColumns([]string{"id", "status"}, nil, []string{"id"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := []string{"id", "status"}; len(got) != 2 || got[0].Name != want[0] || got[1].Name != want[1] {
		t.Fatalf("got %v, want %v", names(got), want)
	}
}

// Selected columns stay in ordinal order regardless of how the include list is
// written, because a row image is positional.
func TestSelectColumnsKeepsOrdinalOrder(t *testing.T) {
	got, err := ordersMeta().SelectColumns([]string{"status", "id", "user_id"}, nil, []string{"id"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := []string{"id", "user_id", "status"}; len(got) != 3 ||
		got[0].Name != want[0] || got[1].Name != want[1] || got[2].Name != want[2] {
		t.Fatalf("got %v, want %v", names(got), want)
	}
}

func TestSelectColumnsHonoursExclude(t *testing.T) {
	got, err := ordersMeta().SelectColumns(nil, []string{"note"}, []string{"id"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, c := range got {
		if c.Name == "note" {
			t.Fatal("excluded column was selected")
		}
	}
}

// A destination row cannot be identified without its key, so dropping a key
// column is a configuration error rather than a preference.
func TestSelectColumnsRefusesToDropKeyColumns(t *testing.T) {
	m := ordersMeta()

	if _, err := m.SelectColumns([]string{"status", "note"}, nil, []string{"id"}); err == nil {
		t.Error("expected include without the key column to be rejected")
	}
	if _, err := m.SelectColumns(nil, []string{"id"}, []string{"id"}); err == nil {
		t.Error("expected exclude of the key column to be rejected")
	}
}

func TestSelectColumnsRejectsUnknownNames(t *testing.T) {
	m := ordersMeta()

	if _, err := m.SelectColumns([]string{"id", "nope"}, nil, []string{"id"}); err == nil {
		t.Error("expected an unknown include column to be rejected")
	}
	if _, err := m.SelectColumns(nil, []string{"nope"}, []string{"id"}); err == nil {
		t.Error("expected an unknown exclude column to be rejected")
	}
}

func TestSelectColumnsRejectsIncludeAndExcludeTogether(t *testing.T) {
	if _, err := ordersMeta().SelectColumns([]string{"id"}, []string{"note"}, []string{"id"}); err == nil {
		t.Fatal("expected include and exclude together to be rejected")
	}
}

func TestParseMemberList(t *testing.T) {
	for _, tc := range []struct {
		name       string
		columnType string
		keyword    string
		want       []string
	}{
		{"simple enum", "enum('draft','paid','shipped')", "enum", []string{"draft", "paid", "shipped"}},
		{"simple set", "set('web','ios')", "set", []string{"web", "ios"}},
		// A label may contain a comma, so splitting on commas would be wrong.
		{"label with comma", "enum('a,b','c')", "enum", []string{"a,b", "c"}},
		// MySQL doubles an embedded quote.
		{"label with quote", "enum('it''s','other')", "enum", []string{"it's", "other"}},
		{"label with parenthesis", "enum('a(1)','b')", "enum", []string{"a(1)", "b"}},
		{"empty label", "enum('','x')", "enum", []string{"", "x"}},
		{"not a member list", "varchar(64)", "enum", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := parseMemberList(tc.columnType, tc.keyword)
			if len(got) != len(tc.want) {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("got %q, want %q", got, tc.want)
				}
			}
		})
	}
}

// fakeLoader counts loads so caching can be asserted.
type fakeLoader struct {
	loads int
	meta  *TableMeta
	err   error
}

func (f *fakeLoader) Load(context.Context, string, string) (*TableMeta, error) {
	f.loads++
	if f.err != nil {
		return nil, f.err
	}
	return f.meta, nil
}

// A definition is needed for every row event, so loading it per event would put a
// query on the source for every change.
func TestStoreCachesDefinitions(t *testing.T) {
	loader := &fakeLoader{meta: ordersMeta()}
	store := NewStore(loader)

	for i := 0; i < 5; i++ {
		if _, err := store.Table(t.Context(), "shop", "orders"); err != nil {
			t.Fatalf("table: %v", err)
		}
	}

	if loader.loads != 1 {
		t.Fatalf("expected 1 load for 5 lookups, got %d", loader.loads)
	}
}

func TestStoreLookupIgnoresCase(t *testing.T) {
	loader := &fakeLoader{meta: ordersMeta()}
	store := NewStore(loader)

	if _, err := store.Table(t.Context(), "shop", "orders"); err != nil {
		t.Fatalf("table: %v", err)
	}
	if _, err := store.Table(t.Context(), "SHOP", "ORDERS"); err != nil {
		t.Fatalf("table: %v", err)
	}

	if loader.loads != 1 {
		t.Fatalf("expected the cache to be case-insensitive, got %d loads", loader.loads)
	}
}

// DDL changes a table's shape, so a stale definition would decode later rows
// against the wrong columns.
func TestStoreInvalidateForcesReload(t *testing.T) {
	loader := &fakeLoader{meta: ordersMeta()}
	store := NewStore(loader)

	if _, err := store.Table(t.Context(), "shop", "orders"); err != nil {
		t.Fatalf("table: %v", err)
	}
	store.Invalidate("shop", "orders")
	if _, err := store.Table(t.Context(), "shop", "orders"); err != nil {
		t.Fatalf("table: %v", err)
	}

	if loader.loads != 2 {
		t.Fatalf("expected a reload after invalidation, got %d loads", loader.loads)
	}
	if store.Cached() != 1 {
		t.Fatalf("expected one cached definition, got %d", store.Cached())
	}
}

func TestStoreInvalidateAllClearsCache(t *testing.T) {
	loader := &fakeLoader{meta: ordersMeta()}
	store := NewStore(loader)

	if _, err := store.Table(t.Context(), "shop", "orders"); err != nil {
		t.Fatalf("table: %v", err)
	}
	store.InvalidateAll()

	if store.Cached() != 0 {
		t.Fatalf("expected an empty cache, got %d entries", store.Cached())
	}
}

// A failed load must not be cached, or a transient error would persist until
// restart.
func TestStoreDoesNotCacheFailures(t *testing.T) {
	loader := &fakeLoader{err: errors.New("connection reset")}
	store := NewStore(loader)

	if _, err := store.Table(t.Context(), "shop", "orders"); err == nil {
		t.Fatal("expected the load error to surface")
	}
	if store.Cached() != 0 {
		t.Fatal("a failed load must not be cached")
	}

	loader.err = nil
	loader.meta = ordersMeta()
	if _, err := store.Table(t.Context(), "shop", "orders"); err != nil {
		t.Fatalf("expected a retry to succeed: %v", err)
	}
}
