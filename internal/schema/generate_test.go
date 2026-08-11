package schema

import (
	"encoding/json"
	"strings"
	"testing"
)

// generatorMeta covers the columns whose destination type is easy to get wrong by
// hand, which is the reason this generator exists.
func generatorMeta() *TableMeta {
	m := &TableMeta{
		Schema: "shop",
		Table:  "orders",
		Columns: []Column{
			{Name: "id", Position: 0, DataType: "bigint", ColumnType: "bigint unsigned", Unsigned: true},
			{Name: "user_id", Position: 1, DataType: "bigint", ColumnType: "bigint unsigned", Unsigned: true},
			{Name: "status", Position: 2, DataType: "enum", ColumnType: "enum('draft','paid')",
				EnumValues: []string{"draft", "paid"}},
			{Name: "channels", Position: 3, DataType: "set", ColumnType: "set('web','ios')",
				SetValues: []string{"web", "ios"}},
			{Name: "total_amount", Position: 4, DataType: "decimal", ColumnType: "decimal(18,2)",
				NumericPrecision: 18, NumericScale: 2},
			{Name: "is_gift", Position: 5, DataType: "tinyint", ColumnType: "tinyint(1)"},
			{Name: "note", Position: 6, DataType: "varchar", ColumnType: "varchar(64)", Nullable: true},
			{Name: "metadata", Position: 7, DataType: "json", ColumnType: "json", Nullable: true},
			{Name: "placed_at", Position: 8, DataType: "datetime", ColumnType: "datetime(3)",
				Nullable: true, DateTimePrecision: 3},
			{Name: "updated_at", Position: 9, DataType: "timestamp", ColumnType: "timestamp(3)",
				DateTimePrecision: 3},
			{Name: "internal_note", Position: 10, DataType: "text", ColumnType: "text", Nullable: true},
			{Name: "total_with_tax", Position: 11, DataType: "decimal", ColumnType: "decimal(18,2)",
				Generated: true, NumericPrecision: 18, NumericScale: 2},
		},
		PrimaryKey: []string{"id"},
	}
	m.index()
	return m
}

func generateES(t *testing.T, include, exclude []string, rename map[string]string) Generated {
	t.Helper()
	got, err := GenerateElasticsearch(generatorMeta(), include, exclude, []string{"id"}, rename, 1, 1)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	return got
}

// property reads a field's declared type out of the generated mapping, which also
// proves the output is valid JSON.
func property(t *testing.T, body, field string) map[string]any {
	t.Helper()

	var doc struct {
		Mappings struct {
			Dynamic    string                    `json:"dynamic"`
			Properties map[string]map[string]any `json:"properties"`
		} `json:"mappings"`
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("generated mapping is not valid JSON: %v\n%s", err, body)
	}
	return doc.Mappings.Properties[field]
}

func TestGeneratedMappingIsValidJSON(t *testing.T) {
	got := generateES(t, nil, nil, nil)

	var parsed map[string]any
	if err := json.Unmarshal([]byte(got.Body), &parsed); err != nil {
		t.Fatalf("not valid JSON: %v\n%s", err, got.Body)
	}
}

// The mapping is generated from the same type map replication uses, so the awkward
// cases cannot drift apart.
func TestGeneratedMappingUsesTheTypeMap(t *testing.T) {
	got := generateES(t, nil, nil, nil)

	for _, tc := range []struct {
		field string
		want  string
	}{
		// A signed long would wrap silently above 2^63.
		{"id", "unsigned_long"},
		{"user_id", "unsigned_long"},
		// Exact decimals travel as text, since Elasticsearch has no decimal type.
		{"total_amount", "keyword"},
		{"status", "keyword"},
		{"channels", "keyword"},
		{"is_gift", "boolean"},
		{"note", "keyword"},
		{"placed_at", "date"},
		{"updated_at", "date"},
		{"internal_note", "text"},
	} {
		t.Run(tc.field, func(t *testing.T) {
			p := property(t, got.Body, tc.field)
			if p == nil {
				t.Fatalf("field %s is missing from the mapping", tc.field)
			}
			if p["type"] != tc.want {
				t.Errorf("%s is %v, want %s", tc.field, p["type"], tc.want)
			}
		})
	}
}

// An unmapped field must fail loudly rather than being indexed with a guessed type,
// which is how a column added to the source ends up as the wrong type forever.
func TestGeneratedMappingDisablesDynamicFields(t *testing.T) {
	got := generateES(t, nil, nil, nil)

	var doc struct {
		Mappings struct {
			Dynamic string `json:"dynamic"`
		} `json:"mappings"`
	}
	if err := json.Unmarshal([]byte(got.Body), &doc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if doc.Mappings.Dynamic != "strict" {
		t.Errorf("dynamic = %q, want strict", doc.Mappings.Dynamic)
	}
}

// A JSON column's shape is the application's business, and indexing it invites a
// mapping explosion.
func TestJSONColumnIsStoredButNotIndexed(t *testing.T) {
	got := generateES(t, nil, nil, nil)

	p := property(t, got.Body, "metadata")
	if p["type"] != "object" {
		t.Fatalf("metadata type = %v, want object", p["type"])
	}
	if enabled, ok := p["enabled"].(bool); !ok || enabled {
		t.Errorf("metadata should not be indexed, got enabled=%v", p["enabled"])
	}
}

func TestGeneratedMappingHonoursIncludeExcludeAndRename(t *testing.T) {
	got := generateES(t, []string{"id", "total_amount"}, nil, map[string]string{"total_amount": "total"})

	if property(t, got.Body, "total") == nil {
		t.Error("renamed field is missing")
	}
	if property(t, got.Body, "total_amount") != nil {
		t.Error("the source name should not appear once renamed")
	}
	if property(t, got.Body, "status") != nil {
		t.Error("a field outside the include list should not appear")
	}

	excluded := generateES(t, nil, []string{"internal_note"}, nil)
	if property(t, excluded.Body, "internal_note") != nil {
		t.Error("an excluded field should not appear")
	}
}

// A generated column never appears in a binlog row image, so replication never sends
// it and the mapping must not claim it exists.
func TestGeneratedColumnIsNotInTheMapping(t *testing.T) {
	got := generateES(t, nil, nil, nil)

	if property(t, got.Body, "total_with_tax") != nil {
		t.Error("a generated column must not appear in the destination mapping")
	}
}

// A regenerated file should produce a reviewable diff, which requires stable output.
func TestGeneratedMappingIsDeterministic(t *testing.T) {
	first := generateES(t, nil, nil, nil)
	for i := 0; i < 5; i++ {
		if again := generateES(t, nil, nil, nil); again.Body != first.Body {
			t.Fatal("output varies between runs, so a regenerated file would produce a noisy diff")
		}
	}
}

func TestGeneratedMappingRefusesUnsupportedColumns(t *testing.T) {
	meta := generatorMeta()
	meta.Columns = append(meta.Columns, Column{Name: "shape", Position: 12, DataType: "geometry", ColumnType: "geometry"})
	meta.index()

	_, err := GenerateElasticsearch(meta, nil, nil, []string{"id"}, nil, 1, 1)
	if err == nil {
		t.Fatal("expected a spatial column to be refused")
	}
	if !strings.Contains(err.Error(), "shape") {
		t.Errorf("error should name the column, got: %v", err)
	}
}

func generateCH(t *testing.T, rename map[string]string) Generated {
	t.Helper()
	got, err := GenerateClickHouse(generatorMeta(), nil, nil, []string{"id"}, rename, "analytics.orders")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	return got
}

// collapseSpaces makes assertions independent of the column alignment, which exists
// to make generated DDL readable in review rather than to be asserted on.
func collapseSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func TestGeneratedClickHouseTableUsesTheReplacingEngine(t *testing.T) {
	got := generateCH(t, nil)
	flat := collapseSpaces(got.Body)

	// The engine parameters are load bearing: without them duplicate versions are
	// never collapsed and deletes never take effect.
	if !strings.Contains(got.Body, "ENGINE = ReplacingMergeTree(_version, _is_deleted)") {
		t.Errorf("engine line is wrong:\n%s", got.Body)
	}
	if !strings.Contains(got.Body, "ORDER BY (`id`)") {
		t.Errorf("sort key is wrong:\n%s", got.Body)
	}
	for _, want := range []string{"`_version` UInt64", "`_is_deleted` UInt8"} {
		if !strings.Contains(flat, want) {
			t.Errorf("missing replication column %q:\n%s", want, got.Body)
		}
	}
}

func TestGeneratedClickHouseTypes(t *testing.T) {
	got := generateCH(t, nil)
	flat := collapseSpaces(got.Body)

	for _, want := range []string{
		"`id` UInt64",
		"`total_amount` Decimal(18, 2)",
		"`status` LowCardinality(String)",
		"`channels` Array(String)",
		"`is_gift` Bool",
		// A nullable column is wrapped, and LowCardinality sits outside Nullable, which
		// is the order ClickHouse accepts.
		"`note` Nullable(String)",
		"`placed_at` Nullable(DateTime64(3))",
		// A TIMESTAMP is an instant, so its zone is fixed rather than left to the
		// reader's session.
		"`updated_at` DateTime64(3, 'UTC')",
	} {
		if !strings.Contains(flat, want) {
			t.Errorf("expected %q in:\n%s", want, got.Body)
		}
	}
}

// Documents carry destination names, so the sort key must use them too or the engine
// would order on a column that does not exist.
func TestGeneratedClickHouseSortKeyUsesDestinationNames(t *testing.T) {
	got, err := GenerateClickHouse(generatorMeta(), []string{"id", "status"}, nil, []string{"id"},
		map[string]string{"id": "order_id"}, "analytics.orders")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	if !strings.Contains(got.Body, "ORDER BY (`order_id`)") {
		t.Errorf("sort key should use the renamed column:\n%s", got.Body)
	}
}

// The reserved columns cannot be shadowed by a mapped field, or the engine would
// deduplicate on row data.
func TestRenameOntoAReservedColumnIsRefused(t *testing.T) {
	_, err := GenerateClickHouse(generatorMeta(), nil, nil, []string{"id"},
		map[string]string{"total_amount": VersionColumn}, "analytics.orders")
	if err == nil {
		t.Fatal("expected a rename onto _version to be refused")
	}

	if _, err := GenerateElasticsearch(generatorMeta(), nil, nil, []string{"id"},
		map[string]string{"total_amount": DeletedColumn}, 1, 1); err == nil {
		t.Fatal("expected a rename onto _is_deleted to be refused")
	}
}

func TestGeneratedClickHouseRequiresATableName(t *testing.T) {
	if _, err := GenerateClickHouse(generatorMeta(), nil, nil, []string{"id"}, nil, ""); err == nil {
		t.Fatal("expected a missing table name to be refused")
	}
}

// The warnings are the part an operator has to act on, so they must actually be
// produced.
func TestGeneratorWarnsAboutWhatNeedsAttention(t *testing.T) {
	es := generateES(t, nil, nil, nil)
	joined := strings.Join(es.Warnings, "\n")
	// Nullability costs something in ClickHouse and little in Elasticsearch, so the
	// warning belongs to the generator it applies to.
	if strings.Contains(strings.ToLower(joined), "null mask") {
		t.Errorf("an Elasticsearch mapping should not warn about ClickHouse null masks:\n%s", joined)
	}
	if !strings.Contains(joined, "alias") {
		t.Errorf("expected the rebuild procedure to be mentioned:\n%s", joined)
	}

	ch := generateCH(t, nil)
	joined = strings.Join(ch.Warnings, "\n")
	if !strings.Contains(joined, "Nullable") {
		t.Errorf("expected a warning about nullable columns:\n%s", joined)
	}
	if !strings.Contains(joined, "FINAL") {
		t.Errorf("expected a warning that readers need FINAL:\n%s", joined)
	}
	if !strings.Contains(joined, minClickHouseCH) {
		t.Errorf("expected the minimum ClickHouse version to be stated:\n%s", joined)
	}
}
