package schema

import (
	"strings"
	"testing"
)

func col(name, dataType, columnType string, opts ...func(*Column)) Column {
	c := Column{
		Name:       name,
		DataType:   dataType,
		ColumnType: columnType,
		Unsigned:   strings.Contains(columnType, "unsigned"),
	}
	for _, o := range opts {
		o(&c)
	}
	return c
}

func nullable(c *Column)  { c.Nullable = true }
func generated(c *Column) { c.Generated = true }

func decimalCol(precision, scale int) Column {
	c := col("total_amount", "decimal", "decimal(18,2)")
	c.NumericPrecision, c.NumericScale = precision, scale
	return c
}

func TestMapIntegerTypes(t *testing.T) {
	for _, tc := range []struct {
		dataType   string
		columnType string
		wantKind   Kind
		wantES     string
		wantCH     string
	}{
		{"tinyint", "tinyint(1)", KindBool, "boolean", "Bool"},
		{"tinyint", "tinyint(4)", KindInt, "byte", "Int8"},
		{"tinyint", "tinyint unsigned", KindUint, "short", "UInt8"},
		{"smallint", "smallint", KindInt, "short", "Int16"},
		{"smallint", "smallint unsigned", KindUint, "integer", "UInt16"},
		{"mediumint", "mediumint", KindInt, "integer", "Int32"},
		{"int", "int", KindInt, "integer", "Int32"},
		{"int", "int unsigned", KindUint, "long", "UInt32"},
		{"bigint", "bigint", KindInt, "long", "Int64"},
		{"bigint", "bigint unsigned", KindUint, "unsigned_long", "UInt64"},
	} {
		t.Run(tc.columnType, func(t *testing.T) {
			got, err := Map(col("c", tc.dataType, tc.columnType))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Kind != tc.wantKind {
				t.Errorf("kind = %v, want %v", got.Kind, tc.wantKind)
			}
			if got.Elasticsearch != tc.wantES {
				t.Errorf("elasticsearch = %q, want %q", got.Elasticsearch, tc.wantES)
			}
			if got.ClickHouse != tc.wantCH {
				t.Errorf("clickhouse = %q, want %q", got.ClickHouse, tc.wantCH)
			}
		})
	}
}

// A decimal must never travel as a float. Elasticsearch has no exact decimal
// type, so the value is kept as a keyword and stays byte-identical.
func TestMapDecimalPreservesExactness(t *testing.T) {
	got, err := Map(decimalCol(18, 2))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Kind != KindDecimal {
		t.Errorf("kind = %v, want KindDecimal", got.Kind)
	}
	if got.Elasticsearch != "keyword" {
		t.Errorf("elasticsearch = %q, want keyword so the value is not rounded", got.Elasticsearch)
	}
	if got.ClickHouse != "Decimal(18, 2)" {
		t.Errorf("clickhouse = %q, want Decimal(18, 2)", got.ClickHouse)
	}
}

func TestMapDecimalCarriesPrecisionThrough(t *testing.T) {
	got, err := Map(decimalCol(38, 10))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ClickHouse != "Decimal(38, 10)" {
		t.Fatalf("clickhouse = %q, want Decimal(38, 10)", got.ClickHouse)
	}
}

// DATETIME is wall-clock with no zone; TIMESTAMP is an instant stored as UTC.
// Treating them alike shifts every value by the server's offset.
func TestMapDistinguishesDatetimeFromTimestamp(t *testing.T) {
	dt := col("placed_at", "datetime", "datetime(3)")
	dt.DateTimePrecision = 3
	ts := col("updated_at", "timestamp", "timestamp(3)")
	ts.DateTimePrecision = 3
	gotDT, err := Map(dt)
	if err != nil {
		t.Fatalf("datetime: %v", err)
	}
	gotTS, err := Map(ts)
	if err != nil {
		t.Fatalf("timestamp: %v", err)
	}
	if gotDT.Kind == gotTS.Kind {
		t.Errorf("datetime and timestamp must map to different kinds, both were %v", gotDT.Kind)
	}
	if gotDT.ClickHouse != "DateTime64(3)" {
		t.Errorf("datetime clickhouse = %q, want DateTime64(3)", gotDT.ClickHouse)
	}
	if gotTS.ClickHouse != "DateTime64(3, 'UTC')" {
		t.Errorf("timestamp clickhouse = %q, want DateTime64(3, 'UTC')", gotTS.ClickHouse)
	}
}

func TestMapDateUsesDate32ToCoverPre1970(t *testing.T) {
	got, err := Map(col("d", "date", "date"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ClickHouse != "Date32" {
		t.Fatalf("clickhouse = %q, want Date32", got.ClickHouse)
	}
}

func TestMapEnumAndSet(t *testing.T) {
	enum := col("status", "enum", "enum('draft','paid','shipped')")
	enum.EnumValues = []string{"draft", "paid", "shipped"}
	set := col("channels", "set", "set('web','ios')")
	set.SetValues = []string{"web", "ios"}
	gotEnum, err := Map(enum)
	if err != nil {
		t.Fatalf("enum: %v", err)
	}
	if gotEnum.Kind != KindEnum || gotEnum.Elasticsearch != "keyword" {
		t.Errorf("enum mapped to %v/%q", gotEnum.Kind, gotEnum.Elasticsearch)
	}
	if gotEnum.ClickHouse != "LowCardinality(String)" {
		t.Errorf("enum clickhouse = %q, want LowCardinality(String)", gotEnum.ClickHouse)
	}
	gotSet, err := Map(set)
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if gotSet.Kind != KindSet {
		t.Errorf("set kind = %v, want KindSet", gotSet.Kind)
	}
	if gotSet.ClickHouse != "Array(String)" {
		t.Errorf("set clickhouse = %q, want Array(String)", gotSet.ClickHouse)
	}
}

// An ENUM with no member list means the server did not supply labels, and the
// binlog carries only integers. Emitting those as if they were labels would write
// wrong data.
func TestMapEnumWithoutLabelsIsRefused(t *testing.T) {
	if _, err := Map(col("status", "enum", "enum('a','b')")); err == nil {
		t.Fatal("expected an enum with no member list to be refused")
	}
}

func TestMapStringAndTextTypes(t *testing.T) {
	for _, tc := range []struct {
		dataType, columnType, wantES, wantCH string
		wantKind                             Kind
	}{
		{"varchar", "varchar(64)", "keyword", "String", KindString},
		{"char", "char(2)", "keyword", "String", KindString},
		{"text", "text", "text", "String", KindString},
		{"mediumtext", "mediumtext", "text", "String", KindString},
		{"longtext", "longtext", "text", "String", KindString},
		{"varbinary", "varbinary(255)", "binary", "String", KindBytes},
		{"blob", "blob", "binary", "String", KindBytes},
		{"json", "json", "object", "String", KindJSON},
		{"bit", "bit(8)", "long", "UInt64", KindBit},
		{"year", "year", "short", "UInt16", KindYear},
		{"time", "time", "long", "Int64", KindTime},
		{"float", "float", "float", "Float32", KindFloat},
		{"double", "double", "double", "Float64", KindFloat},
	} {
		t.Run(tc.columnType, func(t *testing.T) {
			got, err := Map(col("c", tc.dataType, tc.columnType))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Kind != tc.wantKind {
				t.Errorf("kind = %v, want %v", got.Kind, tc.wantKind)
			}
			if got.Elasticsearch != tc.wantES {
				t.Errorf("elasticsearch = %q, want %q", got.Elasticsearch, tc.wantES)
			}
			if got.ClickHouse != tc.wantCH {
				t.Errorf("clickhouse = %q, want %q", got.ClickHouse, tc.wantCH)
			}
		})
	}
}

// Spatial types are refused outright rather than being allowed to fail per row at
// runtime, so the mistake surfaces when the config is checked.
func TestMapRefusesSpatialTypes(t *testing.T) {
	for _, dt := range []string{"geometry", "point", "linestring", "polygon", "multipoint", "geomcollection"} {
		if _, err := Map(col("g", dt, dt)); err == nil {
			t.Errorf("expected %s to be unsupported", dt)
		}
	}
}

func TestMapRefusesUnknownTypes(t *testing.T) {
	if _, err := Map(col("x", "quantumfloat", "quantumfloat")); err == nil {
		t.Fatal("expected an unrecognised type to be refused rather than guessed at")
	}
}

// A nullable column becomes Nullable(T) in ClickHouse, which costs a null mask
// and blocks some optimisations, so it is applied only where the schema says so.
func TestNullableColumnsWrapClickHouseType(t *testing.T) {
	got, err := Map(col("note", "varchar", "varchar(64)", nullable))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ClickHouse != "Nullable(String)" {
		t.Fatalf("clickhouse = %q, want Nullable(String)", got.ClickHouse)
	}
	notNull, err := Map(col("note", "varchar", "varchar(64)"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if notNull.ClickHouse != "String" {
		t.Fatalf("clickhouse = %q, want String", notNull.ClickHouse)
	}
}

// A LowCardinality wrapper must sit outside Nullable, which is the order
// ClickHouse accepts.
func TestNullableEnumNesting(t *testing.T) {
	enum := col("status", "enum", "enum('a','b')", nullable)
	enum.EnumValues = []string{"a", "b"}
	got, err := Map(enum)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ClickHouse != "LowCardinality(Nullable(String))" {
		t.Fatalf("clickhouse = %q, want LowCardinality(Nullable(String))", got.ClickHouse)
	}
}

// Generated columns are absent from binlog row images, so a stream that maps one
// would always write a null.
func TestMapRefusesGeneratedColumns(t *testing.T) {
	if _, err := Map(col("total_with_tax", "decimal", "decimal(18,2)", generated)); err == nil {
		t.Fatal("expected a generated column to be refused")
	}
}

func TestSupportedReportsWithoutError(t *testing.T) {
	if !Supported(col("id", "bigint", "bigint unsigned")) {
		t.Error("bigint unsigned should be supported")
	}
	if Supported(col("g", "geometry", "geometry")) {
		t.Error("geometry should not be supported")
	}
}
