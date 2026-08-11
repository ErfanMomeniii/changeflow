package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/config"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
)

// ordersMeta mirrors the development schema, including the awkward columns.
func ordersMeta(t *testing.T) *schema.TableMeta {
	t.Helper()
	m := &schema.TableMeta{
		Schema: "shop",
		Table:  "orders",
		Columns: []schema.Column{
			{Name: "id", Position: 0, DataType: "bigint", ColumnType: "bigint unsigned", Unsigned: true},
			{Name: "user_id", Position: 1, DataType: "bigint", ColumnType: "bigint unsigned", Unsigned: true},
			{Name: "status", Position: 2, DataType: "enum", ColumnType: "enum('draft','paid','shipped')",
				EnumValues: []string{"draft", "paid", "shipped"}},
			{Name: "channels", Position: 3, DataType: "set", ColumnType: "set('web','ios','android','pos')",
				SetValues: []string{"web", "ios", "android", "pos"}},
			{Name: "total_amount", Position: 4, DataType: "decimal", ColumnType: "decimal(18,2)",
				NumericPrecision: 18, NumericScale: 2},
			{Name: "is_gift", Position: 5, DataType: "tinyint", ColumnType: "tinyint(1)"},
			{Name: "note_latin1", Position: 6, DataType: "varchar", ColumnType: "varchar(64)",
				Nullable: true, CharacterSet: "latin1"},
			{Name: "metadata", Position: 7, DataType: "json", ColumnType: "json", Nullable: true},
			{Name: "placed_at", Position: 8, DataType: "datetime", ColumnType: "datetime(3)",
				Nullable: true, DateTimePrecision: 3},
			{Name: "updated_at", Position: 9, DataType: "timestamp", ColumnType: "timestamp(3)",
				DateTimePrecision: 3},
			{Name: "total_with_tax", Position: 10, DataType: "decimal", ColumnType: "decimal(18,2)",
				Generated: true, NumericPrecision: 18, NumericScale: 2},
		},
		PrimaryKey: []string{"id"},
	}
	// Exercise the same path a loaded definition takes.
	if _, ok := m.Column("id"); !ok {
		t.Fatal("meta not indexed")
	}
	return m
}

// fullRow returns a row in ordinal order for the table above.
func fullRow() cdc.Row {
	return cdc.Row{
		uint64(42),                             // id
		uint64(7),                              // user_id
		int64(2),                               // status: paid, stored as a member number
		int64(0b0011),                          // channels: web,ios as a bitmask
		decimal.RequireFromString("19.90"),     // total_amount
		int64(1),                               // is_gift
		string([]byte{0x63, 0x61, 0x66, 0xE9}), // note_latin1: "café" in latin1
		[]byte(`{"coupon":"WELCOME"}`),         // metadata
		"2026-08-11 10:00:00.000",              // placed_at, wall clock
		"2026-08-11 11:00:00.000",              // updated_at, already UTC
		decimal.RequireFromString("21.69"),     // total_with_tax, generated
	}
}

func newPlan(t *testing.T, m *schema.TableMeta, mapping config.Mapping, dialect Dialect) *Plan {
	t.Helper()
	p, err := Compile(m, mapping, dialect, time.UTC, "null")
	if err != nil {
		t.Fatalf("compile plan: %v", err)
	}
	return p
}

func insertEvent(m *schema.TableMeta) *cdc.ChangeEvent {
	return &cdc.ChangeEvent{Meta: m, Op: cdc.OpInsert, After: fullRow(), Seq: 1000, GTID: "uuid:1"}
}

func applyOne(t *testing.T, p *Plan, ev *cdc.ChangeEvent) cdc.Doc {
	t.Helper()
	docs, err := p.Apply(ev)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 document, got %d", len(docs))
	}
	return docs[0]
}

func body(t *testing.T, d cdc.Doc) string {
	t.Helper()
	return string(d.Body)
}

func TestInsertProducesOneUpsert(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}}, DialectElasticsearch)

	doc := applyOne(t, p, insertEvent(m))

	if doc.Key != "42" {
		t.Errorf("key = %q, want 42", doc.Key)
	}
	if doc.Deleted {
		t.Error("an insert must not be marked deleted")
	}
	if doc.Version != 1000 {
		t.Errorf("version = %d, want 1000", doc.Version)
	}
}

// A generated column is absent from row images, so it must never be written.
func TestGeneratedColumnIsNotWritten(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}}, DialectElasticsearch)

	if got := body(t, applyOne(t, p, insertEvent(m))); strings.Contains(got, "total_with_tax") {
		t.Fatalf("generated column present in body: %s", got)
	}
}

func TestRenameAndIncludeAreApplied(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{
		Key:     []string{"id"},
		Include: []string{"id", "status", "total_amount"},
		Rename:  map[string]string{"total_amount": "total"},
	}, DialectElasticsearch)

	got := body(t, applyOne(t, p, insertEvent(m)))

	if !strings.Contains(got, `"total":`) {
		t.Errorf("renamed field missing: %s", got)
	}
	if strings.Contains(got, "total_amount") {
		t.Errorf("original name should be gone: %s", got)
	}
	if strings.Contains(got, "user_id") {
		t.Errorf("excluded field present: %s", got)
	}
}

// An unsigned 64-bit value above 2^63 must survive exactly. Passing it through a
// float, or a signed conversion, would corrupt it silently.
func TestLargeUnsignedIntegerIsExact(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}, Include: []string{"id"}}, DialectElasticsearch)

	row := fullRow()
	row[0] = uint64(18446744073709551001)
	doc := applyOne(t, p, &cdc.ChangeEvent{Meta: m, Op: cdc.OpInsert, After: row, Seq: 1})

	if !strings.Contains(body(t, doc), "18446744073709551001") {
		t.Fatalf("value lost precision: %s", body(t, doc))
	}
	if doc.Key != "18446744073709551001" {
		t.Fatalf("key = %q, want the exact value", doc.Key)
	}
}

// Elasticsearch has no exact decimal type, so the value is written as a string to
// keep it byte-identical, including its trailing zero.
func TestDecimalIsQuotedForElasticsearch(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}, Include: []string{"id", "total_amount"}}, DialectElasticsearch)

	if got := body(t, applyOne(t, p, insertEvent(m))); !strings.Contains(got, `"total_amount":"19.90"`) {
		t.Fatalf("expected a quoted exact decimal, got: %s", got)
	}
}

// ClickHouse's column is Decimal(18,2), which takes a JSON number.
func TestDecimalIsUnquotedForClickHouse(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}, Include: []string{"id", "total_amount"}}, DialectClickHouse)

	if got := body(t, applyOne(t, p, insertEvent(m))); !strings.Contains(got, `"total_amount":19.90`) {
		t.Fatalf("expected an unquoted exact decimal, got: %s", got)
	}
}

// The binlog stores an ENUM as a member number; writing that number would be
// meaningless in a search index.
func TestEnumIsWrittenAsItsLabel(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}, Include: []string{"id", "status"}}, DialectElasticsearch)

	if got := body(t, applyOne(t, p, insertEvent(m))); !strings.Contains(got, `"status":"paid"`) {
		t.Fatalf("expected the label, got: %s", got)
	}
}

// A SET is a bitmask, and its members are written as an array in declaration
// order.
func TestSetIsWrittenAsAnArrayOfLabels(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}, Include: []string{"id", "channels"}}, DialectElasticsearch)

	if got := body(t, applyOne(t, p, insertEvent(m))); !strings.Contains(got, `"channels":["web","ios"]`) {
		t.Fatalf("expected an array of labels, got: %s", got)
	}
}

func TestEmptySetIsAnEmptyArray(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}, Include: []string{"id", "channels"}}, DialectElasticsearch)

	row := fullRow()
	row[3] = int64(0)
	doc := applyOne(t, p, &cdc.ChangeEvent{Meta: m, Op: cdc.OpInsert, After: row, Seq: 1})

	if got := body(t, doc); !strings.Contains(got, `"channels":[]`) {
		t.Fatalf("expected an empty array, got: %s", got)
	}
}

// A latin1 column's bytes are not UTF-8. Writing them unchanged produces mojibake
// in the destination, so they are converted using the column's charset.
func TestLatin1ColumnIsConvertedToUTF8(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}, Include: []string{"id", "note_latin1"}}, DialectElasticsearch)

	got := body(t, applyOne(t, p, insertEvent(m)))

	if !strings.Contains(got, `"note_latin1":"café"`) {
		t.Fatalf("expected latin1 to be converted to UTF-8, got: %s", got)
	}
}

func TestTinyIntOneBecomesBoolean(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}, Include: []string{"id", "is_gift"}}, DialectElasticsearch)

	if got := body(t, applyOne(t, p, insertEvent(m))); !strings.Contains(got, `"is_gift":true`) {
		t.Fatalf("expected a boolean, got: %s", got)
	}
}

// JSON is passed through verbatim: re-encoding it could reorder keys or lose
// numeric precision.
func TestJSONIsPassedThroughVerbatim(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}, Include: []string{"id", "metadata"}}, DialectElasticsearch)

	if got := body(t, applyOne(t, p, insertEvent(m))); !strings.Contains(got, `"metadata":{"coupon":"WELCOME"}`) {
		t.Fatalf("expected the JSON document unchanged, got: %s", got)
	}
}

func TestNullIsWrittenAsNull(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}, Include: []string{"id", "note_latin1"}}, DialectElasticsearch)

	row := fullRow()
	row[6] = nil
	doc := applyOne(t, p, &cdc.ChangeEvent{Meta: m, Op: cdc.OpInsert, After: row, Seq: 1})

	if got := body(t, doc); !strings.Contains(got, `"note_latin1":null`) {
		t.Fatalf("expected null, got: %s", got)
	}
}

// DATETIME is wall-clock with no zone, so it is interpreted in the source's zone
// and written as an instant.
func TestDatetimeIsInterpretedInSourceZone(t *testing.T) {
	m := ordersMeta(t)
	tehran, err := time.LoadLocation("Asia/Tehran")
	if err != nil {
		t.Skipf("zone data unavailable: %v", err)
	}
	p, err := Compile(m, config.Mapping{Key: []string{"id"}, Include: []string{"id", "placed_at"}},
		DialectElasticsearch, tehran, "null")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	got := body(t, applyOne(t, p, insertEvent(m)))

	// 10:00 in Tehran is 06:30 UTC.
	if !strings.Contains(got, `"placed_at":"2026-08-11T06:30:00Z"`) {
		t.Fatalf("expected the wall clock to be converted from the source zone, got: %s", got)
	}
}

// TIMESTAMP is already an instant in UTC, so it must not be shifted again.
func TestTimestampIsNotShifted(t *testing.T) {
	m := ordersMeta(t)
	tehran, err := time.LoadLocation("Asia/Tehran")
	if err != nil {
		t.Skipf("zone data unavailable: %v", err)
	}
	p, err := Compile(m, config.Mapping{Key: []string{"id"}, Include: []string{"id", "updated_at"}},
		DialectElasticsearch, tehran, "null")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	got := body(t, applyOne(t, p, insertEvent(m)))

	if !strings.Contains(got, `"updated_at":"2026-08-11T11:00:00Z"`) {
		t.Fatalf("a TIMESTAMP is already UTC and must not be converted, got: %s", got)
	}
}

func TestZeroDatePolicies(t *testing.T) {
	m := ordersMeta(t)

	for _, tc := range []struct {
		policy   string
		wantBody string
		wantErr  bool
	}{
		{"null", `"placed_at":null`, false},
		{"epoch", `"placed_at":"1970-01-01T00:00:00Z"`, false},
		{"error", "", true},
	} {
		t.Run(tc.policy, func(t *testing.T) {
			p, err := Compile(m, config.Mapping{Key: []string{"id"}, Include: []string{"id", "placed_at"}},
				DialectElasticsearch, time.UTC, tc.policy)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}

			row := fullRow()
			row[8] = "0000-00-00 00:00:00.000"
			docs, err := p.Apply(&cdc.ChangeEvent{Meta: m, Op: cdc.OpInsert, After: row, Seq: 1})

			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error for a zero date")
				}
				return
			}
			if err != nil {
				t.Fatalf("apply: %v", err)
			}
			if got := string(docs[0].Body); !strings.Contains(got, tc.wantBody) {
				t.Fatalf("got %s, want it to contain %s", got, tc.wantBody)
			}
		})
	}
}

func TestDeleteProducesATombstoneKeyedFromBefore(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}}, DialectElasticsearch)

	doc := applyOne(t, p, &cdc.ChangeEvent{Meta: m, Op: cdc.OpDelete, Before: fullRow(), Seq: 2000})

	if !doc.Deleted {
		t.Error("expected the document to be marked deleted")
	}
	if doc.Body != nil {
		t.Errorf("a delete carries no body, got %s", doc.Body)
	}
	if doc.Key != "42" {
		t.Errorf("key = %q, want 42 taken from the prior values", doc.Key)
	}
}

// An update that changes the key must remove the old document. Without this, the
// old key is orphaned in the destination forever, since no later event mentions it.
func TestKeyChangingUpdateProducesDeleteThenUpsert(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}}, DialectElasticsearch)

	before, after := fullRow(), fullRow()
	after[0] = uint64(43)

	docs, err := p.Apply(&cdc.ChangeEvent{Meta: m, Op: cdc.OpUpdate, Before: before, After: after, Seq: 5000})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	if len(docs) != 2 {
		t.Fatalf("expected 2 documents, got %d", len(docs))
	}
	if !docs[0].Deleted || docs[0].Key != "42" {
		t.Errorf("first document should delete the old key, got %+v", docs[0])
	}
	if docs[1].Deleted || docs[1].Key != "43" {
		t.Errorf("second document should write the new key, got key=%q deleted=%v", docs[1].Key, docs[1].Deleted)
	}
	// Consecutive versions fix their order relative to each other.
	if docs[1].Version <= docs[0].Version {
		t.Errorf("versions must increase: delete=%d upsert=%d", docs[0].Version, docs[1].Version)
	}
}

func TestUpdateWithoutKeyChangeProducesOneUpsert(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}}, DialectElasticsearch)

	before, after := fullRow(), fullRow()
	after[2] = int64(3) // status becomes shipped

	docs, err := p.Apply(&cdc.ChangeEvent{Meta: m, Op: cdc.OpUpdate, Before: before, After: after, Seq: 7000})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 document, got %d", len(docs))
	}
	if !strings.Contains(string(docs[0].Body), `"status":"shipped"`) {
		t.Errorf("expected the new value, got %s", docs[0].Body)
	}
}

// A snapshot row is an upsert, and the destination cannot tell it apart from an
// insert.
func TestSnapshotRowIsAnUpsert(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}}, DialectElasticsearch)

	doc := applyOne(t, p, &cdc.ChangeEvent{Meta: m, Op: cdc.OpSnapshot, After: fullRow(), Seq: 10})

	if doc.Deleted || doc.Key != "42" {
		t.Fatalf("expected an upsert keyed 42, got %+v", doc)
	}
}

func TestCompositeKeyIsJoined(t *testing.T) {
	m := &schema.TableMeta{
		Schema: "shop", Table: "order_items",
		Columns: []schema.Column{
			{Name: "order_id", Position: 0, DataType: "bigint", ColumnType: "bigint unsigned", Unsigned: true},
			{Name: "sku", Position: 1, DataType: "varchar", ColumnType: "varchar(64)"},
		},
		PrimaryKey: []string{"order_id", "sku"},
	}
	p := newPlan(t, m, config.Mapping{Key: []string{"order_id", "sku"}}, DialectElasticsearch)

	doc := applyOne(t, p, &cdc.ChangeEvent{Meta: m, Op: cdc.OpInsert, After: cdc.Row{uint64(100), "SKU-1"}, Seq: 1})

	if doc.Key != "100:SKU-1" {
		t.Fatalf("key = %q, want 100:SKU-1", doc.Key)
	}
}

// Without escaping, the keys (100, "1:x") and (100, "1"),"x" would collide.
func TestCompositeKeyEscapesSeparatorInValues(t *testing.T) {
	m := &schema.TableMeta{
		Schema: "shop", Table: "order_items",
		Columns: []schema.Column{
			{Name: "order_id", Position: 0, DataType: "bigint", ColumnType: "bigint unsigned", Unsigned: true},
			{Name: "sku", Position: 1, DataType: "varchar", ColumnType: "varchar(64)"},
		},
		PrimaryKey: []string{"order_id", "sku"},
	}
	p := newPlan(t, m, config.Mapping{Key: []string{"order_id", "sku"}}, DialectElasticsearch)

	first := applyOne(t, p, &cdc.ChangeEvent{Meta: m, Op: cdc.OpInsert, After: cdc.Row{uint64(100), "SKU:99"}, Seq: 1})
	second := applyOne(t, p, &cdc.ChangeEvent{Meta: m, Op: cdc.OpInsert, After: cdc.Row{uint64(100), "SKU"}, Seq: 2})

	if first.Key == second.Key {
		t.Fatalf("distinct keys collided: both produced %q", first.Key)
	}
	if strings.Count(first.Key, ":") != 1 {
		t.Errorf("the separator should appear once, escaped elsewhere: %q", first.Key)
	}
}

// Elasticsearch refuses an _id longer than 512 bytes, so an over-long key is
// replaced by a stable digest rather than failing every write.
func TestOverlongKeyIsHashed(t *testing.T) {
	m := &schema.TableMeta{
		Schema: "shop", Table: "wide",
		Columns: []schema.Column{
			{Name: "k", Position: 0, DataType: "varchar", ColumnType: "varchar(1024)"},
		},
		PrimaryKey: []string{"k"},
	}
	p := newPlan(t, m, config.Mapping{Key: []string{"k"}}, DialectElasticsearch)

	long := strings.Repeat("x", 600)
	first := applyOne(t, p, &cdc.ChangeEvent{Meta: m, Op: cdc.OpInsert, After: cdc.Row{long}, Seq: 1})
	again := applyOne(t, p, &cdc.ChangeEvent{Meta: m, Op: cdc.OpInsert, After: cdc.Row{long}, Seq: 2})

	if len(first.Key) > 512 {
		t.Fatalf("key is %d bytes, which Elasticsearch would reject", len(first.Key))
	}
	if first.Key != again.Key {
		t.Fatal("the digest must be stable for the same input")
	}
	other := applyOne(t, p, &cdc.ChangeEvent{Meta: m, Op: cdc.OpInsert, After: cdc.Row{strings.Repeat("y", 600)}, Seq: 3})
	if other.Key == first.Key {
		t.Fatal("different keys must not digest to the same value")
	}
}

// A null in a key column cannot identify a row, so it is refused rather than
// written under a key that reads as the string "null".
func TestNullKeyIsRefused(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}}, DialectElasticsearch)

	row := fullRow()
	row[0] = nil

	if _, err := p.Apply(&cdc.ChangeEvent{Meta: m, Op: cdc.OpInsert, After: row, Seq: 1}); err == nil {
		t.Fatal("expected a null key to be refused")
	}
}

func TestApplyRejectsRowOfWrongWidth(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}}, DialectElasticsearch)

	short := cdc.Row{uint64(42), uint64(7)}

	if _, err := p.Apply(&cdc.ChangeEvent{Meta: m, Op: cdc.OpInsert, After: short, Seq: 1}); err == nil {
		t.Fatal("expected a row with fewer values than columns to be refused")
	}
}

// An unsupported charset must fail loudly rather than write bytes that are not
// valid UTF-8 into a text field.
func TestUnsupportedCharsetIsRefusedAtCompileTime(t *testing.T) {
	m := ordersMeta(t)
	for i := range m.Columns {
		if m.Columns[i].Name == "note_latin1" {
			m.Columns[i].CharacterSet = "gbk"
		}
	}

	_, err := Compile(m, config.Mapping{Key: []string{"id"}, Include: []string{"id", "note_latin1"}},
		DialectElasticsearch, time.UTC, "null")
	if err == nil {
		t.Fatal("expected an unsupported charset to be refused when the plan is compiled")
	}
	if !strings.Contains(err.Error(), "gbk") {
		t.Fatalf("error should name the charset, got: %v", err)
	}
}

func TestCompileRejectsUnsupportedColumnType(t *testing.T) {
	m := ordersMeta(t)
	m.Columns = append(m.Columns, schema.Column{
		Name: "shape", Position: 11, DataType: "geometry", ColumnType: "geometry",
	})

	if _, err := Compile(m, config.Mapping{Key: []string{"id"}}, DialectElasticsearch, time.UTC, "null"); err == nil {
		t.Fatal("expected a spatial column to be refused when the plan is compiled")
	}
}

// The body is produced by a single encode, so a plan reused across events must not
// leak the previous document into the next.
func TestPlanIsReusableAcrossEvents(t *testing.T) {
	m := ordersMeta(t)
	p := newPlan(t, m, config.Mapping{Key: []string{"id"}, Include: []string{"id", "status"}}, DialectElasticsearch)

	first := applyOne(t, p, insertEvent(m))

	row := fullRow()
	row[0] = uint64(99)
	row[2] = int64(1)
	second := applyOne(t, p, &cdc.ChangeEvent{Meta: m, Op: cdc.OpInsert, After: row, Seq: 2})

	if strings.Contains(string(second.Body), `"paid"`) {
		t.Fatalf("second document contains the first's value: %s", second.Body)
	}
	if !strings.Contains(string(first.Body), `"paid"`) {
		t.Fatalf("first document was overwritten: %s", first.Body)
	}
}
