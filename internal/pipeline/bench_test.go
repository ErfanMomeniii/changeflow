package pipeline

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/config"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
)

// benchMeta and benchRow mirror the development schema, including the columns whose
// encoding costs the most: an exact decimal, an enum, a set, a latin1 string, and
// two flavours of timestamp.
var benchMeta = func() schema.TableMeta {
	m := schema.TableMeta{
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
			{Name: "placed_at", Position: 7, DataType: "datetime", ColumnType: "datetime(3)",
				Nullable: true, DateTimePrecision: 3},
			{Name: "updated_at", Position: 8, DataType: "timestamp", ColumnType: "timestamp(3)",
				DateTimePrecision: 3},
		},
		PrimaryKey: []string{"id"},
	}
	return m
}()

func benchRow() cdc.Row {
	return cdc.Row{
		uint64(18446744073709551001),
		uint64(7),
		int64(2),
		int64(0b0011),
		decimal.RequireFromString("19.90"),
		int64(1),
		string([]byte{0x63, 0x61, 0x66, 0xE9}),
		"2026-08-11 10:00:00.000",
		"2026-08-11 11:00:00.000",
	}
}

// benchPlan compiles a mapping over the awkward table, so the measurement covers
// the work a real stream does rather than a trivial two-column case.
func benchPlan(b *testing.B, dialect Dialect) *Plan {
	b.Helper()

	m := &benchMeta
	p, err := Compile(m, config.Mapping{Key: []string{"id"}}, dialect, time.UTC, "null")
	if err != nil {
		b.Fatalf("compile: %v", err)
	}
	return p
}

func BenchmarkTransformInsert(b *testing.B) {
	p := benchPlan(b, DialectElasticsearch)
	ev := &cdc.ChangeEvent{Meta: &benchMeta, Op: cdc.OpInsert, After: benchRow(), Seq: 1000}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		docs, err := p.Apply(ev)
		if err != nil {
			b.Fatal(err)
		}
		if len(docs) != 1 {
			b.Fatalf("expected 1 document, got %d", len(docs))
		}
	}
}

func BenchmarkTransformUpdate(b *testing.B) {
	p := benchPlan(b, DialectElasticsearch)
	before, after := benchRow(), benchRow()
	after[2] = int64(3)
	ev := &cdc.ChangeEvent{Meta: &benchMeta, Op: cdc.OpUpdate, Before: before, After: after, Seq: 1000}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Apply(ev); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTransformDelete(b *testing.B) {
	p := benchPlan(b, DialectElasticsearch)
	ev := &cdc.ChangeEvent{Meta: &benchMeta, Op: cdc.OpDelete, Before: benchRow(), Seq: 1000}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Apply(ev); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatcherAdd(b *testing.B) {
	batcher, err := NewBatcher(Limits{MaxRows: 1000, MaxBytes: 8 << 20, FlushInterval: time.Hour}, time.Now)
	if err != nil {
		b.Fatal(err)
	}
	doc := cdc.Doc{Key: "1234567890", Version: 1, Body: []byte(`{"id":1234567890,"status":"paid"}`)}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batcher.Add(doc)
	}
}

// Allocation counts are the gate, not wall time.
//
// Allocations are a property of the code and reproduce anywhere; nanoseconds on a
// shared CI runner are not comparable to a developer's machine. The time bound here
// is deliberately loose, to catch something pathological rather than a regression of
// a few percent.
func TestTransformStaysWithinItsAllocationBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("performance budgets are not measured in short mode")
	}
	if raceDetectorEnabled {
		t.Skip("performance budgets are not measured under the race detector, which multiplies both time and allocation counts")
	}

	// The time bound comes from the design's target of 150k documents per second per
	// core, which is 6.7 microseconds each. Measured cost is around 1 microsecond, so
	// this bound catches a collapse rather than a small regression, and stays honest
	// on a slower shared runner.
	const perDocumentBudget = 6_700

	for _, tc := range []struct {
		name      string
		benchmark func(*testing.B)
		maxAllocs int64
		maxNanos  int64
	}{
		// Allocation counts are set just above what the code does today: the body
		// buffer, the key, the returned slice, and the values copied into the body.
		//
		// The design aims at under one allocation per event, which these do not meet.
		// Reaching it means pooling the body buffer, and pooling needs the sink to
		// signal when it has finished with the bytes. Recorded here rather than
		// quietly written off.
		{"insert", BenchmarkTransformInsert, 14, perDocumentBudget},
		{"update", BenchmarkTransformUpdate, 17, perDocumentBudget},
		{"delete", BenchmarkTransformDelete, 5, perDocumentBudget},
		{"batcher", BenchmarkBatcherAdd, 1, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := testing.Benchmark(tc.benchmark)
			if result.N == 0 {
				t.Fatal("benchmark did not run")
			}

			if allocs := result.AllocsPerOp(); allocs > tc.maxAllocs {
				t.Errorf("%d allocations per event, budget is %d; %s",
					allocs, tc.maxAllocs, result.MemString())
			}
			if ns := result.NsPerOp(); ns > tc.maxNanos {
				t.Errorf("%dns per event, loose bound is %dns; %s",
					ns, tc.maxNanos, result.String())
			}
		})
	}
}
