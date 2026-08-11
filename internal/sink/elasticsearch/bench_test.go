package elasticsearch

import (
	"fmt"
	"testing"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
)

// benchDocs builds a batch the size a real stream sends.
func benchDocs(n int) []cdc.Doc {
	docs := make([]cdc.Doc, 0, n)
	for i := 0; i < n; i++ {
		docs = append(docs, cdc.Doc{
			Key:     fmt.Sprintf("%d", 1_000_000+i),
			Version: uint64(1_800_000_000_000_000 + i),
			Body:    []byte(`{"id":1000000,"user_id":7,"status":"paid","total_amount":"19.90"}`),
		})
	}
	return docs
}

func BenchmarkEncodeBulk(b *testing.B) {
	s, err := New(Options{Addresses: []string{"http://localhost:9200"}, Index: "orders-v1"})
	if err != nil {
		b.Fatal(err)
	}
	docs := benchDocs(1000)

	b.ReportAllocs()
	b.SetBytes(int64(len(docs)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body, err := s.encodeBulk(docs)
		if err != nil {
			b.Fatal(err)
		}
		if len(body) == 0 {
			b.Fatal("empty body")
		}
	}
}

// Encoding a batch must stay cheap relative to the network round trip it precedes,
// or batching stops paying for itself.
func TestBulkEncodingStaysCheap(t *testing.T) {
	if testing.Short() {
		t.Skip("performance budgets are not measured in short mode")
	}
	if raceDetectorEnabled {
		t.Skip("performance budgets are not measured under the race detector, which multiplies both time and allocation counts")
	}

	result := testing.Benchmark(BenchmarkEncodeBulk)
	if result.N == 0 {
		t.Fatal("benchmark did not run")
	}

	const docsPerBatch = 1000
	nsPerDoc := result.NsPerOp() / docsPerBatch
	allocsPerDoc := result.AllocsPerOp() / docsPerBatch

	// A microsecond per document would make encoding a millisecond of every batch,
	// which is the same order as the request itself.
	if nsPerDoc > 1_000 {
		t.Errorf("%dns per document to encode, budget is 1000ns; %s", nsPerDoc, result.String())
	}
	// Bodies are appended, not re-marshalled, so the count per document should be
	// close to zero and grow only with buffer growth.
	if allocsPerDoc > 2 {
		t.Errorf("%d allocations per document, budget is 2; %s", allocsPerDoc, result.MemString())
	}
}
