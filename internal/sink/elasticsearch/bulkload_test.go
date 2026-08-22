package elasticsearch

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type settingsServer struct {
	mu                      sync.Mutex
	declared                string
	applied                 []string
	refreshed, merged       int
	readStatus, writeStatus int
	mergeStatus             int
}

func (f *settingsServer) sink(t *testing.T) *Sink {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/_settings"):
			if f.readStatus != 0 {
				w.WriteHeader(f.readStatus)
				_, _ = w.Write([]byte(`{"error":{"type":"index_not_found_exception"}}`))
				return
			}
			_, _ = w.Write([]byte(`{"orders-v2":{"settings":{"index":{` + f.declared + `}}}}`))
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/_settings"):
			body, _ := io.ReadAll(r.Body)
			f.applied = append(f.applied, string(body))
			if f.writeStatus != 0 {
				w.WriteHeader(f.writeStatus)
				_, _ = w.Write([]byte(`{"error":{"type":"illegal_argument_exception"}}`))
				return
			}
			_, _ = w.Write([]byte(`{"acknowledged":true}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_refresh"):
			f.refreshed++
			_, _ = w.Write([]byte(`{"_shards":{"total":1,"successful":1,"failed":0}}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_forcemerge"):
			f.merged++
			if f.mergeStatus != 0 {
				w.WriteHeader(f.mergeStatus)
				_, _ = w.Write([]byte(`{"error":{"type":"circuit_breaking_exception"}}`))
				return
			}
			_, _ = w.Write([]byte(`{"_shards":{"total":1,"successful":1,"failed":0}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	s, err := New(Options{Addresses: []string{server.URL}, Index: "orders-v2", Alias: "orders"})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func (f *settingsServer) recorded() ([]string, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.applied...), f.refreshed, f.merged
}

// Refreshing during a scan is work done over data that is about to grow, so it is off for
// its duration.
func TestBulkLoadTurnsOffRefreshing(t *testing.T) {
	server := &settingsServer{declared: `"refresh_interval":"5s","number_of_replicas":"2"`}
	s := server.sink(t)
	load, err := s.BeginBulkLoad(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if !load.Applied() {
		t.Fatal("the settings were reported as unchanged")
	}
	applied, _, _ := server.recorded()
	if len(applied) != 1 {
		t.Fatalf("expected one settings change, got %d: %v", len(applied), applied)
	}
	if !strings.Contains(applied[0], `"refresh_interval":"-1"`) {
		t.Errorf("refreshing was not turned off: %s", applied[0])
	}
	if strings.Contains(applied[0], "number_of_replicas") {
		t.Errorf("the replica count should not be touched: %s", applied[0])
	}
}

// What was replaced has to come back exactly. Restoring a guessed value would quietly
// change an index's replication or refresh behaviour as a side effect of a rebuild.
func TestBulkLoadRestoresWhatItReplaced(t *testing.T) {
	server := &settingsServer{declared: `"refresh_interval":"5s","number_of_replicas":"2"`}
	s := server.sink(t)
	load, err := s.BeginBulkLoad(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := s.EndBulkLoad(context.Background(), load); err != nil {
		t.Fatalf("end: %v", err)
	}
	applied, refreshed, _ := server.recorded()
	if len(applied) != 2 {
		t.Fatalf("expected the settings to be changed and restored, got %d changes: %v", len(applied), applied)
	}
	if !strings.Contains(applied[1], `"refresh_interval":"5s"`) {
		t.Errorf("the refresh interval was not restored: %s", applied[1])
	}
	if refreshed != 1 {
		t.Errorf("the index was refreshed %d times, want once so the scanned rows are visible", refreshed)
	}
}

// An index that never declared these settings must go back to inheriting the cluster
// default, not to whatever this build believes the default to be.
func TestBulkLoadClearsSettingsTheIndexNeverDeclared(t *testing.T) {
	server := &settingsServer{declared: `"number_of_shards":"1"`}
	s := server.sink(t)
	load, err := s.BeginBulkLoad(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := s.EndBulkLoad(context.Background(), load); err != nil {
		t.Fatalf("end: %v", err)
	}
	applied, _, _ := server.recorded()
	if got := applied[1]; !strings.Contains(got, `"refresh_interval":null`) {
		t.Errorf("expected the override to be cleared, got %s", got)
	}
}

// A scan killed partway through leaves refreshing off. Reading that back as the value to
// restore would leave the index never refreshing, while an alias was moved to it: readers
// would find it empty and nothing would say why.
func TestBulkLoadTreatsRefreshingAlreadyOffAsLeftBehind(t *testing.T) {
	server := &settingsServer{declared: `"refresh_interval":"-1","number_of_replicas":"0"`}
	s := server.sink(t)
	load, err := s.BeginBulkLoad(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := s.EndBulkLoad(context.Background(), load); err != nil {
		t.Fatalf("end: %v", err)
	}
	applied, refreshed, _ := server.recorded()
	if got := applied[1]; !strings.Contains(got, `"refresh_interval":null`) {
		t.Errorf("restored to %s, want the cluster default rather than the value a killed scan left", got)
	}
	if refreshed != 1 {
		t.Errorf("the index was refreshed %d times, want once", refreshed)
	}
}

// Restoring settings that were never changed would be a change of its own.
func TestEndBulkLoadDoesNothingWhenNothingWasApplied(t *testing.T) {
	server := &settingsServer{declared: `"refresh_interval":"5s"`}
	s := server.sink(t)
	if err := s.EndBulkLoad(context.Background(), LoadSettings{}); err != nil {
		t.Fatalf("end: %v", err)
	}
	applied, refreshed, _ := server.recorded()
	if len(applied) != 0 || refreshed != 0 {
		t.Errorf("expected no requests, got %v and %d refreshes", applied, refreshed)
	}
}

func TestBulkLoadReportsAFailureToRead(t *testing.T) {
	server := &settingsServer{readStatus: http.StatusNotFound}
	s := server.sink(t)
	load, err := s.BeginBulkLoad(context.Background())
	if err == nil {
		t.Fatal("expected an error when the settings cannot be read")
	}
	if load.Applied() {
		t.Error("a failed start reported that it had changed settings")
	}
}

func TestBulkLoadReportsAFailureToApply(t *testing.T) {
	server := &settingsServer{declared: `"refresh_interval":"5s"`, writeStatus: http.StatusBadRequest}
	s := server.sink(t)
	load, err := s.BeginBulkLoad(context.Background())
	if err == nil {
		t.Fatal("expected an error when the settings cannot be changed")
	}
	if load.Applied() {
		t.Error("a rejected change reported that it had been applied")
	}
	if !strings.Contains(err.Error(), "orders-v2") {
		t.Errorf("the error should name the index, got %v", err)
	}
}

func TestForceMergeAsksForASingleSegment(t *testing.T) {
	server := &settingsServer{declared: `"refresh_interval":"5s"`}
	s := server.sink(t)
	if err := s.ForceMerge(context.Background()); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if _, _, merged := server.recorded(); merged != 1 {
		t.Errorf("the index was merged %d times, want once", merged)
	}
}

// A merge that fails leaves an index that is slower to search, not a wrong one, so the
// error is reported for the caller to log rather than being hidden.
func TestForceMergeReportsAFailure(t *testing.T) {
	server := &settingsServer{declared: `"refresh_interval":"5s"`, mergeStatus: http.StatusTooManyRequests}
	s := server.sink(t)
	if err := s.ForceMerge(context.Background()); err == nil {
		t.Fatal("expected an error when the merge is refused")
	}
}
