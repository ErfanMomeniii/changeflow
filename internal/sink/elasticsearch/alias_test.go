package elasticsearch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type aliasServer struct {
	mu          sync.Mutex
	targets     []string
	actions     []map[string]map[string]any
	readStatus  int
	writeStatus int
}

func (a *aliasServer) sink(t *testing.T, index, alias string) *Sink {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/_alias/"):
			a.mu.Lock()
			status, targets := a.readStatus, append([]string(nil), a.targets...)
			a.mu.Unlock()
			if status != 0 {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"error":"nope"}`))
				return
			}
			if len(targets) == 0 {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error":{"type":"alias_not_found_exception"}}`))
				return
			}
			body := map[string]any{}
			for _, target := range targets {
				body[target] = map[string]any{"aliases": map[string]any{}}
			}
			_ = json.NewEncoder(w).Encode(body)
		case r.Method == http.MethodPost && r.URL.Path == "/_aliases":
			var request struct {
				Actions []map[string]map[string]any `json:"actions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			a.mu.Lock()
			a.actions = request.Actions
			status := a.writeStatus
			a.mu.Unlock()
			if status != 0 {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"error":{"type":"index_not_found_exception"}}`))
				return
			}
			_, _ = w.Write([]byte(`{"acknowledged":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	s, err := New(Options{Addresses: []string{server.URL}, Index: index, Alias: alias})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func (a *aliasServer) recorded() []map[string]map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.actions
}

// A rebuild's whole point: readers move from the old index to the new one in a single
// request, so they never see the alias pointing at nothing or at a half-built index.
func TestPromoteAliasMovesReadersInOneRequest(t *testing.T) {
	server := &aliasServer{targets: []string{"orders-v1"}}
	s := server.sink(t, "orders-v2", "orders")
	if err := s.PromoteAlias(context.Background()); err != nil {
		t.Fatalf("promote: %v", err)
	}
	actions := server.recorded()
	if len(actions) != 2 {
		t.Fatalf("expected a removal and an addition together, got %d actions: %+v", len(actions), actions)
	}
	if remove, ok := actions[0]["remove"]; !ok || remove["index"] != "orders-v1" {
		t.Errorf("first action should remove the old index, got %+v", actions[0])
	}
	if add, ok := actions[1]["add"]; !ok || add["index"] != "orders-v2" {
		t.Errorf("second action should add the new index, got %+v", actions[1])
	}
}

// On a first run the alias does not exist yet, which is not an error.
func TestPromoteAliasCreatesAMissingAlias(t *testing.T) {
	server := &aliasServer{}
	s := server.sink(t, "orders-v1", "orders")
	if err := s.PromoteAlias(context.Background()); err != nil {
		t.Fatalf("promote: %v", err)
	}
	actions := server.recorded()
	if len(actions) != 1 {
		t.Fatalf("expected only an addition, got %+v", actions)
	}
	if _, ok := actions[0]["add"]; !ok {
		t.Errorf("expected an add action, got %+v", actions[0])
	}
}

// Running after every completed scan is simpler than deciding whether a rebuild
// happened, so promoting an already-correct alias must do nothing.
func TestPromoteAliasIsANoOpWhenAlreadyCorrect(t *testing.T) {
	server := &aliasServer{targets: []string{"orders-v2"}}
	s := server.sink(t, "orders-v2", "orders")
	if err := s.PromoteAlias(context.Background()); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if actions := server.recorded(); actions != nil {
		t.Errorf("expected no alias change, got %+v", actions)
	}
}

// An alias fanned out over several indices would make reads return duplicates from the
// old and new copies, so every other target is removed.
func TestPromoteAliasRemovesEveryOtherTarget(t *testing.T) {
	server := &aliasServer{targets: []string{"orders-v1", "orders-v2", "orders-v3"}}
	s := server.sink(t, "orders-v3", "orders")
	if err := s.PromoteAlias(context.Background()); err != nil {
		t.Fatalf("promote: %v", err)
	}
	actions := server.recorded()
	var removed []string
	for _, action := range actions {
		if remove, ok := action["remove"]; ok {
			removed = append(removed, remove["index"].(string))
		}
	}
	if len(removed) != 2 {
		t.Fatalf("expected the two stale targets to be removed, got %v", removed)
	}
	for _, index := range removed {
		if index == "orders-v3" {
			t.Error("the index being promoted must not be removed")
		}
	}
}

// Configuring one name for both would leave no way to rebuild, since there would be no
// second index for readers to be moved from.
func TestPromoteAliasRefusesWhenAliasEqualsIndex(t *testing.T) {
	server := &aliasServer{}
	s := server.sink(t, "orders", "orders")
	if err := s.PromoteAlias(context.Background()); err == nil {
		t.Fatal("expected an alias equal to the index name to be refused")
	}
}

func TestPromoteAliasWithoutAnAliasConfiguredDoesNothing(t *testing.T) {
	server := &aliasServer{targets: []string{"orders-v1"}}
	s := server.sink(t, "orders-v1", "")
	if err := s.PromoteAlias(context.Background()); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if actions := server.recorded(); actions != nil {
		t.Errorf("expected nothing to happen without an alias, got %+v", actions)
	}
}

func TestPromoteAliasReportsAFailedMove(t *testing.T) {
	server := &aliasServer{targets: []string{"orders-v1"}, writeStatus: http.StatusNotFound}
	s := server.sink(t, "orders-v2", "orders")
	err := s.PromoteAlias(context.Background())
	if err == nil {
		t.Fatal("expected a failed move to be reported")
	}
	if !strings.Contains(err.Error(), "orders") {
		t.Errorf("error should name the alias, got: %v", err)
	}
}

// An unreadable alias must stop the promotion: acting on an unknown current state could
// leave readers pointed at the wrong index.
func TestPromoteAliasStopsWhenTheAliasCannotBeRead(t *testing.T) {
	server := &aliasServer{readStatus: http.StatusInternalServerError}
	s := server.sink(t, "orders-v2", "orders")
	if err := s.PromoteAlias(context.Background()); err == nil {
		t.Fatal("expected an unreadable alias to stop the promotion")
	}
	if actions := server.recorded(); actions != nil {
		t.Errorf("nothing should have been changed, got %+v", actions)
	}
}
