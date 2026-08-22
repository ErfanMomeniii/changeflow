package elasticsearch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func mappingServer(t *testing.T, status int, body string) *Sink {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/_mapping") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	s, err := New(Options{Addresses: []string{server.URL}, Index: "orders-v1"})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func mappingBody(properties string) string {
	return `{"orders-v1":{"mappings":{"properties":{` + properties + `}}}}`
}

func TestValidateAcceptsAMatchingMapping(t *testing.T) {
	s := mappingServer(t, http.StatusOK, mappingBody(`
		"id":           {"type":"unsigned_long"},
		"status":       {"type":"keyword"},
		"total_amount": {"type":"keyword"},
		"placed_at":    {"type":"date"},
		"metadata":     {"type":"object","enabled":false}
	`))
	err := s.ValidateMapping(context.Background(), map[string]string{
		"id": "unsigned_long", "status": "keyword", "total_amount": "keyword",
		"placed_at": "date", "metadata": "object",
	})
	if err != nil {
		t.Fatalf("expected the mapping to be accepted: %v", err)
	}
}

// A missing field is either rejected on write, under a strict mapping, or indexed with
// a guessed type. Both are worth refusing to start over.
func TestValidateReportsMissingFields(t *testing.T) {
	s := mappingServer(t, http.StatusOK, mappingBody(`"id": {"type":"unsigned_long"}`))
	err := s.ValidateMapping(context.Background(), map[string]string{
		"id": "unsigned_long", "status": "keyword", "total_amount": "keyword",
	})
	if err == nil {
		t.Fatal("expected missing fields to be reported")
	}
	for _, want := range []string{"status", "total_amount"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should name %s, got: %v", want, err)
		}
	}
}

// The case that matters most: a signed long cannot hold the upper half of an unsigned
// range, and the write would succeed while wrapping the value.
func TestValidateRefusesASignedLongForAnUnsignedField(t *testing.T) {
	s := mappingServer(t, http.StatusOK, mappingBody(`"id": {"type":"long"}`))
	err := s.ValidateMapping(context.Background(), map[string]string{"id": "unsigned_long"})
	if err == nil {
		t.Fatal("expected long to be refused where unsigned_long is required")
	}
	if !strings.Contains(err.Error(), "unsigned_long") || !strings.Contains(err.Error(), "long") {
		t.Errorf("error should name both types, got: %v", err)
	}
}

func TestValidateReportsATypeMismatch(t *testing.T) {
	s := mappingServer(t, http.StatusOK, mappingBody(`"total_amount": {"type":"double"}`))
	err := s.ValidateMapping(context.Background(), map[string]string{"total_amount": "keyword"})
	if err == nil {
		t.Fatal("expected a double to be refused where an exact keyword is required")
	}
}

// A keyword with a text sibling is the usual way to make a value both searchable and
// exact, so refusing it would make this check an obstacle rather than a safeguard.
func TestValidateAcceptsAMultiFieldKeyword(t *testing.T) {
	s := mappingServer(t, http.StatusOK, mappingBody(`
		"status": {"type":"text","fields":{"keyword":{"type":"keyword"}}}
	`))
	if err := s.ValidateMapping(context.Background(), map[string]string{"status": "keyword"}); err != nil {
		t.Fatalf("expected a text field with a keyword sibling to be accepted: %v", err)
	}
}

func TestValidateRefusesTextWithoutAKeywordSibling(t *testing.T) {
	s := mappingServer(t, http.StatusOK, mappingBody(`"status": {"type":"text"}`))
	if err := s.ValidateMapping(context.Background(), map[string]string{"status": "keyword"}); err == nil {
		t.Fatal("expected plain text to be refused where an exact value is required")
	}
}

// Elasticsearch omits the type for a plain object, so an absent type must not read as
// a mismatch.
func TestValidateAcceptsObjectVariants(t *testing.T) {
	for _, declared := range []string{
		`"metadata": {"type":"object"}`,
		`"metadata": {"type":"object","enabled":false}`,
		`"metadata": {"properties":{"coupon":{"type":"keyword"}}}`,
		`"metadata": {"type":"flattened"}`,
	} {
		t.Run(declared, func(t *testing.T) {
			s := mappingServer(t, http.StatusOK, mappingBody(declared))
			if err := s.ValidateMapping(context.Background(), map[string]string{"metadata": "object"}); err != nil {
				t.Errorf("expected %s to be accepted: %v", declared, err)
			}
		})
	}
}

// A wider numeric type still holds what we send, so it is accepted rather than
// treated as drift.
func TestValidateAcceptsWiderNumericTypes(t *testing.T) {
	for _, tc := range []struct{ declared, expected string }{
		{`"n": {"type":"long"}`, "integer"},
		{`"n": {"type":"integer"}`, "short"},
		{`"n": {"type":"short"}`, "byte"},
		{`"n": {"type":"double"}`, "float"},
		{`"n": {"type":"unsigned_long"}`, "long"},
	} {
		t.Run(tc.expected+"_in_"+tc.declared, func(t *testing.T) {
			s := mappingServer(t, http.StatusOK, mappingBody(tc.declared))
			if err := s.ValidateMapping(context.Background(), map[string]string{"n": tc.expected}); err != nil {
				t.Errorf("expected %s to hold %s: %v", tc.declared, tc.expected, err)
			}
		})
	}
}

// Fields the index has and we do not send are not our business.
func TestValidateIgnoresExtraFieldsInTheIndex(t *testing.T) {
	s := mappingServer(t, http.StatusOK, mappingBody(`
		"id":        {"type":"unsigned_long"},
		"legacy":    {"type":"keyword"},
		"computed":  {"type":"double"}
	`))
	if err := s.ValidateMapping(context.Background(), map[string]string{"id": "unsigned_long"}); err != nil {
		t.Fatalf("extra fields should not fail validation: %v", err)
	}
}

func TestValidateReportsAMissingIndexClearly(t *testing.T) {
	s := mappingServer(t, http.StatusNotFound, `{"error":{"type":"index_not_found_exception"}}`)
	err := s.ValidateMapping(context.Background(), map[string]string{"id": "unsigned_long"})
	if err == nil {
		t.Fatal("expected a missing index to fail")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("error should say the index is missing, got: %v", err)
	}
	if !strings.Contains(err.Error(), "generate-schema") {
		t.Errorf("error should suggest how to create it, got: %v", err)
	}
}

// A name resolving to several indices is an alias, and writing through one is
// ambiguous, so it must be refused rather than validated against an arbitrary member.
func TestValidateRefusesANameResolvingToSeveralIndices(t *testing.T) {
	s := mappingServer(t, http.StatusOK, `{
		"orders-v1": {"mappings":{"properties":{"id":{"type":"unsigned_long"}}}},
		"orders-v2": {"mappings":{"properties":{"id":{"type":"unsigned_long"}}}}
	}`)
	err := s.ValidateMapping(context.Background(), map[string]string{"id": "unsigned_long"})
	if err == nil {
		t.Fatal("expected an ambiguous name to be refused")
	}
	if !strings.Contains(err.Error(), "alias") {
		t.Errorf("error should explain the alias distinction, got: %v", err)
	}
}

func TestValidateFailsOnAnUnreadableResponse(t *testing.T) {
	s := mappingServer(t, http.StatusOK, `not json`)
	if err := s.ValidateMapping(context.Background(), map[string]string{"id": "unsigned_long"}); err == nil {
		t.Fatal("expected an unreadable mapping to fail rather than be assumed correct")
	}
}

func TestValidateRequiresSomethingToCheck(t *testing.T) {
	s := mappingServer(t, http.StatusOK, mappingBody(`"id": {"type":"unsigned_long"}`))
	if err := s.ValidateMapping(context.Background(), nil); err == nil {
		t.Fatal("expected validating an empty field set to be refused")
	}
}

// Every problem at once: fixing a mapping one error per restart is a slow way to work.
func TestValidateReportsEveryProblemTogether(t *testing.T) {
	s := mappingServer(t, http.StatusOK, mappingBody(`
		"id":     {"type":"long"},
		"status": {"type":"text"}
	`))
	err := s.ValidateMapping(context.Background(), map[string]string{
		"id": "unsigned_long", "status": "keyword", "missing": "date",
	})
	if err == nil {
		t.Fatal("expected problems to be reported")
	}
	for _, want := range []string{"id", "status", "missing"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("expected the report to mention %s, got: %v", want, err)
		}
	}
}
