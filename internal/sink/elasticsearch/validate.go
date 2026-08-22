package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// ValidateMapping compares the live index against the fields this stream will write.
//
// Checked at startup because the alternative is discovering a mismatch at document
// four hundred thousand, having already written three hundred and ninety nine
// thousand documents that need a rebuild to correct. A field declared as the wrong
// type is worse than a missing one: the write succeeds and the value is quietly
// changed.
func (s *Sink) ValidateMapping(ctx context.Context, expected map[string]string) error {
	if len(expected) == 0 {
		return errors.New("elasticsearch: nothing to validate; the stream writes no fields")
	}
	actual, err := s.fetchMapping(ctx)
	if err != nil {
		return err
	}
	var problems []string
	names := make([]string, 0, len(expected))
	for name := range expected {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		want := expected[name]
		field, present := actual[name]
		if !present {
			problems = append(problems, fmt.Sprintf(
				"%s is missing from the index; it would be rejected on write, or indexed with a guessed type", name))
			continue
		}
		if !typesCompatible(want, field) {
			problems = append(problems, fmt.Sprintf(
				"%s is %s in the index but changeflow sends %s", name, field.describe(), want))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("elasticsearch: index %s does not match what stream would write:\n  - %s\n"+
			"regenerate the mapping with `changeflow generate-schema` and apply it to a new index",
			s.opts.Index, strings.Join(problems, "\n  - "))
	}
	return nil
}

type mappedField struct {
	Type    string                 `json:"type"`
	Enabled *bool                  `json:"enabled"`
	Fields  map[string]mappedField `json:"fields"`
}

func (f mappedField) describe() string {
	switch {
	case f.Type != "":
		return f.Type
	case f.Enabled != nil && !*f.Enabled:
		return "a disabled object"
	default:
		return "an object"
	}
}

func typesCompatible(want string, actual mappedField) bool {
	if actual.Type == want {
		return true
	}
	switch want {
	case "object":
		return actual.Type == "" || actual.Type == "object" || actual.Type == "nested" || actual.Type == "flattened"
	case "keyword":
		if actual.Type == "text" {
			for _, sub := range actual.Fields {
				if sub.Type == "keyword" {
					return true
				}
			}
		}
		return false
	case "unsigned_long":
		return false
	case "long":
		return actual.Type == "unsigned_long"
	case "integer":
		return actual.Type == "long" || actual.Type == "unsigned_long"
	case "short":
		return actual.Type == "integer" || actual.Type == "long" || actual.Type == "unsigned_long"
	case "byte":
		return actual.Type == "short" || actual.Type == "integer" || actual.Type == "long" || actual.Type == "unsigned_long"
	case "float":
		return actual.Type == "double"
	default:
		return false
	}
}

func (s *Sink) fetchMapping(ctx context.Context) (map[string]mappedField, error) {
	status, payload, err := s.do(ctx, http.MethodGet, "/"+s.opts.Index+"/_mapping", nil)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch: read the mapping of %s: %w", s.opts.Index, err)
	}
	switch {
	case status == http.StatusNotFound:
		return nil, fmt.Errorf("elasticsearch: index %s does not exist; create it from `changeflow generate-schema` before starting", s.opts.Index)
	case status >= 300:
		return nil, fmt.Errorf("elasticsearch: read the mapping of %s: status %d: %s", s.opts.Index, status, snippet(payload))
	}
	var byIndex map[string]struct {
		Mappings struct {
			Properties map[string]mappedField `json:"properties"`
		} `json:"mappings"`
	}
	if err := json.Unmarshal(payload, &byIndex); err != nil {
		return nil, fmt.Errorf("elasticsearch: cannot read the mapping of %s: %w", s.opts.Index, err)
	}
	if len(byIndex) == 0 {
		return nil, fmt.Errorf("elasticsearch: no mapping returned for %s", s.opts.Index)
	}
	if len(byIndex) > 1 {
		return nil, fmt.Errorf("elasticsearch: %s resolves to %d indices; configure sink.index as a concrete index and use sink.alias for readers",
			s.opts.Index, len(byIndex))
	}
	for _, entry := range byIndex {
		return entry.Mappings.Properties, nil
	}
	return nil, fmt.Errorf("elasticsearch: no mapping returned for %s", s.opts.Index)
}
