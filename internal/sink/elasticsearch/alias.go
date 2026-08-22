package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
)

// PromoteAlias points the read alias at the index this sink writes to.
//
// One request, so readers never see the alias resolve to nothing or to a half-built
// index, and the index they came from is still there to move back to. A no-op when the
// alias already points only here, so it can run after every completed scan.
func (s *Sink) PromoteAlias(ctx context.Context) error {
	if s.opts.Alias == "" {
		return nil
	}
	if s.opts.Alias == s.opts.Index {
		return fmt.Errorf("elasticsearch: alias %q is also the index name; readers and writers need separate names for a rebuild to be possible", s.opts.Alias)
	}
	current, err := s.aliasTargets(ctx)
	if err != nil {
		return err
	}
	if len(current) == 1 && current[0] == s.opts.Index {
		return nil
	}
	actions := make([]map[string]any, 0, len(current)+1)
	for _, index := range current {
		if index == s.opts.Index {
			continue
		}
		actions = append(actions, map[string]any{
			"remove": map[string]any{"index": index, "alias": s.opts.Alias},
		})
	}
	actions = append(actions, map[string]any{
		"add": map[string]any{"index": s.opts.Index, "alias": s.opts.Alias, "is_write_index": true},
	})
	body, err := json.Marshal(map[string]any{"actions": actions})
	if err != nil {
		return fmt.Errorf("elasticsearch: encode alias actions: %w", err)
	}
	status, payload, err := s.do(ctx, http.MethodPost, "/_aliases", body)
	if err != nil {
		return fmt.Errorf("elasticsearch: move alias %s: %w", s.opts.Alias, err)
	}
	if status >= 300 {
		return fmt.Errorf("elasticsearch: move alias %s to %s: status %d: %s",
			s.opts.Alias, s.opts.Index, status, snippet(payload))
	}
	return nil
}

// AliasTargets returns the indices the read alias currently resolves to.
func (s *Sink) AliasTargets(ctx context.Context) ([]string, error) {
	return s.aliasTargets(ctx)
}

func (s *Sink) aliasTargets(ctx context.Context) ([]string, error) {
	status, payload, err := s.do(ctx, http.MethodGet, "/_alias/"+s.opts.Alias, nil)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch: read alias %s: %w", s.opts.Alias, err)
	}
	switch {
	case status == http.StatusNotFound:
		return nil, nil
	case status >= 300:
		return nil, fmt.Errorf("elasticsearch: read alias %s: status %d: %s", s.opts.Alias, status, snippet(payload))
	}
	var byIndex map[string]any
	if err := json.Unmarshal(payload, &byIndex); err != nil {
		return nil, fmt.Errorf("elasticsearch: cannot read alias %s: %w", s.opts.Alias, err)
	}
	targets := make([]string, 0, len(byIndex))
	for index := range byIndex {
		targets = append(targets, index)
	}
	sort.Strings(targets)
	return targets, nil
}
