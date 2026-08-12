package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
)

// PromoteAlias points the read alias at the index this sink writes to, in one atomic
// action.
//
// This is what makes a rebuild safe. A new index is filled by a scan while readers keep
// using the old one, and a single request moves them across: no window where the alias
// resolves to a half-built index, and the previous index is still there to move back to.
//
// It is a no-op when the alias already points only here, so it can run after every
// completed scan rather than being a special case.
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

	// Removals and the addition travel together, so readers never observe the alias
	// pointing at nothing.
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

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.nextAddress()+"/_aliases", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.opts.Username != "" {
		req.SetBasicAuth(s.opts.Username, s.opts.Password)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("elasticsearch: move alias %s: %w", s.opts.Alias, err)
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 300 {
		return fmt.Errorf("elasticsearch: move alias %s to %s: status %d: %s",
			s.opts.Alias, s.opts.Index, resp.StatusCode, snippet(payload))
	}
	return nil
}

// AliasTargets returns the indices the read alias currently resolves to.
func (s *Sink) AliasTargets(ctx context.Context) ([]string, error) {
	return s.aliasTargets(ctx)
}

func (s *Sink) aliasTargets(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.nextAddress()+"/_alias/"+s.opts.Alias, nil)
	if err != nil {
		return nil, err
	}
	if s.opts.Username != "" {
		req.SetBasicAuth(s.opts.Username, s.opts.Password)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch: read alias %s: %w", s.opts.Alias, err)
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(resp.Body)

	switch {
	case resp.StatusCode == http.StatusNotFound:
		// The alias does not exist yet, which is the first-run case.
		return nil, nil
	case resp.StatusCode >= 300:
		return nil, fmt.Errorf("elasticsearch: read alias %s: status %d: %s", s.opts.Alias, resp.StatusCode, snippet(payload))
	}

	var byIndex map[string]any
	if err := json.Unmarshal(payload, &byIndex); err != nil {
		return nil, fmt.Errorf("elasticsearch: cannot read alias %s: %w", s.opts.Alias, err)
	}

	targets := make([]string, 0, len(byIndex))
	for index := range byIndex {
		targets = append(targets, index)
	}
	// Sorted, so logs and errors read the same way between runs.
	sort.Strings(targets)
	return targets, nil
}
