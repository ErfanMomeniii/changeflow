package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// LoadSettings records the index settings a scan replaced, so they can be put back.
//
// A missing value means the index did not declare one, and restoring it means clearing
// the override rather than writing a guess: an index whose refresh interval was left at
// the cluster default must go back to the default, not to whatever this build thinks the
// default is.
type LoadSettings struct {
	refreshInterval json.RawMessage
	replicas        json.RawMessage
	applied         bool
}

// Applied reports whether the settings were changed and therefore need restoring.
func (l LoadSettings) Applied() bool { return l.applied }

// BeginBulkLoad turns off refreshing and replication for the duration of a scan.
//
// Both cost work per document that is wasted while an index is being filled: every
// refresh builds segments over data that is about to grow, and every replica copies rows
// that would be copied again by the next merge. Turning them off for the scan and back on
// afterwards is the difference between a rebuild that takes an hour and one that takes
// several.
//
// Only safe on an index no reader is using, since an index that never refreshes returns
// nothing to searches. The caller decides that; this only reports what it changed.
func (s *Sink) BeginBulkLoad(ctx context.Context) (LoadSettings, error) {
	var previous LoadSettings

	status, payload, err := s.do(ctx, http.MethodGet, "/"+s.opts.Index+"/_settings", nil)
	if err != nil {
		return previous, fmt.Errorf("elasticsearch: read the settings of %s: %w", s.opts.Index, err)
	}
	if status >= 300 {
		return previous, fmt.Errorf("elasticsearch: read the settings of %s: status %d: %s",
			s.opts.Index, status, snippet(payload))
	}

	// Keyed by concrete index name, which is not necessarily the name requested.
	var byIndex map[string]struct {
		Settings struct {
			Index struct {
				RefreshInterval json.RawMessage `json:"refresh_interval"`
				Replicas        json.RawMessage `json:"number_of_replicas"`
			} `json:"index"`
		} `json:"settings"`
	}
	if err := json.Unmarshal(payload, &byIndex); err != nil {
		return previous, fmt.Errorf("elasticsearch: cannot read the settings of %s: %w", s.opts.Index, err)
	}
	for _, entry := range byIndex {
		previous.refreshInterval = entry.Settings.Index.RefreshInterval
		previous.replicas = entry.Settings.Index.Replicas
		break
	}

	if err := s.putSettings(ctx, `{"index":{"refresh_interval":"-1","number_of_replicas":0}}`); err != nil {
		return previous, err
	}
	previous.applied = true
	return previous, nil
}

// EndBulkLoad puts back what BeginBulkLoad changed and makes the scanned rows visible.
//
// The order matters: settings first, then a refresh, so that by the time this returns a
// search of the index sees everything written. Anything moving readers to this index can
// then do so without a window where it looks empty.
func (s *Sink) EndBulkLoad(ctx context.Context, previous LoadSettings) error {
	if !previous.applied {
		return nil
	}

	body := fmt.Sprintf(`{"index":{"refresh_interval":%s,"number_of_replicas":%s}}`,
		orNull(previous.refreshInterval), orNull(previous.replicas))
	if err := s.putSettings(ctx, body); err != nil {
		return err
	}

	status, payload, err := s.do(ctx, http.MethodPost, "/"+s.opts.Index+"/_refresh", nil)
	if err != nil {
		return fmt.Errorf("elasticsearch: refresh %s: %w", s.opts.Index, err)
	}
	if status >= 300 {
		return fmt.Errorf("elasticsearch: refresh %s: status %d: %s", s.opts.Index, status, snippet(payload))
	}
	return nil
}

// ForceMerge collapses the many small segments a scan leaves behind.
//
// Worth doing once after a bulk load, because search cost scales with segment count, and
// a freshly scanned index has far more segments than one that grew gradually. Merging
// while the index still has no replicas also means the merged result is copied once
// rather than the pre-merge segments being copied and then merged again on every replica.
//
// This can take a long time on a large index, so it is the caller's decision when to
// spend that, and a failure is not fatal: an unmerged index is slower to search, not
// wrong.
func (s *Sink) ForceMerge(ctx context.Context) error {
	status, payload, err := s.do(ctx, http.MethodPost, "/"+s.opts.Index+"/_forcemerge?max_num_segments=1", nil)
	if err != nil {
		return fmt.Errorf("elasticsearch: merge segments of %s: %w", s.opts.Index, err)
	}
	if status >= 300 {
		return fmt.Errorf("elasticsearch: merge segments of %s: status %d: %s", s.opts.Index, status, snippet(payload))
	}
	return nil
}

func (s *Sink) putSettings(ctx context.Context, body string) error {
	status, payload, err := s.do(ctx, http.MethodPut, "/"+s.opts.Index+"/_settings", []byte(body))
	if err != nil {
		return fmt.Errorf("elasticsearch: change the settings of %s: %w", s.opts.Index, err)
	}
	if status >= 300 {
		return fmt.Errorf("elasticsearch: change the settings of %s to %s: status %d: %s",
			s.opts.Index, body, status, snippet(payload))
	}
	return nil
}

// orNull renders a recorded setting, or null to clear the override when the index never
// declared one.
func orNull(value json.RawMessage) string {
	if len(value) == 0 {
		return "null"
	}
	return string(value)
}

// do issues one administrative request. Writes go through post, which retries and
// balances; these are single management calls where a clear failure is more useful than a
// retry.
func (s *Sink) do(ctx context.Context, method, path string, body []byte) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, s.nextAddress()+path, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if s.opts.Username != "" {
		req.SetBasicAuth(s.opts.Username, s.opts.Password)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, payload, nil
}
