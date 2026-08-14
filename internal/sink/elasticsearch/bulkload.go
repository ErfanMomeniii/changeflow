package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// LoadSettings records the index setting a scan replaced, so it can be put back.
//
// A missing value means the index did not declare one, and restoring it means clearing
// the override rather than writing a guess: an index whose refresh interval was left at
// the cluster default must go back to the default, not to whatever this build thinks the
// default is.
type LoadSettings struct {
	refreshInterval json.RawMessage
	applied         bool
}

// Applied reports whether the settings were changed and therefore need restoring.
func (l LoadSettings) Applied() bool { return l.applied }

// BeginBulkLoad turns off refreshing for the duration of a scan.
//
// Every refresh builds segments over data that is about to grow, which is work thrown
// away while an index is being filled. Turning it off for the scan and back on afterwards
// is a large part of what a rebuild costs.
//
// Only the refresh interval, and deliberately not the replica count: relaxing a setting
// is only safe if the original can be recovered, and after a scan is killed partway
// through there is no way to tell an index that declares one replica from one that had
// its replicas taken away. The replica count for a rebuild index belongs where it can be
// chosen deliberately, which is the mapping `generate-schema --replicas` produces.
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
			} `json:"index"`
		} `json:"settings"`
	}
	if err := json.Unmarshal(payload, &byIndex); err != nil {
		return previous, fmt.Errorf("elasticsearch: cannot read the settings of %s: %w", s.opts.Index, err)
	}
	for _, entry := range byIndex {
		previous.refreshInterval = entry.Settings.Index.RefreshInterval
		break
	}

	// Reading back the value a killed scan left behind would make it the value to
	// restore, and the index would never refresh again — while an alias was moved to it,
	// so readers would find it empty. Nothing wants an index that never refreshes, so
	// this is treated as a leftover and restored to the cluster default instead.
	if refreshDisabled(previous.refreshInterval) {
		previous.refreshInterval = nil
	}

	if err := s.putSettings(ctx, `{"index":{"refresh_interval":"-1"}}`); err != nil {
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

	body := fmt.Sprintf(`{"index":{"refresh_interval":%s}}`, orNull(previous.refreshInterval))
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
// a freshly scanned index has far more segments than one that grew gradually.
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

// refreshDisabled reports whether a recorded interval is one that stops refreshing.
func refreshDisabled(value json.RawMessage) bool {
	return strings.Trim(string(value), `"`) == "-1"
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
