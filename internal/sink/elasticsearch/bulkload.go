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
// A missing value means the index declared none, and restoring it clears the override
// rather than writing this build's idea of the default.
type LoadSettings struct {
	refreshInterval json.RawMessage
	applied         bool
}

// Applied reports whether the settings were changed and therefore need restoring.
func (l LoadSettings) Applied() bool { return l.applied }

// BeginBulkLoad turns off refreshing for the duration of a scan, since every refresh
// builds segments over data that is about to grow.
//
// The replica count is left alone: after a kill there is no telling an index that declares
// no replicas from one that had them taken away. Only safe on an index no reader is using,
// which the caller decides — one that never refreshes answers searches with nothing.
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
// Settings first, then a refresh, so a caller moving readers here afterwards never finds
// the index looking empty.
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

// ForceMerge collapses the many small segments a scan leaves behind, since search cost
// scales with their number.
//
// Slow on a large index, so when to spend that is the caller's decision, and a failure
// leaves an index that is slower to search rather than wrong.
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

// do issues one administrative request. Document writes go through post instead, which
// retries and balances; here a clear failure is more useful than a retry.
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
