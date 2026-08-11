package config

import (
	"math"
	"strings"
	"testing"
	"time"
)

const minimalYAML = `
source:
  dsn: "cdc:cdc@tcp(127.0.0.1:3306)/"
  server_id: 1001
checkpoint:
  dsn: "cdc:cdc@tcp(127.0.0.1:3306)/changeflow_meta"
streams:
  orders_to_es:
    table: shop.orders
    sink:
      type: elasticsearch
      addresses: ["http://localhost:9200"]
      index: orders-v1
    mapping:
      key: [id]
`

func load(t *testing.T, yaml string) (*Config, error) {
	t.Helper()
	return Parse([]byte(yaml), func(string) (string, bool) { return "", false })
}

func mustLoad(t *testing.T, yaml string) *Config {
	t.Helper()
	cfg, err := load(t, yaml)
	if err != nil {
		t.Fatalf("expected the config to load, got: %v", err)
	}
	return cfg
}

func TestParseMinimalConfig(t *testing.T) {
	cfg := mustLoad(t, minimalYAML)

	if len(cfg.Streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(cfg.Streams))
	}
	s := cfg.Streams["orders_to_es"]
	if s.Name != "orders_to_es" {
		t.Errorf("stream name not taken from the map key: %q", s.Name)
	}
	if s.Schema() != "shop" || s.TableName() != "orders" {
		t.Errorf("table not split: schema=%q table=%q", s.Schema(), s.TableName())
	}
}

func TestDefaultsFillOmittedValues(t *testing.T) {
	cfg := mustLoad(t, minimalYAML)
	s := cfg.Streams["orders_to_es"]

	for _, tc := range []struct {
		name string
		got  any
		want any
	}{
		{"source.time_zone", cfg.Source.TimeZone, "UTC"},
		{"source.heartbeat_period", cfg.Source.HeartbeatPeriod.Duration(), 5 * time.Second},
		{"source.read_timeout", cfg.Source.ReadTimeout.Duration(), 90 * time.Second},
		{"checkpoint.table", cfg.Checkpoint.Table, "changeflow_checkpoints"},
		{"runtime.buffer_size", cfg.Runtime.BufferSize, 8192},
		{"runtime.shutdown_grace", cfg.Runtime.ShutdownGrace.Duration(), 30 * time.Second},
		{"batch.max_rows", s.Batch.MaxRows, 1000},
		{"batch.max_bytes", s.Batch.MaxBytes.Bytes(), uint64(5 << 20)},
		{"batch.flush_interval", s.Batch.FlushInterval.Duration(), 500 * time.Millisecond},
		{"sink.workers", s.Sink.Workers, 4},
		{"snapshot.enabled", s.Snapshot.Enabled, true},
		{"snapshot.chunk_size", s.Snapshot.ChunkSize, 5000},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %v, want %v", tc.name, tc.got, tc.want)
		}
	}
}

func TestExplicitValuesOverrideDefaults(t *testing.T) {
	cfg := mustLoad(t, minimalYAML+`
    batch:
      max_rows: 50000
      max_bytes: 64MiB
      flush_interval: 2s
`)
	s := cfg.Streams["orders_to_es"]

	if s.Batch.MaxRows != 50000 {
		t.Errorf("max_rows = %d, want 50000", s.Batch.MaxRows)
	}
	if s.Batch.MaxBytes.Bytes() != 64<<20 {
		t.Errorf("max_bytes = %s, want 64MiB", s.Batch.MaxBytes)
	}
	if s.Batch.FlushInterval.Duration() != 2*time.Second {
		t.Errorf("flush_interval = %v, want 2s", s.Batch.FlushInterval.Duration())
	}
}

func TestEnvExpansion(t *testing.T) {
	env := map[string]string{"MYSQL_DSN": "cdc:secret@tcp(db:3306)/", "ES_URL": "http://es:9200"}
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }

	cfg, err := Parse([]byte(`
source:
  dsn: "${MYSQL_DSN}"
  server_id: 1001
checkpoint:
  dsn: "${MYSQL_DSN}changeflow_meta"
streams:
  orders_to_es:
    table: shop.orders
    sink:
      type: elasticsearch
      addresses: ["${ES_URL}"]
      index: orders-v1
`), lookup)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if cfg.Source.DSN != "cdc:secret@tcp(db:3306)/" {
		t.Errorf("dsn not expanded: %q", cfg.Source.DSN)
	}
	if got := cfg.Streams["orders_to_es"].Sink.Addresses[0]; got != "http://es:9200" {
		t.Errorf("address not expanded: %q", got)
	}
}

// Falling back to an empty value would produce a config that looks valid and then
// fails to connect, so a missing variable is an error that names it.
func TestMissingEnvVarIsReportedByName(t *testing.T) {
	_, err := load(t, `
source:
  dsn: "${MYSQL_DSN}"
  server_id: 1001
checkpoint:
  dsn: "x"
streams:
  s:
    table: a.b
    sink: {type: elasticsearch, addresses: ["x"], index: i}
`)
	if err == nil {
		t.Fatal("expected an error for an unset variable")
	}
	if !strings.Contains(err.Error(), "MYSQL_DSN") {
		t.Fatalf("error should name the missing variable, got: %v", err)
	}
}

func TestEnvDefaultSyntax(t *testing.T) {
	cfg, err := Parse([]byte(strings.Replace(minimalYAML,
		`dsn: "cdc:cdc@tcp(127.0.0.1:3306)/"`,
		`dsn: "${MYSQL_DSN:-cdc:cdc@tcp(fallback:3306)/}"`, 1)),
		func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Source.DSN != "cdc:cdc@tcp(fallback:3306)/" {
		t.Fatalf("default not applied: %q", cfg.Source.DSN)
	}
}

// A typo in a field name must fail rather than being silently ignored, which
// would leave the operator believing a setting took effect.
func TestUnknownFieldIsRejected(t *testing.T) {
	_, err := load(t, minimalYAML+`
    batch:
      max_row: 100
`)
	if err == nil {
		t.Fatal("expected an error for the misspelled field max_row")
	}
	if !strings.Contains(err.Error(), "max_row") {
		t.Fatalf("error should name the unknown field, got: %v", err)
	}
}

func TestValidationRejectsBadConfigurations(t *testing.T) {
	for _, tc := range []struct {
		name     string
		yaml     string
		contains string
	}{
		{
			name:     "no streams",
			yaml:     "source:\n  dsn: x\n  server_id: 1\ncheckpoint:\n  dsn: y\nstreams: {}\n",
			contains: "streams",
		},
		{
			name:     "missing source dsn",
			yaml:     strings.Replace(minimalYAML, `dsn: "cdc:cdc@tcp(127.0.0.1:3306)/"`, `dsn: ""`, 1),
			contains: "source.dsn",
		},
		{
			name:     "zero server id",
			yaml:     strings.Replace(minimalYAML, "server_id: 1001", "server_id: 0", 1),
			contains: "server_id",
		},
		{
			name:     "table without a database",
			yaml:     strings.Replace(minimalYAML, "table: shop.orders", "table: orders", 1),
			contains: "database.table",
		},
		{
			name:     "unknown sink type",
			yaml:     strings.Replace(minimalYAML, "type: elasticsearch", "type: mongodb", 1),
			contains: "mongodb",
		},
		{
			name:     "elasticsearch without an index",
			yaml:     strings.Replace(minimalYAML, "index: orders-v1", "", 1),
			contains: "index",
		},
		{
			name:     "elasticsearch without addresses",
			yaml:     strings.Replace(minimalYAML, `addresses: ["http://localhost:9200"]`, "", 1),
			contains: "addresses",
		},
		{
			name:     "zero batch rows",
			yaml:     minimalYAML + "    batch:\n      max_rows: 0\n",
			contains: "max_rows",
		},
		{
			name:     "zero workers",
			yaml:     strings.Replace(minimalYAML, "      index: orders-v1", "      index: orders-v1\n      workers: 0", 1),
			contains: "workers",
		},
		{
			name:     "negative buffer size",
			yaml:     minimalYAML + "runtime:\n  buffer_size: 0\n",
			contains: "buffer_size",
		},
		{
			name:     "unknown time zone",
			yaml:     strings.Replace(minimalYAML, "  server_id: 1001", "  server_id: 1001\n  time_zone: Mars/Olympus", 1),
			contains: "time_zone",
		},
		{
			name:     "unknown on_zero_date policy",
			yaml:     minimalYAML + "      on_zero_date: whatever\n",
			contains: "on_zero_date",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(t, tc.yaml)
			if err == nil {
				t.Fatalf("expected an error mentioning %q", tc.contains)
			}
			if !strings.Contains(err.Error(), tc.contains) {
				t.Fatalf("error should mention %q, got: %v", tc.contains, err)
			}
		})
	}
}

// The checkpoint table's stream column bounds this, and finding out at the first
// write rather than at startup would mean a stream that can never checkpoint.
func TestStreamNameLengthIsBounded(t *testing.T) {
	long := strings.Repeat("s", 49)
	_, err := load(t, strings.Replace(minimalYAML, "orders_to_es:", long+":", 1))
	if err == nil {
		t.Fatal("expected an over-long stream name to be rejected")
	}
	if !strings.Contains(err.Error(), "48") {
		t.Fatalf("error should state the limit, got: %v", err)
	}
}

func TestStreamNameCharactersAreBounded(t *testing.T) {
	for _, name := range []string{"orders to es", "orders/es", "orders:es", ""} {
		yaml := strings.Replace(minimalYAML, "orders_to_es:", `"`+name+`":`, 1)
		if _, err := load(t, yaml); err == nil {
			t.Errorf("expected stream name %q to be rejected", name)
		}
	}
}

func TestIncludeAndExcludeAreMutuallyExclusive(t *testing.T) {
	_, err := load(t, minimalYAML+`
      include: [id, status]
      exclude: [internal_note]
`)
	if err == nil {
		t.Fatal("expected include and exclude together to be rejected")
	}
	if !strings.Contains(err.Error(), "include") || !strings.Contains(err.Error(), "exclude") {
		t.Fatalf("error should mention both, got: %v", err)
	}
}

// Two source columns renamed onto one target would silently drop a field.
func TestRenameCollisionIsRejected(t *testing.T) {
	_, err := load(t, minimalYAML+`
      rename:
        total_amount: total
        grand_total: total
`)
	if err == nil {
		t.Fatal("expected two columns renamed to the same target to be rejected")
	}
	if !strings.Contains(err.Error(), "total") {
		t.Fatalf("error should name the colliding target, got: %v", err)
	}
}

func TestRenameOntoAnotherIncludedColumnIsRejected(t *testing.T) {
	_, err := load(t, minimalYAML+`
      include: [id, status, total_amount]
      rename:
        total_amount: status
`)
	if err == nil {
		t.Fatal("expected a rename onto an included column to be rejected")
	}
}

// Reporting one problem at a time turns fixing a config into a guessing game.
func TestValidationReportsEveryProblemAtOnce(t *testing.T) {
	_, err := load(t, `
source:
  dsn: ""
  server_id: 0
checkpoint:
  dsn: ""
streams:
  bad_stream:
    table: orders
    sink:
      type: mongodb
`)
	if err == nil {
		t.Fatal("expected errors")
	}
	msg := err.Error()
	for _, want := range []string{"source.dsn", "server_id", "checkpoint.dsn", "database.table", "mongodb"} {
		if !strings.Contains(msg, want) {
			t.Errorf("expected the report to mention %q; got:\n%s", want, msg)
		}
	}
}

func TestClickHouseSinkRequirements(t *testing.T) {
	incomplete := `
source:
  dsn: x
  server_id: 1
checkpoint:
  dsn: y
streams:
  orders_to_clickhouse:
    table: shop.orders
    sink:
      type: clickhouse
`
	_, err := load(t, incomplete)
	if err == nil {
		t.Fatal("expected a ClickHouse sink without dsn and table to be rejected")
	}
	for _, want := range []string{"dsn", "table"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q, got: %v", want, err)
		}
	}

	complete := `
source:
  dsn: x
  server_id: 1
checkpoint:
  dsn: y
streams:
  orders_to_clickhouse:
    table: shop.orders
    sink:
      type: clickhouse
      dsn: "http://localhost:8123"
      table: analytics.orders
      workers: 2
`
	cfg := mustLoad(t, complete)
	if got := cfg.Streams["orders_to_clickhouse"].Sink.Workers; got != 2 {
		t.Fatalf("workers = %d, want 2", got)
	}
}

// Elasticsearch options on a ClickHouse sink are almost certainly a mistake, and
// silently ignoring them hides it.
func TestSinkOptionsMustMatchSinkType(t *testing.T) {
	_, err := load(t, `
source:
  dsn: x
  server_id: 1
checkpoint:
  dsn: y
streams:
  orders_to_clickhouse:
    table: shop.orders
    sink:
      type: clickhouse
      dsn: "http://localhost:8123"
      table: analytics.orders
      index: orders-v1
`)
	if err == nil {
		t.Fatal("expected an Elasticsearch-only option on a ClickHouse sink to be rejected")
	}
	if !strings.Contains(err.Error(), "index") {
		t.Fatalf("error should name the misplaced option, got: %v", err)
	}
}

// An explicit false must be honoured, which a plain bool defaulting to true could
// not express.
func TestSnapshotCanBeDisabledExplicitly(t *testing.T) {
	cfg := mustLoad(t, minimalYAML+`
    snapshot:
      enabled: false
`)
	if cfg.Streams["orders_to_es"].Snapshot.Enabled {
		t.Fatal("snapshot.enabled: false was ignored")
	}
}

func TestStreamNamesAreSortedForDeterministicOutput(t *testing.T) {
	cfg := mustLoad(t, `
source:
  dsn: x
  server_id: 1
checkpoint:
  dsn: y
streams:
  zebra:
    table: a.b
    sink: {type: elasticsearch, addresses: [x], index: i}
  alpha:
    table: a.c
    sink: {type: elasticsearch, addresses: [x], index: j}
  middle:
    table: a.d
    sink: {type: elasticsearch, addresses: [x], index: k}
`)

	got := cfg.StreamNames()
	want := []string{"alpha", "middle", "zebra"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("StreamNames() = %v, want %v", got, want)
		}
	}
}

func TestMemoryEstimateAccountsForBuffersAndBatches(t *testing.T) {
	cfg := mustLoad(t, minimalYAML)
	s := cfg.Streams["orders_to_es"]

	want := uint64(cfg.Runtime.BufferSize)*cfg.Runtime.AssumedRowBytes.Bytes() +
		uint64(s.Sink.Workers)*s.Batch.MaxBytes.Bytes()

	if got := cfg.EstimatedMemory(); got != want {
		t.Fatalf("EstimatedMemory() = %d, want %d", got, want)
	}
}

// A config whose bound exceeds the process memory limit is a scheduled crash, so
// it is better refused at startup than discovered under load.
func TestConfigExceedingMemoryLimitIsRejected(t *testing.T) {
	cfg := mustLoad(t, minimalYAML)

	tiny := int64(cfg.EstimatedMemory() / 2)
	if err := cfg.CheckMemoryLimit(tiny); err == nil {
		t.Fatal("expected a config needing more memory than the limit to be rejected")
	}

	if err := cfg.CheckMemoryLimit(math.MaxInt64); err != nil {
		t.Fatalf("expected no error when no meaningful limit is set: %v", err)
	}
}

func TestStreamLookupReportsUnknownName(t *testing.T) {
	cfg := mustLoad(t, minimalYAML)

	if _, err := cfg.Stream("orders_to_es"); err != nil {
		t.Fatalf("expected a configured stream to be found: %v", err)
	}
	_, err := cfg.Stream("not_configured")
	if err == nil {
		t.Fatal("expected an error for an unknown stream")
	}
	// The message should help, by listing what does exist.
	if !strings.Contains(err.Error(), "orders_to_es") {
		t.Fatalf("error should list configured streams, got: %v", err)
	}
}
