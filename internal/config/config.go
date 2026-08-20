// Package config loads and validates changeflow's configuration.
//
// Validation is deliberately strict and exhaustive: an unknown field is an error
// rather than a setting silently ignored, and every problem in a file is reported
// at once so fixing a config is not a guessing game. Anything that can be checked
// before connecting is checked here, because the alternative is discovering it
// after several hundred thousand documents have been written.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// MaxStreamNameLen bounds a stream name so it fits the checkpoint table's column.
// Exceeding it would produce a stream that runs but can never checkpoint.
const MaxStreamNameLen = 48

// Sink types changeflow can write to.
const (
	SinkElasticsearch = "elasticsearch"
	SinkClickHouse    = "clickhouse"
)

// MetricsDisabled is the metrics_addr value that switches the endpoint off.
const MetricsDisabled = "off"

// Config is a whole configuration file.
type Config struct {
	Source     Source             `yaml:"source"`
	Checkpoint CheckpointStore    `yaml:"checkpoint"`
	Runtime    Runtime            `yaml:"runtime"`
	Streams    map[string]*Stream `yaml:"streams"`
}

// Source describes the MySQL server to replicate from.
type Source struct {
	DSN string `yaml:"dsn"`
	// ServerID identifies us to the master as a replica, and must not collide with
	// any other replica or with the master itself.
	ServerID uint32 `yaml:"server_id"`
	// SnapshotDSN optionally points table scans at a different server, so a
	// backfill can read from a replica while the binlog comes from the master.
	SnapshotDSN     string   `yaml:"snapshot_dsn"`
	TimeZone        string   `yaml:"time_zone"`
	HeartbeatPeriod Duration `yaml:"heartbeat_period"`
	ReadTimeout     Duration `yaml:"read_timeout"`
	TLS             string   `yaml:"tls"`
}

// CheckpointStore describes where positions are kept.
type CheckpointStore struct {
	DSN   string `yaml:"dsn"`
	Table string `yaml:"table"`
}

// Runtime holds process-wide settings.
type Runtime struct {
	// BufferSizeRaw is nil when omitted, which is how an explicit zero stays
	// distinguishable from an absent value and can be reported as a mistake.
	BufferSizeRaw *int     `yaml:"buffer_size"`
	ShutdownGrace Duration `yaml:"shutdown_grace"`
	// MetricsAddr is where metrics and health are served. The value "off" disables
	// them, which an empty value cannot express because it takes the default.
	MetricsAddr string `yaml:"metrics_addr"`
	// AssumedRowBytes is the per-row estimate the memory projection uses, for tables
	// whose rows are much larger or smaller than typical.
	AssumedRowBytes ByteSize `yaml:"assumed_row_bytes"`

	// BufferSize is the resolved value, filled once defaults are applied.
	BufferSize int `yaml:"-"`
}

// Stream is one table-to-sink route.
type Stream struct {
	// Name comes from the map key, not the file body.
	Name string `yaml:"-"`

	Table    string   `yaml:"table"`
	Snapshot Snapshot `yaml:"snapshot"`
	Batch    Batch    `yaml:"batch"`
	Sink     Sink     `yaml:"sink"`
	Mapping  Mapping  `yaml:"mapping"`
}

// Snapshot controls the one-time backfill of rows that already exist.
type Snapshot struct {
	// Snapshots are on unless switched off, so presence has to be tracked: a plain
	// bool defaulting to true could never express "enabled: false".
	EnabledRaw           *bool `yaml:"enabled"`
	ChunkSizeRaw         *int  `yaml:"chunk_size"`
	MaxRateRowsPerSecRaw *int  `yaml:"max_rate_rows_per_sec"`
	// Resolved values, filled once defaults are applied.
	Enabled           bool `yaml:"-"`
	ChunkSize         int  `yaml:"-"`
	MaxRateRowsPerSec int  `yaml:"-"`
}

// Batch controls how writes are grouped.
type Batch struct {
	MaxRowsRaw    *int      `yaml:"max_rows"`
	MaxBytesRaw   *ByteSize `yaml:"max_bytes"`
	FlushInterval Duration  `yaml:"flush_interval"`

	// Resolved values, filled once defaults are applied.
	MaxRows  int      `yaml:"-"`
	MaxBytes ByteSize `yaml:"-"`
}

// Sink describes a destination.
type Sink struct {
	Type       string `yaml:"type"`
	WorkersRaw *int   `yaml:"workers"`

	// Workers is the resolved value, filled once defaults are applied.
	Workers int `yaml:"-"`

	// Elasticsearch
	Addresses []string `yaml:"addresses"`
	Index     string   `yaml:"index"`
	Alias     string   `yaml:"alias"`

	// ClickHouse
	DSN   string `yaml:"dsn"`
	Table string `yaml:"table"`
}

// Mapping selects and reshapes the columns a stream writes.
type Mapping struct {
	Key        []string          `yaml:"key"`
	Include    []string          `yaml:"include"`
	Exclude    []string          `yaml:"exclude"`
	Rename     map[string]string `yaml:"rename"`
	OnZeroDate string            `yaml:"on_zero_date"`
}

var (
	streamNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
	envRefPattern     = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}`)
)

// Load reads and validates a configuration file, expanding environment
// references from the process environment.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	return Parse(raw, os.LookupEnv)
}

// Parse expands environment references, decodes, applies defaults, and validates.
// The lookup function is a parameter so tests need no process environment.
func Parse(raw []byte, lookup func(string) (string, bool)) (*Config, error) {
	expanded, err := expandEnv(raw, lookup)
	if err != nil {
		return nil, err
	}

	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(expanded))
	// Reject fields the struct does not define, so a typo cannot masquerade as a
	// setting that took effect.
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	for name, s := range cfg.Streams {
		if s == nil {
			return nil, fmt.Errorf("stream %q has no body", name)
		}
		s.Name = name
	}

	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// expandEnv replaces ${VAR} and ${VAR:-default}. An unset variable with no
// default is an error naming it: substituting an empty string would produce a
// config that looks valid and then fails to connect.
func expandEnv(raw []byte, lookup func(string) (string, bool)) ([]byte, error) {
	var missing []string

	out := envRefPattern.ReplaceAllFunc(raw, func(match []byte) []byte {
		groups := envRefPattern.FindSubmatch(match)
		name := string(groups[1])
		hasDefault := len(groups[2]) > 0

		if v, ok := lookup(name); ok {
			return []byte(v)
		}
		if hasDefault {
			return groups[3]
		}
		missing = append(missing, name)
		return match
	})

	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, fmt.Errorf("unset environment variable(s): %s", strings.Join(dedupe(missing), ", "))
	}
	return out, nil
}

func dedupe(in []string) []string {
	out := in[:0:0]
	seen := make(map[string]bool, len(in))
	for _, v := range in {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

func (c *Config) applyDefaults() {
	setString(&c.Source.TimeZone, "UTC")
	setString(&c.Source.TLS, "preferred")
	setDuration(&c.Source.HeartbeatPeriod, 5*time.Second)
	setDuration(&c.Source.ReadTimeout, 90*time.Second)

	setString(&c.Checkpoint.Table, "changeflow_checkpoints")

	c.Runtime.BufferSize = intOr(c.Runtime.BufferSizeRaw, 8192)
	setDuration(&c.Runtime.ShutdownGrace, 30*time.Second)
	setString(&c.Runtime.MetricsAddr, "127.0.0.1:9187")
	if c.Runtime.AssumedRowBytes == 0 {
		c.Runtime.AssumedRowBytes = 1 << 10
	}

	for _, s := range c.Streams {
		s.Batch.MaxRows = intOr(s.Batch.MaxRowsRaw, 1000)
		s.Batch.MaxBytes = 5 << 20
		if s.Batch.MaxBytesRaw != nil {
			s.Batch.MaxBytes = *s.Batch.MaxBytesRaw
		}
		setDuration(&s.Batch.FlushInterval, 500*time.Millisecond)

		s.Sink.Workers = intOr(s.Sink.WorkersRaw, 4)
		s.Snapshot.Enabled = s.Snapshot.EnabledRaw == nil || *s.Snapshot.EnabledRaw
		s.Snapshot.ChunkSize = intOr(s.Snapshot.ChunkSizeRaw, 5000)
		s.Snapshot.MaxRateRowsPerSec = intOr(s.Snapshot.MaxRateRowsPerSecRaw, 20000)

		if s.Mapping.OnZeroDate == "" {
			s.Mapping.OnZeroDate = "null"
		}
	}
}

// intOr resolves an optional setting, so an omitted value takes the default while
// an explicit one is kept even when it is invalid and needs reporting.
func intOr(raw *int, def int) int {
	if raw == nil {
		return def
	}
	return *raw
}

func setString(field *string, def string) {
	if *field == "" {
		*field = def
	}
}

func setDuration(field *Duration, def time.Duration) {
	if *field == 0 {
		*field = Duration(def)
	}
}

// Validate reports every problem it can find, rather than stopping at the first.
func (c *Config) Validate() error {
	var problems []error
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Errorf(format, args...))
	}

	if c.Source.DSN == "" {
		add("source.dsn is required")
	}
	if c.Source.ServerID == 0 {
		add("source.server_id must be non-zero and unique among this master's replicas")
	}
	if _, err := time.LoadLocation(c.Source.TimeZone); err != nil {
		add("source.time_zone %q is not a known location", c.Source.TimeZone)
	}
	if c.Checkpoint.DSN == "" {
		add("checkpoint.dsn is required; positions must outlive the process")
	}
	if c.Runtime.BufferSize < 1 {
		add("runtime.buffer_size must be at least 1, got %d", c.Runtime.BufferSize)
	}

	if len(c.Streams) == 0 {
		add("streams must define at least one stream")
	}
	for _, name := range c.StreamNames() {
		c.Streams[name].validate(name, add)
	}

	return errors.Join(problems...)
}

func (s *Stream) validate(name string, add func(string, ...any)) {
	path := "streams." + name

	switch {
	case name == "":
		add("streams has an empty stream name")
	case len(name) > MaxStreamNameLen:
		add("%s: stream name is %d characters; the checkpoint table allows at most %d", path, len(name), MaxStreamNameLen)
	case !streamNamePattern.MatchString(name):
		add("%s: stream name may contain only letters, digits, and underscore", path)
	}

	if s.Schema() == "" || s.TableName() == "" {
		add("%s.table must be written as database.table, got %q", path, s.Table)
	}

	if s.Batch.MaxRows < 1 {
		add("%s.batch.max_rows must be at least 1", path)
	}
	if s.Batch.MaxBytes == 0 {
		add("%s.batch.max_bytes must be above zero", path)
	}
	if s.Sink.Workers < 1 {
		add("%s.sink.workers must be at least 1", path)
	}
	if s.Snapshot.ChunkSize < 1 {
		add("%s.snapshot.chunk_size must be at least 1", path)
	}

	s.Sink.validate(path+".sink", add)
	s.Mapping.validate(path+".mapping", add)

	switch s.Mapping.OnZeroDate {
	case "null", "error", "epoch":
	default:
		add("%s.mapping.on_zero_date must be null, error, or epoch, got %q", path, s.Mapping.OnZeroDate)
	}
}

func (k *Sink) validate(path string, add func(string, ...any)) {
	switch k.Type {
	case SinkElasticsearch:
		if len(k.Addresses) == 0 {
			add("%s.addresses is required for an elasticsearch sink", path)
		}
		if k.Index == "" {
			add("%s.index is required for an elasticsearch sink", path)
		}
		// Options belonging to another sink type are almost certainly a mistake,
		// and ignoring them silently hides it.
		if k.DSN != "" {
			add("%s.dsn does not apply to an elasticsearch sink", path)
		}
		if k.Table != "" {
			add("%s.table does not apply to an elasticsearch sink; use index", path)
		}

	case SinkClickHouse:
		if k.DSN == "" {
			add("%s.dsn is required for a clickhouse sink", path)
		}
		if k.Table == "" {
			add("%s.table is required for a clickhouse sink", path)
		}
		if len(k.Addresses) > 0 {
			add("%s.addresses does not apply to a clickhouse sink; use dsn", path)
		}
		if k.Index != "" {
			add("%s.index does not apply to a clickhouse sink; use table", path)
		}
		if k.Alias != "" {
			add("%s.alias does not apply to a clickhouse sink", path)
		}

	case "":
		add("%s.type is required (%s or %s)", path, SinkElasticsearch, SinkClickHouse)
	default:
		add("%s.type %q is not a known sink; use %s or %s", path, k.Type, SinkElasticsearch, SinkClickHouse)
	}
}

func (m *Mapping) validate(path string, add func(string, ...any)) {
	if len(m.Include) > 0 && len(m.Exclude) > 0 {
		add("%s: include and exclude cannot both be set; choose one", path)
	}

	// A rename that lands on a name already in use would drop a field without
	// saying so.
	targets := make(map[string]string, len(m.Rename))
	for from, to := range m.Rename {
		if to == "" {
			add("%s.rename[%s] has an empty target", path, from)
			continue
		}
		if other, clash := targets[to]; clash {
			add("%s.rename maps both %q and %q onto %q", path, other, from, to)
			continue
		}
		targets[to] = from
	}
	for _, col := range m.Include {
		if from, clash := targets[col]; clash && from != col {
			add("%s.rename maps %q onto %q, which is also included as a source column", path, from, col)
		}
	}
}

// Schema returns the database part of the stream's table.
func (s *Stream) Schema() string {
	schema, _, found := strings.Cut(s.Table, ".")
	if !found {
		return ""
	}
	return schema
}

// TableName returns the table part of the stream's table.
func (s *Stream) TableName() string {
	_, table, found := strings.Cut(s.Table, ".")
	if !found {
		return ""
	}
	return table
}

// StreamNames returns every configured stream name in sorted order, so logs,
// status output, and startup order do not vary between runs.
func (c *Config) StreamNames() []string {
	names := make([]string, 0, len(c.Streams))
	for name := range c.Streams {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// MetricsEnabled reports whether metrics and health should be served.
func (r Runtime) MetricsEnabled() bool {
	return r.MetricsAddr != "" && r.MetricsAddr != MetricsDisabled
}

// Stream returns one configured stream, listing what exists when it is missing.
func (c *Config) Stream(name string) (*Stream, error) {
	if s, ok := c.Streams[name]; ok {
		return s, nil
	}
	return nil, fmt.Errorf("no stream named %q; configured streams are: %s", name, strings.Join(c.StreamNames(), ", "))
}

// EstimatedMemory bounds steady-state memory: each pipeline's queue holds rows,
// and each sink worker can hold a full batch in flight.
func (c *Config) EstimatedMemory() uint64 {
	var total uint64
	for _, s := range c.Streams {
		total += uint64(c.Runtime.BufferSize) * c.Runtime.AssumedRowBytes.Bytes()
		total += uint64(s.Sink.Workers) * s.Batch.MaxBytes.Bytes()
	}
	return total
}

// CheckMemoryLimit refuses a configuration whose bound exceeds the process memory
// limit, turning a scheduled out-of-memory kill into a startup error. A limit of
// math.MaxInt64 means none is set.
func (c *Config) CheckMemoryLimit(limit int64) error {
	if limit <= 0 || limit == 1<<63-1 {
		return nil
	}
	need := c.EstimatedMemory()
	if need > uint64(limit) {
		return fmt.Errorf("configuration needs about %s of buffers and in-flight batches, which exceeds the memory limit of %s; reduce runtime.buffer_size, batch.max_bytes, or sink.workers",
			ByteSize(need), ByteSize(limit))
	}
	return nil
}
