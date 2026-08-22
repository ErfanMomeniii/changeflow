package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ByteSize is a size in bytes, written in configuration with a unit suffix.
//
// Both binary and decimal units are accepted and treated distinctly: MiB is
// 1048576 and MB is 1000000. The difference matters when a batch size is being
// matched against a service's real request limit.
type ByteSize uint64

var byteUnits = []struct {
	suffix string
	factor uint64
}{
	{"KIB", 1 << 10},
	{"MIB", 1 << 20},
	{"GIB", 1 << 30},
	{"TIB", 1 << 40},
	{"KB", 1_000},
	{"MB", 1_000_000},
	{"GB", 1_000_000_000},
	{"TB", 1_000_000_000_000},
	{"B", 1},
}

// UnmarshalYAML parses a size such as "5MiB", "512KB", or a plain byte count.
func (b *ByteSize) UnmarshalYAML(unmarshal func(any) error) error {
	var raw string
	if err := unmarshal(&raw); err != nil {
		var n uint64
		if err2 := unmarshal(&n); err2 != nil {
			return fmt.Errorf("size must be a string like \"5MiB\" or a byte count: %w", err)
		}
		*b = ByteSize(n)
		return nil
	}
	size, err := ParseByteSize(raw)
	if err != nil {
		return err
	}
	*b = size
	return nil
}

// ParseByteSize converts a size string into bytes.
func ParseByteSize(raw string) (ByteSize, error) {
	s := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(raw), " ", ""))
	if s == "" {
		return 0, fmt.Errorf("size is empty; write something like \"5MiB\"")
	}
	factor := uint64(1)
	for _, u := range byteUnits {
		if strings.HasSuffix(s, u.suffix) {
			factor = u.factor
			s = strings.TrimSuffix(s, u.suffix)
			break
		}
	}
	if s == "" {
		return 0, fmt.Errorf("size %q has a unit but no number", raw)
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("size %q is not a whole number of bytes with an optional unit (KiB, MiB, GiB, KB, MB, GB)", raw)
	}
	if n != 0 && factor > ^uint64(0)/n {
		return 0, fmt.Errorf("size %q overflows", raw)
	}
	return ByteSize(n * factor), nil
}

// String renders the size using the largest binary unit that divides it exactly,
// so a value read back from configuration looks the way it was written.
func (b ByteSize) String() string {
	n := uint64(b)
	for _, u := range []struct {
		suffix string
		factor uint64
	}{
		{"TiB", 1 << 40},
		{"GiB", 1 << 30},
		{"MiB", 1 << 20},
		{"KiB", 1 << 10},
	} {
		if n >= u.factor && n%u.factor == 0 {
			return strconv.FormatUint(n/u.factor, 10) + u.suffix
		}
	}
	return strconv.FormatUint(n, 10) + "B"
}

// Bytes returns the size as a plain integer.
func (b ByteSize) Bytes() uint64 { return uint64(b) }

// Duration is a time span written in configuration as a Go duration string.
//
// A bare number is rejected: "5" could mean seconds or milliseconds, and guessing
// would change behaviour without telling anyone.
type Duration time.Duration

// UnmarshalYAML parses a duration such as "500ms" or "1m30s".
func (d *Duration) UnmarshalYAML(unmarshal func(any) error) error {
	var raw string
	if err := unmarshal(&raw); err != nil {
		return fmt.Errorf("duration must be a string like \"500ms\" or \"2s\": %w", err)
	}
	parsed, err := ParseDuration(raw)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

// ParseDuration converts a duration string into a time.Duration, rejecting
// negatives and unitless numbers.
func ParseDuration(raw string) (time.Duration, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, fmt.Errorf("duration is empty; write something like \"500ms\"")
	}
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return 0, fmt.Errorf("duration %q needs a unit, for example %qs or %qms", raw, s, s)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("duration %q is not valid; use forms like \"500ms\", \"2s\", \"1m30s\"", raw)
	}
	if parsed < 0 {
		return 0, fmt.Errorf("duration %q is negative", raw)
	}
	return parsed, nil
}

// Duration returns the span as a time.Duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// String renders the span the way Go formats durations.
func (d Duration) String() string { return time.Duration(d).String() }
