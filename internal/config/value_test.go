package config

import (
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestByteSizeParsing(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want uint64
	}{
		{"0", 0},
		{"1024", 1024},
		{"5MiB", 5 * 1024 * 1024},
		{"64MiB", 64 * 1024 * 1024},
		{"1GiB", 1024 * 1024 * 1024},
		{"512KiB", 512 * 1024},
		// SI units are decimal, and the difference matters when sizing a batch
		// against a service's real request limit.
		{"1KB", 1000},
		{"5MB", 5_000_000},
		{"2GB", 2_000_000_000},
		{"1B", 1},
		// Formatting people actually write.
		{" 5 MiB ", 5 * 1024 * 1024},
		{"5mib", 5 * 1024 * 1024},
	} {
		t.Run(tc.in, func(t *testing.T) {
			var got ByteSize
			if err := yaml.Unmarshal([]byte("v: "+quote(tc.in)), &struct {
				V *ByteSize `yaml:"v"`
			}{V: &got}); err != nil {
				t.Fatalf("unmarshal %q: %v", tc.in, err)
			}
			if uint64(got) != tc.want {
				t.Fatalf("got %d, want %d", uint64(got), tc.want)
			}
		})
	}
}

func TestByteSizeRejectsNonsense(t *testing.T) {
	for _, in := range []string{"", "MiB", "-5MiB", "5 apples", "5MiBB", "1.5MiB", "0x10"} {
		t.Run(in, func(t *testing.T) {
			var got ByteSize
			err := yaml.Unmarshal([]byte("v: "+quote(in)), &struct {
				V *ByteSize `yaml:"v"`
			}{V: &got})
			if err == nil {
				t.Fatalf("expected %q to be rejected, got %d", in, uint64(got))
			}
		})
	}
}

func TestByteSizeStringRoundTrips(t *testing.T) {
	for _, tc := range []struct {
		in   ByteSize
		want string
	}{
		{0, "0B"},
		{1024, "1KiB"},
		{5 * 1024 * 1024, "5MiB"},
		{1536, "1536B"}, // not a whole unit, so reported exactly
	} {
		if got := tc.in.String(); got != tc.want {
			t.Errorf("ByteSize(%d).String() = %q, want %q", uint64(tc.in), got, tc.want)
		}
	}
}

func TestDurationParsing(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want time.Duration
	}{
		{"500ms", 500 * time.Millisecond},
		{"2s", 2 * time.Second},
		{"1m30s", 90 * time.Second},
		{"90s", 90 * time.Second},
		{"1h", time.Hour},
		{"0s", 0},
	} {
		t.Run(tc.in, func(t *testing.T) {
			var got Duration
			if err := yaml.Unmarshal([]byte("v: "+quote(tc.in)), &struct {
				V *Duration `yaml:"v"`
			}{V: &got}); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got.Duration() != tc.want {
				t.Fatalf("got %v, want %v", got.Duration(), tc.want)
			}
		})
	}
}

func TestDurationRejectsNonsense(t *testing.T) {
	// A bare number is rejected on purpose: "5" is ambiguous between seconds and
	// milliseconds, and guessing wrong changes behaviour silently.
	for _, in := range []string{"", "5", "soon", "-2s", "2 s"} {
		t.Run(in, func(t *testing.T) {
			var got Duration
			if err := yaml.Unmarshal([]byte("v: "+quote(in)), &struct {
				V *Duration `yaml:"v"`
			}{V: &got}); err == nil {
				t.Fatalf("expected %q to be rejected, got %v", in, got.Duration())
			}
		})
	}
}

func quote(s string) string {
	return "\"" + s + "\""
}
