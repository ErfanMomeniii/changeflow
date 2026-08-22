package main

import "testing"

func TestSplitHostPort(t *testing.T) {
	for _, tc := range []struct {
		name     string
		addr     string
		wantHost string
		wantPort uint16
	}{
		{"host and port", "db.internal:3307", "db.internal", 3307},
		{"host only defaults the port", "db.internal", "db.internal", 3306},
		{"ipv4", "127.0.0.1:13306", "127.0.0.1", 13306},
		// Splitting on the first colon would truncate this to "[".
		{"bracketed ipv6 with port", "[::1]:3306", "::1", 3306},
		{"bracketed ipv6 without port", "[2001:db8::1]", "2001:db8::1", 3306},
	} {
		t.Run(tc.name, func(t *testing.T) {
			host, port, err := splitHostPort(tc.addr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if host != tc.wantHost || port != tc.wantPort {
				t.Fatalf("got %s:%d, want %s:%d", host, port, tc.wantHost, tc.wantPort)
			}
		})
	}
}

func TestSplitHostPortRejectsBadInput(t *testing.T) {
	for _, tc := range []struct{ name, addr string }{
		{"empty", ""},
		{"non-numeric port", "host:mysql"},
		{"port zero", "host:0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := splitHostPort(tc.addr); err == nil {
				t.Fatalf("expected an error for %q", tc.addr)
			}
		})
	}
}
