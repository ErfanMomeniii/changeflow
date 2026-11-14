package source

import "testing"

func TestAddress(t *testing.T) {
	for _, tc := range []struct {
		name     string
		dsn      string
		wantHost string
		wantPort uint16
	}{
		{"host and port", "u:p@tcp(db.internal:3307)/", "db.internal", 3307},
		{"host only defaults the port", "u:p@tcp(db.internal)/", "db.internal", 3306},
		{"ipv4", "u:p@tcp(127.0.0.1:13306)/", "127.0.0.1", 13306},
		{"bracketed ipv6 with port", "u:p@tcp([::1]:3306)/", "::1", 3306},
	} {
		t.Run(tc.name, func(t *testing.T) {
			host, port, err := Address(tc.dsn)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if host != tc.wantHost || port != tc.wantPort {
				t.Fatalf("got %s:%d, want %s:%d", host, port, tc.wantHost, tc.wantPort)
			}
		})
	}
}

func TestAddressRejectsBadInput(t *testing.T) {
	for _, tc := range []struct{ name, dsn string }{
		{"non-tcp", "u:p@unix(/tmp/mysql.sock)/"},
		{"non-numeric port", "u:p@tcp(host:mysql)/"},
		{"unparsable", "://bad dsn"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := Address(tc.dsn); err == nil {
				t.Fatalf("expected an error for %q", tc.dsn)
			}
		})
	}
}
