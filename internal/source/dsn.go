// Package source owns connecting to the MySQL server everything reads from.
package source

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	driver "github.com/go-sql-driver/mysql"
)

// Open connects to a MySQL server described by a DSN.
func Open(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	db.SetConnMaxLifetime(time.Hour)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// Address splits a tcp DSN into host and port.
func Address(dsn string) (string, uint16, error) {
	cfg, err := driver.ParseDSN(dsn)
	if err != nil {
		return "", 0, fmt.Errorf("parse dsn: %w", err)
	}
	if cfg.Net != "tcp" {
		return "", 0, fmt.Errorf("replication needs a tcp dsn, got %q; use user:pass@tcp(host:3306)/", cfg.Net)
	}
	host, portStr, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return strings.Trim(cfg.Addr, "[]"), 3306, nil
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return "", 0, fmt.Errorf("parse port in %q: %w", cfg.Addr, err)
	}
	return host, uint16(port), nil
}

// Username returns the user named in a DSN, or "" if it does not parse.
func Username(dsn string) string {
	if cfg, err := driver.ParseDSN(dsn); err == nil {
		return cfg.User
	}
	return ""
}

// Password returns the password in a DSN, or "" if it does not parse.
func Password(dsn string) string {
	if cfg, err := driver.ParseDSN(dsn); err == nil {
		return cfg.Passwd
	}
	return ""
}

// Position returns the server's current gtid_executed, the point to stream from.
func Position(ctx context.Context, db *sql.DB) (string, error) {
	var gtid string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&gtid); err != nil {
		return "", fmt.Errorf("read gtid_executed: %w", err)
	}
	gtid = strings.ReplaceAll(gtid, "\n", "")
	if strings.TrimSpace(gtid) == "" {
		return "", errors.New("gtid_executed is empty: the server has logged no transactions yet, so there is no position to stream from; write a row first")
	}
	return gtid, nil
}
