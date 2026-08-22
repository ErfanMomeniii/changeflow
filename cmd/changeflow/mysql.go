package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/ErfanMomeniii/changeflow/internal/preflight"
)

func open(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(2)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect to mysql: %w", err)
	}
	return db, nil
}

func printReport(r preflight.Report) {
	for _, c := range r.Checks {
		mark := "✓"
		if !c.OK {
			mark = "✗"
		}
		fmt.Printf("%s %-28s want=%-10s got=%s\n", mark, c.Name, c.Want, c.Got)
	}
	for _, c := range r.Warnings() {
		fmt.Printf("\nwarning: %s (%s)\n  %s\n", c.Name, c.Severity, c.Why)
	}
	for _, c := range r.Failures() {
		fmt.Printf("\nFAIL: %s want=%s got=%s\n  %s\n", c.Name, c.Want, c.Got, c.Why)
	}
	fmt.Println()
}

func splitHostPort(addr string) (string, uint16, error) {
	if addr == "" {
		return "", 0, errors.New("dsn has no server address")
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		host = strings.TrimSuffix(strings.TrimPrefix(addr, "["), "]")
		return host, 3306, nil
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return "", 0, fmt.Errorf("parse port in %q: %w", addr, err)
	}
	if port == 0 {
		return "", 0, fmt.Errorf("port 0 in %q", addr)
	}
	return host, uint16(port), nil
}
