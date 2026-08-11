// Command changeflow replicates MySQL changes into downstream stores.
//
// It currently offers two diagnostics: "preflight", which reports whether a
// server is configured for change data capture, and "tail", which prints decoded
// row changes as they happen.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/ErfanMomeniii/changeflow/internal/preflight"
	"github.com/ErfanMomeniii/changeflow/internal/tail"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "tail":
		err = runTail(ctx, os.Args[2:])
	case "preflight":
		err = runPreflight(ctx, os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `changeflow - MySQL change data capture

Usage:
  changeflow preflight --dsn <dsn>
        Check whether a MySQL server is configured for CDC.

  changeflow tail --dsn <dsn> [--table db.table] [--server-id N]
                  [--capture DIR] [--for 60s]
        Register as a replica and print decoded row changes.

DSN format:
  user:password@tcp(host:3306)/

`)
}

type commonFlags struct {
	dsn      string
	serverID uint
}

func (c *commonFlags) bind(fs *flag.FlagSet) {
	fs.StringVar(&c.dsn, "dsn", os.Getenv("CHANGEFLOW_DSN"), "MySQL DSN, e.g. user:pass@tcp(host:3306)/ (env: CHANGEFLOW_DSN)")
	fs.UintVar(&c.serverID, "server-id", 1001, "replica server_id; must be unique among replicas of this master")
}

func runPreflight(ctx context.Context, args []string) error {
	var cf commonFlags
	fs := flag.NewFlagSet("preflight", flag.ExitOnError)
	cf.bind(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if cf.dsn == "" {
		return fmt.Errorf("--dsn is required (or set CHANGEFLOW_DSN)")
	}

	db, err := open(ctx, cf.dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	report, err := preflight.Run(ctx, preflight.DBReader{DB: db})
	if err != nil {
		return err
	}
	printReport(report)
	if !report.OK() {
		return fmt.Errorf("%d required check(s) failed", len(report.Failures()))
	}
	return nil
}

func runTail(ctx context.Context, args []string) error {
	var (
		cf         commonFlags
		tables     stringList
		captureDir string
		duration   time.Duration
		skipCheck  bool
	)
	fs := flag.NewFlagSet("tail", flag.ExitOnError)
	cf.bind(fs)
	fs.Var(&tables, "table", "db.table to watch; repeatable, default all tables")
	fs.StringVar(&captureDir, "capture", "", "directory to write raw event fixtures into")
	fs.DurationVar(&duration, "for", 0, "stop after this long; 0 means run until interrupted")
	fs.BoolVar(&skipCheck, "skip-preflight", false, "stream even if required checks fail")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if cf.dsn == "" {
		return fmt.Errorf("--dsn is required (or set CHANGEFLOW_DSN)")
	}

	cfg, err := driver.ParseDSN(cf.dsn)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	// Replication runs over its own TCP connection rather than through the SQL
	// driver, so a unix socket DSN cannot be used for it.
	if cfg.Net != "tcp" {
		return fmt.Errorf("replication needs a tcp dsn, got %q; use user:pass@tcp(host:3306)/", cfg.Net)
	}
	host, port, err := splitHostPort(cfg.Addr)
	if err != nil {
		return err
	}

	db, err := open(ctx, cf.dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	report, err := preflight.Run(ctx, preflight.DBReader{DB: db})
	if err != nil {
		return err
	}
	printReport(report)
	if !report.OK() && !skipCheck {
		return fmt.Errorf("%d required check(s) failed; fix the server or pass --skip-preflight to stream anyway", len(report.Failures()))
	}

	// A replica claiming the source's own id is rejected by the server, and the
	// resulting error names neither side, so catch it here.
	if sourceID, ok := report.Get("server_id"); ok && sourceID == strconv.FormatUint(uint64(cf.serverID), 10) {
		return fmt.Errorf("--server-id %d is the source's own server_id; pick a different one", cf.serverID)
	}

	// Start from the server's current position. Rows written before now produce
	// no further binlog events, so they are invisible here; reading existing rows
	// requires a table scan rather than the binlog.
	var gtid string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&gtid); err != nil {
		return fmt.Errorf("read gtid_executed: %w", err)
	}
	// A multi-server GTID set is returned across several lines.
	gtid = strings.ReplaceAll(gtid, "\n", "")
	if strings.TrimSpace(gtid) == "" {
		return errors.New("gtid_executed is empty: the server has logged no transactions yet, so there is no position to stream from; write a row first")
	}

	return tail.Tail(ctx, tail.Config{
		Host:       host,
		Port:       port,
		User:       cfg.User,
		Password:   cfg.Passwd,
		ServerID:   uint32(cf.serverID),
		Tables:     tables,
		CaptureDir: captureDir,
		Duration:   duration,
		StartGTID:  gtid,
		Out:        os.Stdout,
	})
}

func open(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(2)
	// Ping under the caller's context so an unreachable host does not hang past
	// an interrupt.
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

// splitHostPort separates a "host:port" address, defaulting the port and handling
// bracketed IPv6 literals, where splitting on the first colon would truncate the
// address.
func splitHostPort(addr string) (string, uint16, error) {
	if addr == "" {
		return "", 0, errors.New("dsn has no server address")
	}

	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		// No port present: treat the whole value as the host. Strip brackets so a
		// bare IPv6 literal is usable.
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

// stringList collects a repeatable flag.
type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
}
