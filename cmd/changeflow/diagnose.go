package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/ErfanMomeniii/changeflow/internal/preflight"
	"github.com/ErfanMomeniii/changeflow/internal/tail"
)

// sourceFlags are the flags the diagnostics share: they talk to a server directly
// rather than through a configuration file.
type sourceFlags struct {
	dsn      string
	serverID uint32
}

func (s *sourceFlags) bind(cmd *cobra.Command) {
	cmd.Flags().StringVar(&s.dsn, "dsn", os.Getenv("CHANGEFLOW_DSN"), "MySQL DSN, e.g. user:pass@tcp(host:3306)/ (env: CHANGEFLOW_DSN)")
	cmd.Flags().Uint32Var(&s.serverID, "server-id", 1001, "replica server_id; must be unique among replicas of this master")
}

func (s *sourceFlags) require() error {
	if s.dsn == "" {
		return errors.New("--dsn is required (or set CHANGEFLOW_DSN)")
	}
	return nil
}

func newPreflightCmd() *cobra.Command {
	var sf sourceFlags

	cmd := &cobra.Command{
		Use:   "preflight",
		Short: "Check whether a MySQL server is configured for CDC",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := sf.require(); err != nil {
				return err
			}

			db, err := open(cmd.Context(), sf.dsn)
			if err != nil {
				return err
			}
			defer db.Close()

			report, err := preflight.Run(cmd.Context(), preflight.DBReader{DB: db})
			if err != nil {
				return err
			}
			printReport(report)
			if !report.OK() {
				return fmt.Errorf("%d required check(s) failed", len(report.Failures()))
			}
			return nil
		},
	}

	sf.bind(cmd)
	return cmd
}

func newTailCmd() *cobra.Command {
	var (
		sf         sourceFlags
		tables     []string
		captureDir string
		duration   time.Duration
		skipCheck  bool
	)

	cmd := &cobra.Command{
		Use:   "tail",
		Short: "Register as a replica and print decoded row changes",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			if err := sf.require(); err != nil {
				return err
			}

			cfg, err := driver.ParseDSN(sf.dsn)
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

			db, err := open(ctx, sf.dsn)
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
			if sourceID, ok := report.Get("server_id"); ok && sourceID == strconv.FormatUint(uint64(sf.serverID), 10) {
				return fmt.Errorf("--server-id %d is the source's own server_id; pick a different one", sf.serverID)
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
				ServerID:   sf.serverID,
				Tables:     tables,
				CaptureDir: captureDir,
				Duration:   duration,
				StartGTID:  gtid,
				Out:        os.Stdout,
			})
		},
	}

	sf.bind(cmd)
	cmd.Flags().StringArrayVar(&tables, "table", nil, "db.table to watch; repeatable, default all tables")
	cmd.Flags().StringVar(&captureDir, "capture", "", "directory to write raw event fixtures into")
	cmd.Flags().DurationVar(&duration, "for", 0, "stop after this long; 0 means run until interrupted")
	cmd.Flags().BoolVar(&skipCheck, "skip-preflight", false, "stream even if required checks fail")
	return cmd
}
