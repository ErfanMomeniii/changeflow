package main

import (
	"context"
	"database/sql"
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

type tailRequest struct {
	source     sourceFlags
	tables     []string
	captureDir string
	duration   time.Duration
	skipCheck  bool
}

func newTailCmd() *cobra.Command {
	var req tailRequest
	cmd := &cobra.Command{
		Use:   "tail",
		Short: "Register as a replica and print decoded row changes",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := req.source.require(); err != nil {
				return err
			}
			return tailChanges(cmd.Context(), req)
		},
	}
	req.source.bind(cmd)
	cmd.Flags().StringArrayVar(&req.tables, "table", nil, "db.table to watch; repeatable, default all tables")
	cmd.Flags().StringVar(&req.captureDir, "capture", "", "directory to write raw event fixtures into")
	cmd.Flags().DurationVar(&req.duration, "for", 0, "stop after this long; 0 means run until interrupted")
	cmd.Flags().BoolVar(&req.skipCheck, "skip-preflight", false, "stream even if required checks fail")
	return cmd
}

func tailChanges(ctx context.Context, req tailRequest) error {
	cfg, err := driver.ParseDSN(req.source.dsn)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	if cfg.Net != "tcp" {
		return fmt.Errorf("replication needs a tcp dsn, got %q; use user:pass@tcp(host:3306)/", cfg.Net)
	}
	host, port, err := splitHostPort(cfg.Addr)
	if err != nil {
		return err
	}
	db, err := open(ctx, req.source.dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := gateOnPreflight(ctx, db, req.source.serverID, req.skipCheck); err != nil {
		return err
	}
	gtid, err := currentPosition(ctx, db)
	if err != nil {
		return err
	}
	return tail.Tail(ctx, tail.Config{
		Host:       host,
		Port:       port,
		User:       cfg.User,
		Password:   cfg.Passwd,
		ServerID:   req.source.serverID,
		Tables:     req.tables,
		CaptureDir: req.captureDir,
		Duration:   req.duration,
		StartGTID:  gtid,
		Out:        os.Stdout,
	})
}

func gateOnPreflight(ctx context.Context, db *sql.DB, serverID uint32, skipCheck bool) error {
	report, err := preflight.Run(ctx, preflight.DBReader{DB: db})
	if err != nil {
		return err
	}
	printReport(report)
	if !report.OK() && !skipCheck {
		return fmt.Errorf("%d required check(s) failed; fix the server or pass --skip-preflight to stream anyway", len(report.Failures()))
	}
	if sourceID, ok := report.Get("server_id"); ok && sourceID == strconv.FormatUint(uint64(serverID), 10) {
		return fmt.Errorf("--server-id %d is the source's own server_id; pick a different one", serverID)
	}
	return nil
}

func currentPosition(ctx context.Context, db *sql.DB) (string, error) {
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
