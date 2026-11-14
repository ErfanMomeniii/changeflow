package tail

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/erfanmomeniii/changeflow/internal/preflight"
	"github.com/erfanmomeniii/changeflow/internal/source"
)

// Options controls one streaming run, described by a DSN rather than split parts.
type Options struct {
	DSN           string
	ServerID      uint32
	Tables        []string
	CaptureDir    string
	Duration      time.Duration
	SkipPreflight bool
	Out           io.Writer
}

// Stream gates the server on preflight checks, then decodes row changes until
// the context is cancelled or Duration elapses.
func Stream(ctx context.Context, opts Options) error {
	host, port, err := source.Address(opts.DSN)
	if err != nil {
		return err
	}
	db, err := source.Open(ctx, opts.DSN)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := gate(ctx, db, opts); err != nil {
		return err
	}
	gtid, err := source.Position(ctx, db)
	if err != nil {
		return err
	}
	return Tail(ctx, Config{
		Host:       host,
		Port:       port,
		User:       source.Username(opts.DSN),
		Password:   source.Password(opts.DSN),
		ServerID:   opts.ServerID,
		Tables:     opts.Tables,
		CaptureDir: opts.CaptureDir,
		Duration:   opts.Duration,
		StartGTID:  gtid,
		Out:        opts.Out,
	})
}

// gate refuses to stream from a server that cannot replicate correctly.
func gate(ctx context.Context, db *sql.DB, opts Options) error {
	report, err := preflight.Run(ctx, preflight.DBReader{DB: db})
	if err != nil {
		return err
	}
	out := opts.Out
	if out == nil {
		out = os.Stdout
	}
	report.Print(out)
	if !report.OK() && !opts.SkipPreflight {
		return fmt.Errorf("%d required check(s) failed; fix the server or pass --skip-preflight to stream anyway", len(report.Failures()))
	}
	if report.ServerIDConflict(opts.ServerID) {
		return fmt.Errorf("server-id %d is the source's own server_id; pick a different one", opts.ServerID)
	}
	return nil
}
