package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ErfanMomeniii/changeflow/internal/preflight"
)

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
			return checkServer(cmd.Context(), sf.dsn)
		},
	}
	sf.bind(cmd)
	return cmd
}

func checkServer(ctx context.Context, dsn string) error {
	db, err := open(ctx, dsn)
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
