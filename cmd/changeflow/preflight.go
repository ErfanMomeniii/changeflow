package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/erfanmomeniii/changeflow/internal/preflight"
)

func newPreflightCmd() *cobra.Command {
	var dsn string
	cmd := &cobra.Command{
		Use:   "preflight",
		Short: "Check whether a MySQL server is configured for CDC",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if dsn == "" {
				return fmt.Errorf("--dsn is required (or set CHANGEFLOW_DSN)")
			}
			report, err := preflight.CheckServer(cmd.Context(), dsn, os.Stdout)
			if err != nil {
				return err
			}
			if !report.OK() {
				return fmt.Errorf("%d required check(s) failed", len(report.Failures()))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&dsn, "dsn", os.Getenv("CHANGEFLOW_DSN"), "MySQL DSN, e.g. user:pass@tcp(host:3306)/ (env: CHANGEFLOW_DSN)")
	return cmd
}
