package main

import (
	"errors"
	"os"

	"github.com/spf13/cobra"

	"github.com/erfanmomeniii/changeflow/internal/tail"
)

func newTailCmd() *cobra.Command {
	var opts tail.Options
	cmd := &cobra.Command{
		Use:   "tail",
		Short: "Register as a replica and print decoded row changes",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if opts.DSN == "" {
				return errors.New("--dsn is required (or set CHANGEFLOW_DSN)")
			}
			return tail.Stream(cmd.Context(), opts)
		},
	}
	cmd.Flags().StringVar(&opts.DSN, "dsn", os.Getenv("CHANGEFLOW_DSN"), "MySQL DSN, e.g. user:pass@tcp(host:3306)/ (env: CHANGEFLOW_DSN)")
	cmd.Flags().Uint32Var(&opts.ServerID, "server-id", 1001, "replica server_id; must be unique among replicas of this master")
	cmd.Flags().StringArrayVar(&opts.Tables, "table", nil, "db.table to watch; repeatable, default all tables")
	cmd.Flags().StringVar(&opts.CaptureDir, "capture", "", "directory to write raw event fixtures into")
	cmd.Flags().DurationVar(&opts.Duration, "for", 0, "stop after this long; 0 means run until interrupted")
	cmd.Flags().BoolVar(&opts.SkipPreflight, "skip-preflight", false, "stream even if required checks fail")
	return cmd
}
