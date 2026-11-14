package main

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/erfanmomeniii/changeflow/internal/config"
	"github.com/erfanmomeniii/changeflow/internal/supervisor"
)

func newStatusCmd() *cobra.Command {
	var path, dlqDir string
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Report each stream's position, lag, snapshot state, and refused documents",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(path)
			if err != nil {
				return err
			}
			return supervisor.Status(cmd.Context(), os.Stdout, cfg, dlqDir)
		},
	}
	configFlag(cmd, &path)
	cmd.Flags().StringVar(&dlqDir, "dlq-dir", "", "also report how many documents each stream has had refused")
	return cmd
}
