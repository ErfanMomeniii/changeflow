package main

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/ErfanMomeniii/changeflow/internal/config"
	"github.com/ErfanMomeniii/changeflow/internal/supervisor"
)

func newResnapshotCmd() *cobra.Command {
	var (
		path       string
		streamName string
		confirm    bool
	)
	cmd := &cobra.Command{
		Use:   "resnapshot",
		Short: "Ask a stream to scan its table again on next start",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(path)
			if err != nil {
				return err
			}
			return supervisor.Resnapshot(cmd.Context(), os.Stdout, cfg, streamName, confirm)
		},
	}
	configFlag(cmd, &path)
	cmd.Flags().StringVar(&streamName, "stream", "", "which configured stream to rescan")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "required: rescanning reads the whole table")
	_ = cmd.MarkFlagRequired("stream")
	return cmd
}
