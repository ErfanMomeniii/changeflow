package main

import (
	"context"
	"log/slog"
	"os"
	"runtime/debug"

	"github.com/spf13/cobra"

	"github.com/ErfanMomeniii/changeflow/internal/config"
	"github.com/ErfanMomeniii/changeflow/internal/supervisor"
)

func newRunCmd() *cobra.Command {
	var path, stream, dlqDir string
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Replicate configured streams until interrupted",
		Long:  "Replicate every configured stream, or one named stream, until interrupted.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return replicate(cmd.Context(), path, stream, dlqDir)
		},
	}
	configFlag(cmd, &path)
	cmd.Flags().StringVar(&stream, "stream", "", "run only this stream; by default every configured stream runs, sharing one connection to the source")
	cmd.Flags().StringVar(&dlqDir, "dlq-dir", "dlq", "directory for records of documents a destination refused")
	return cmd
}

func replicate(ctx context.Context, path, stream, dlqDir string) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	if err := cfg.CheckMemoryLimit(debug.SetMemoryLimit(-1)); err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	sup, err := supervisor.New(cfg, stream, dlqDir, log)
	if err != nil {
		return err
	}
	return sup.Run(ctx)
}
