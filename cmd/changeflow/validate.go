package main

import (
	"fmt"
	"runtime/debug"

	"github.com/spf13/cobra"

	"github.com/ErfanMomeniii/changeflow/internal/config"
)

func newValidateCmd() *cobra.Command {
	var path string
	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Check a configuration file without connecting to anything",
		Args:  cobra.NoArgs,
		RunE:  func(*cobra.Command, []string) error { return validateConfig(path) },
	}
	configFlag(cmd, &path)
	return cmd
}

func validateConfig(path string) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	if err := cfg.CheckMemoryLimit(debug.SetMemoryLimit(-1)); err != nil {
		return err
	}
	fmt.Printf("configuration is valid: %d stream(s), about %s of buffers and in-flight batches\n",
		len(cfg.Streams), config.ByteSize(cfg.EstimatedMemory()))
	for _, name := range cfg.StreamNames() {
		s := cfg.Streams[name]
		fmt.Printf("  %-28s %s -> %s\n", name, s.Table, s.Sink.Type)
	}
	return nil
}
