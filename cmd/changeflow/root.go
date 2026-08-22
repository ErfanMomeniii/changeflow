package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	started := false
	root := newRootCmd(&started)
	if len(os.Args) < 2 {
		root.SetOut(os.Stderr)
		_ = root.Help()
		os.Exit(2)
	}
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		if !started {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

func newRootCmd(started *bool) *cobra.Command {
	root := &cobra.Command{
		Use:   "changeflow",
		Short: "MySQL change data capture",
		Long: `changeflow - MySQL change data capture

DSN format:
  user:password@tcp(host:3306)/`,
		SilenceErrors: true,
		PersistentPreRun: func(cmd *cobra.Command, _ []string) {
			*started = true
			cmd.SilenceUsage = true
		},
	}
	root.SetOut(os.Stdout)
	root.AddCommand(
		newRunCmd(),
		newStatusCmd(),
		newValidateCmd(),
		newGenerateSchemaCmd(),
		newResnapshotCmd(),
		newPreflightCmd(),
		newTailCmd(),
	)
	return root
}

func configFlag(cmd *cobra.Command, path *string) {
	cmd.Flags().StringVarP(path, "config", "c", "changeflow.yaml", "path to the configuration file")
}
