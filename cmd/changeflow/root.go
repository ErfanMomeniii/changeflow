package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
)

// Set by the linker at release time; a build without them falls back to what the Go
// toolchain stamps in, so a binary can always say what it is.
var (
	version = "dev"
	commit  = ""
	date    = ""
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
		Version:       buildVersion(),
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

// buildVersion reports what this binary is, for a deployment nobody can otherwise identify.
func buildVersion() string {
	revision, built := commit, date
	if revision == "" {
		if info, ok := debug.ReadBuildInfo(); ok {
			for _, setting := range info.Settings {
				switch setting.Key {
				case "vcs.revision":
					revision = setting.Value
				case "vcs.time":
					if built == "" {
						built = setting.Value
					}
				}
			}
		}
	}
	out := version
	// An untagged build names itself by revision already, so repeating it reads as noise.
	if revision != "" && !strings.Contains(out, revision) {
		out += " (" + revision + ")"
	}
	if built != "" {
		out += " built " + built
	}
	return out
}

func configFlag(cmd *cobra.Command, path *string) {
	cmd.Flags().StringVarP(path, "config", "c", "changeflow.yaml", "path to the configuration file")
}
