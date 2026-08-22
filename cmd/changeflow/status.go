package main

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/ErfanMomeniii/changeflow/internal/checkpoint"
	"github.com/ErfanMomeniii/changeflow/internal/config"
	"github.com/ErfanMomeniii/changeflow/internal/sink/dlq"
)

func newStatusCmd() *cobra.Command {
	var path, dlqDir string

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Report each stream's position, lag, snapshot state, and refused documents",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			cfg, err := config.Load(path)
			if err != nil {
				return err
			}

			// Status reads the checkpoint table rather than asking a running process, so it
			// works when a stream is down, which is when it is needed most.
			db, err := open(ctx, cfg.Checkpoint.DSN)
			if err != nil {
				return err
			}
			defer db.Close()

			store, err := checkpoint.NewMySQLStore(db, cfg.Checkpoint.Table)
			if err != nil {
				return err
			}

			now := time.Now()
			header := "%-28s %-10s %-12s %s\n"
			columns := []any{"STREAM", "LAG", "SNAPSHOT", "POSITION"}
			if dlqDir != "" {
				header = "%-28s %-10s %-12s %-8s %s\n"
				columns = []any{"STREAM", "LAG", "SNAPSHOT", "REFUSED", "POSITION"}
			}
			fmt.Printf(header, columns...)

			// row prints one stream, with the refused count only when a directory was given: a
			// zero from a directory nobody passed would read as a reassurance nothing checked.
			row := func(name, lag, snapshot, position string) error {
				if dlqDir == "" {
					fmt.Printf(header, name, lag, snapshot, position)
					return nil
				}
				refused, err := dlq.Count(dlqDir, name)
				if err != nil {
					return err
				}
				fmt.Printf(header, name, lag, snapshot, strconv.Itoa(refused), position)
				return nil
			}

			// Before any stream has run the table does not exist yet, which is worth
			// reporting plainly rather than as a failure.
			if _, err := store.Load(ctx, cfg.StreamNames()[0]); errors.Is(err, checkpoint.ErrNotInitialized) {
				for _, name := range cfg.StreamNames() {
					if err := row(name, "-", "not started", "-"); err != nil {
						return err
					}
				}
				return nil
			}
			for _, name := range cfg.StreamNames() {
				cp, err := store.Load(ctx, name)
				if errors.Is(err, checkpoint.ErrNotFound) {
					if err := row(name, "-", "not started", "-"); err != nil {
						return err
					}
					continue
				}
				if err != nil {
					return err
				}

				lag := "-"
				if d, ok := cp.LagAt(now); ok {
					lag = d.Round(time.Millisecond).String()
				}
				snapshot := "pending"
				if cp.SnapshotDone {
					snapshot = "done"
				} else if cp.SnapshotRowsTotal > 0 {
					snapshot = fmt.Sprintf("%d%%", 100*cp.SnapshotRowsDone/cp.SnapshotRowsTotal)
				}
				if err := row(name, lag, snapshot, cp.GTIDSet); err != nil {
					return err
				}
				if cp.LastError != "" {
					// Under the row rather than in a column: it is the one field worth reading in
					// full, and a stopped stream is why anyone runs this.
					fmt.Printf("%-28s stopped: %s\n", "", cp.LastError)
				}
			}
			return nil
		},
	}

	configFlag(cmd, &path)
	cmd.Flags().StringVar(&dlqDir, "dlq-dir", "", "also report how many documents each stream has had refused")
	return cmd
}
