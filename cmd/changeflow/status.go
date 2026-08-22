package main

import (
	"context"
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
			return showStatus(cmd.Context(), path, dlqDir)
		},
	}

	configFlag(cmd, &path)
	cmd.Flags().StringVar(&dlqDir, "dlq-dir", "", "also report how many documents each stream has had refused")
	return cmd
}

func showStatus(ctx context.Context, path, dlqDir string) error {
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

	table := statusTable{dlqDir: dlqDir}
	table.header()

	// Before any stream has run the table does not exist yet, which is worth
	// reporting plainly rather than as a failure.
	if _, err := store.Load(ctx, cfg.StreamNames()[0]); errors.Is(err, checkpoint.ErrNotInitialized) {
		for _, name := range cfg.StreamNames() {
			if err := table.row(name, "-", "not started", "-"); err != nil {
				return err
			}
		}
		return nil
	}

	now := time.Now()
	for _, name := range cfg.StreamNames() {
		cp, err := store.Load(ctx, name)
		if errors.Is(err, checkpoint.ErrNotFound) {
			if err := table.row(name, "-", "not started", "-"); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}

		if err := table.row(name, lagText(cp, now), snapshotText(cp), cp.GTIDSet); err != nil {
			return err
		}
		if cp.LastError != "" {
			// Under the row rather than in a column: it is the one field worth reading in
			// full, and a stopped stream is why anyone runs this.
			fmt.Printf("%-28s stopped: %s\n", "", cp.LastError)
		}
	}
	return nil
}

// statusTable prints the report, with a refused column only when a dead letter directory was
// given: a zero from a directory nobody passed would read as a reassurance nothing checked.
type statusTable struct {
	dlqDir string
}

func (t statusTable) format() string {
	if t.dlqDir == "" {
		return "%-28s %-10s %-12s %s\n"
	}
	return "%-28s %-10s %-12s %-8s %s\n"
}

func (t statusTable) header() {
	if t.dlqDir == "" {
		fmt.Printf(t.format(), "STREAM", "LAG", "SNAPSHOT", "POSITION")
		return
	}
	fmt.Printf(t.format(), "STREAM", "LAG", "SNAPSHOT", "REFUSED", "POSITION")
}

func (t statusTable) row(name, lag, snapshot, position string) error {
	if t.dlqDir == "" {
		fmt.Printf(t.format(), name, lag, snapshot, position)
		return nil
	}

	refused, err := dlq.Count(t.dlqDir, name)
	if err != nil {
		return err
	}
	fmt.Printf(t.format(), name, lag, snapshot, strconv.Itoa(refused), position)
	return nil
}

func lagText(cp checkpoint.Checkpoint, now time.Time) string {
	d, ok := cp.LagAt(now)
	if !ok {
		return "-"
	}
	return d.Round(time.Millisecond).String()
}

func snapshotText(cp checkpoint.Checkpoint) string {
	switch {
	case cp.SnapshotDone:
		return "done"
	case cp.SnapshotRowsTotal > 0:
		return fmt.Sprintf("%d%%", 100*cp.SnapshotRowsDone/cp.SnapshotRowsTotal)
	default:
		return "pending"
	}
}
