package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ErfanMomeniii/changeflow/internal/checkpoint"
	"github.com/ErfanMomeniii/changeflow/internal/config"
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
			return resnapshot(cmd.Context(), path, streamName, confirm)
		},
	}

	configFlag(cmd, &path)
	cmd.Flags().StringVar(&streamName, "stream", "", "which configured stream to rescan")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "required: rescanning reads the whole table")
	_ = cmd.MarkFlagRequired("stream")
	return cmd
}

// resnapshot asks a stream to scan its table again.
//
// The scan itself happens on the next start, not here: this only clears the state that
// records one as finished. Rebuilding is how a mapping change is applied and how a lost
// checkpoint is recovered from, and it is deliberately a separate, explicit step rather
// than something a running process decides to do.
func resnapshot(ctx context.Context, path, streamName string, confirm bool) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	stream, err := cfg.Stream(streamName)
	if err != nil {
		return err
	}

	if !confirm {
		// A full table read costs real time and load on the source, so it is not
		// something to trigger by mistyping a stream name.
		return fmt.Errorf("rescanning %s reads all of %s and rewrites the destination; pass --confirm to proceed",
			streamName, stream.Table)
	}

	db, err := open(ctx, cfg.Checkpoint.DSN)
	if err != nil {
		return err
	}
	defer db.Close()

	store, err := checkpoint.NewMySQLStore(db, cfg.Checkpoint.Table)
	if err != nil {
		return err
	}

	// The lock is held for a stream's lifetime, so failing to take it means the stream
	// is running. Clearing its scan state underneath it would have it rescan at a
	// moment nobody chose.
	lock, err := store.Lock(ctx, streamName)
	if err != nil {
		if errors.Is(err, checkpoint.ErrStreamLocked) {
			return fmt.Errorf("stream %s is running; stop it before asking for a rescan", streamName)
		}
		return err
	}
	defer lock.Release(ctx)

	cp, err := store.Load(ctx, streamName)
	switch {
	case errors.Is(err, checkpoint.ErrNotFound), errors.Is(err, checkpoint.ErrNotInitialized):
		return fmt.Errorf("stream %s has never run, so its next start will scan anyway", streamName)
	case err != nil:
		return err
	}

	previous := cp.SnapshotRowsDone
	cp.ClearSnapshot()
	if err := store.Save(ctx, cp); err != nil {
		return err
	}

	fmt.Printf("stream %s will scan %s again on next start\n", streamName, stream.Table)
	if previous > 0 {
		fmt.Printf("  the previous scan had read %d rows\n", previous)
	}
	printRebuildAdvice(stream)
	return nil
}

// printRebuildAdvice says what has to be pointed somewhere new before the rescan starts, for
// the destinations where readers would otherwise see a half-built copy.
func printRebuildAdvice(stream *config.Stream) {
	switch {
	case stream.Sink.Type == config.SinkElasticsearch && stream.Sink.Alias != "":
		fmt.Printf("  point sink.index at a new index before starting, and the read alias %q\n"+
			"  will be moved to it once the scan finishes\n", stream.Sink.Alias)
	case stream.Sink.Type == config.SinkClickHouse:
		fmt.Print("  point sink.table at a new table before starting, then swap it in with\n" +
			"  EXCHANGE TABLES once the scan finishes\n")
	}
}
