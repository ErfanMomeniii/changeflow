package supervisor

// Backfill: scanning a table into a destination before streaming begins.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ErfanMomeniii/changeflow/internal/checkpoint"
	"github.com/ErfanMomeniii/changeflow/internal/sink/elasticsearch"
	"github.com/ErfanMomeniii/changeflow/internal/source/snapshot"
)

// snapshotIfNeeded runs a stream's table scan when one is outstanding, and returns where
// its streaming should begin.
//
// The order is what makes a lock-free scan correct: the position is captured first, so
// changes made during the scan are still in the binlog afterwards and carry versions above
// the one stamped on scanned rows. A row modified while being scanned therefore ends up
// with the modification, never the stale copy.
func (s *Supervisor) snapshotIfNeeded(
	ctx context.Context,
	store *checkpoint.MySQLStore,
	sourceDB *sql.DB,
	rt *streamRuntime,
) (string, error) {
	stream := rt.cfg

	cp, err := store.Load(ctx, stream.Name)
	if err != nil && !errors.Is(err, checkpoint.ErrNotFound) {
		// A position that cannot be read must never be guessed at: streaming from the
		// wrong place silently skips or duplicates data.
		return "", fmt.Errorf("supervisor: read checkpoint: %w", err)
	}

	// Already streaming: resume where the destination was last acknowledged.
	if cp.SnapshotDone && cp.GTIDSet != "" {
		return cp.GTIDSet, nil
	}

	if !stream.Snapshot.Enabled {
		if cp.GTIDSet != "" {
			return cp.GTIDSet, nil
		}
		position, err := currentPosition(ctx, sourceDB)
		if err != nil {
			return "", err
		}
		s.log.Warn("snapshots are disabled, so rows written before now will not appear in the destination",
			"stream", stream.Name, "from", position)
		cp.Stream, cp.SnapshotDone, cp.GTIDSet = stream.Name, true, position
		if err := store.Save(ctx, cp); err != nil {
			return "", fmt.Errorf("supervisor: record start position: %w", err)
		}
		return position, nil
	}

	// A first attempt captures the position and the version to stamp on scanned rows. A
	// resumed attempt keeps both, or the guarantee above would not hold.
	if cp.SnapshotStartGTID == "" {
		position, err := currentPosition(ctx, sourceDB)
		if err != nil {
			return "", err
		}
		baseSeq, err := rt.alloc.Next(ctx)
		if err != nil {
			return "", fmt.Errorf("supervisor: allocate the snapshot version: %w", err)
		}

		cp.Stream = stream.Name
		cp.SnapshotStartGTID = position
		cp.SnapshotBaseSeq = baseSeq
		cp.SnapshotRowsTotal = estimateRows(ctx, sourceDB, rt.meta)
		if err := store.Save(ctx, cp); err != nil {
			return "", fmt.Errorf("supervisor: record the snapshot start: %w", err)
		}
		s.log.Info("starting a table scan",
			"stream", stream.Name, "table", stream.Table,
			"from_position", position, "estimated_rows", cp.SnapshotRowsTotal)
	} else {
		s.log.Info("resuming a table scan",
			"stream", stream.Name, "rows_done", cp.SnapshotRowsDone, "estimated_rows", cp.SnapshotRowsTotal)
	}

	scanDB := sourceDB
	if s.cfg.Source.SnapshotDSN != "" {
		// A scan is the only part of changeflow that can slow the source down, so it can
		// be pointed at a replica instead.
		scanDB, err = openMySQL(ctx, s.cfg.Source.SnapshotDSN)
		if err != nil {
			return "", fmt.Errorf("supervisor: connect for the table scan: %w", err)
		}
		defer scanDB.Close()
	}

	key, err := rt.meta.ResolveKey(stream.Mapping.Key)
	if err != nil {
		return "", fmt.Errorf("supervisor: %w", err)
	}

	estimated := cp.SnapshotRowsTotal
	scanner, err := snapshot.New(snapshot.Options{
		DB:            scanDB,
		Meta:          rt.meta,
		Key:           key,
		ChunkSize:     stream.Snapshot.ChunkSize,
		MaxRowsPerSec: stream.Snapshot.MaxRateRowsPerSec,
		Cursor:        cp.SnapshotCursor,
		BaseSeq:       cp.SnapshotBaseSeq,
		Observe: func(rows uint64) {
			rt.metrics.SnapshotProgress(rows, estimated)
			s.log.Info("table scan progress", "stream", stream.Name, "rows", rows, "estimated_total", estimated)
		},
		Logger: s.log,
	})
	if err != nil {
		return "", err
	}

	rt.metrics.SnapshotRunning(true)
	defer rt.metrics.SnapshotRunning(false)

	if err := s.scan(ctx, rt, scanner); err != nil {
		return "", err
	}
	if !scanner.Done() {
		// Stopping short is not a failure, but the scan must not be recorded as complete,
		// or the rows it never reached would never be written.
		return "", ctx.Err()
	}

	cp, err = store.Load(ctx, stream.Name)
	if err != nil {
		return "", fmt.Errorf("supervisor: read checkpoint after the scan: %w", err)
	}
	cp.Stream = stream.Name
	cp.SnapshotDone = true
	cp.SnapshotRowsTotal = cp.SnapshotRowsDone
	if err := store.Save(ctx, cp); err != nil {
		return "", fmt.Errorf("supervisor: record the completed scan: %w", err)
	}
	s.log.Info("table scan complete", "stream", stream.Name, "rows", cp.SnapshotRowsDone)

	if err := s.promoteAlias(ctx, rt); err != nil {
		return "", err
	}

	// Streaming begins where the scan began, so every change made during it is applied on
	// top.
	return cp.SnapshotStartGTID, nil
}

// scan fills the destination from the table, with refreshing turned off where that is safe.
func (s *Supervisor) scan(ctx context.Context, rt *streamRuntime, scanner *snapshot.Snapshotter) error {
	load, err := s.beginBulkLoad(ctx, rt)
	if err != nil {
		// A scan that cannot change the destination's settings is slower, not wrong.
		s.log.Warn("could not prepare the destination for a bulk load",
			"stream", rt.cfg.Name, "error", err)
	}
	// Restored even when the scan stops early, so an interrupted rebuild does not leave
	// behind an index that never refreshes. Detached from the cancelled context, since
	// shutdown is exactly when this has to still happen.
	defer func() {
		if !load.Applied() {
			return
		}
		restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if err := rt.sink.(*elasticsearch.Sink).EndBulkLoad(restoreCtx, load); err != nil {
			s.log.Warn("could not restore the destination's settings after the scan",
				"stream", rt.cfg.Name, "index", rt.cfg.Sink.Index, "error", err)
		}
	}()

	if err := rt.runner.Run(ctx, scanner.Events(ctx)); err != nil {
		return err
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("supervisor: table scan: %w", err)
	}
	if !scanner.Done() {
		return nil
	}

	// Merged once the index is full, because a scan leaves far more segments behind than
	// an index that grew gradually, and search cost scales with their number.
	if load.Applied() {
		if err := rt.sink.(*elasticsearch.Sink).ForceMerge(ctx); err != nil {
			s.log.Warn("could not merge the scanned index's segments, which only makes it slower to search",
				"stream", rt.cfg.Name, "index", rt.cfg.Sink.Index, "error", err)
		}
	}
	return nil
}

// beginBulkLoad relaxes the destination's settings for a scan, but only when readers are
// not using the index being filled.
//
// An index with refreshing off answers searches with nothing, so this is only for an
// index being built behind an alias. Without an alias the index being filled is the one
// being read, and it is left alone.
func (s *Supervisor) beginBulkLoad(ctx context.Context, rt *streamRuntime) (elasticsearch.LoadSettings, error) {
	var none elasticsearch.LoadSettings

	es, ok := rt.sink.(*elasticsearch.Sink)
	if !ok || rt.cfg.Sink.Alias == "" {
		return none, nil
	}

	targets, err := es.AliasTargets(ctx)
	if err != nil {
		return none, err
	}
	for _, index := range targets {
		if index == rt.cfg.Sink.Index {
			return none, nil
		}
	}

	s.log.Info("filling a new index with refreshing off for the scan",
		"stream", rt.cfg.Name, "index", rt.cfg.Sink.Index, "readers_on", targets)
	return es.BeginBulkLoad(ctx)
}

// promoteAlias moves readers to the index a scan has just filled.
//
// Done after the scan rather than at startup, because until it finishes the index is
// incomplete and pointing readers at it would show them a half-built table.
func (s *Supervisor) promoteAlias(ctx context.Context, rt *streamRuntime) error {
	es, ok := rt.sink.(*elasticsearch.Sink)
	if !ok || rt.cfg.Sink.Alias == "" {
		return nil
	}

	before, err := es.AliasTargets(ctx)
	if err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	if err := es.PromoteAlias(ctx); err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	if len(before) != 1 || before[0] != rt.cfg.Sink.Index {
		s.log.Info("read alias moved to the newly scanned index",
			"stream", rt.cfg.Name, "alias", rt.cfg.Sink.Alias,
			"from", before, "to", rt.cfg.Sink.Index)
	}
	return nil
}
