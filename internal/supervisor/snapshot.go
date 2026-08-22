package supervisor

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

func (s *Supervisor) backfill(ctx context.Context, sess *session, runtimes []*streamRuntime) (map[string]string, error) {
	positions := make(map[string]string, len(runtimes))
	for _, rt := range runtimes {
		position, err := s.snapshotIfNeeded(ctx, sess, rt)
		if err != nil {
			return nil, err
		}
		positions[rt.cfg.Name] = position
	}
	return positions, nil
}

func (s *Supervisor) snapshotIfNeeded(ctx context.Context, sess *session, rt *streamRuntime) (string, error) {
	cp, err := sess.store.Load(ctx, rt.cfg.Name)
	if err != nil && !errors.Is(err, checkpoint.ErrNotFound) {
		return "", fmt.Errorf("supervisor: read checkpoint: %w", err)
	}
	if cp.SnapshotDone && cp.GTIDSet != "" {
		return cp.GTIDSet, nil
	}
	if !rt.cfg.Snapshot.Enabled {
		return s.startWithoutSnapshot(ctx, sess, rt, cp)
	}
	cp, err = s.markScanStarted(ctx, sess, rt, cp)
	if err != nil {
		return "", err
	}
	return s.runScan(ctx, sess, rt, cp)
}

func (s *Supervisor) startWithoutSnapshot(
	ctx context.Context,
	sess *session,
	rt *streamRuntime,
	cp checkpoint.Checkpoint,
) (string, error) {
	if cp.GTIDSet != "" {
		return cp.GTIDSet, nil
	}
	position, err := currentPosition(ctx, sess.sourceDB)
	if err != nil {
		return "", err
	}
	s.log.Warn("snapshots are disabled, so rows written before now will not appear in the destination",
		"stream", rt.cfg.Name, "from", position)
	cp.Stream, cp.SnapshotDone, cp.GTIDSet = rt.cfg.Name, true, position
	if err := sess.store.Save(ctx, cp); err != nil {
		return "", fmt.Errorf("supervisor: record start position: %w", err)
	}
	return position, nil
}

func (s *Supervisor) markScanStarted(
	ctx context.Context,
	sess *session,
	rt *streamRuntime,
	cp checkpoint.Checkpoint,
) (checkpoint.Checkpoint, error) {
	if cp.SnapshotStartGTID != "" {
		s.log.Info("resuming a table scan",
			"stream", rt.cfg.Name, "rows_done", cp.SnapshotRowsDone, "estimated_rows", cp.SnapshotRowsTotal)
		return cp, nil
	}
	position, err := currentPosition(ctx, sess.sourceDB)
	if err != nil {
		return cp, err
	}
	baseSeq, err := rt.alloc.Next(ctx)
	if err != nil {
		return cp, fmt.Errorf("supervisor: allocate the snapshot version: %w", err)
	}
	cp.Stream = rt.cfg.Name
	cp.SnapshotStartGTID = position
	cp.SnapshotBaseSeq = baseSeq
	cp.SnapshotRowsTotal = estimateRows(ctx, sess.sourceDB, rt.meta)
	if err := sess.store.Save(ctx, cp); err != nil {
		return cp, fmt.Errorf("supervisor: record the snapshot start: %w", err)
	}
	s.log.Info("starting a table scan",
		"stream", rt.cfg.Name, "table", rt.cfg.Table,
		"from_position", position, "estimated_rows", cp.SnapshotRowsTotal)
	return cp, nil
}

func (s *Supervisor) runScan(
	ctx context.Context,
	sess *session,
	rt *streamRuntime,
	cp checkpoint.Checkpoint,
) (string, error) {
	scanDB := sess.sourceDB
	if s.cfg.Source.SnapshotDSN != "" {
		replica, err := openMySQL(ctx, s.cfg.Source.SnapshotDSN)
		if err != nil {
			return "", fmt.Errorf("supervisor: connect for the table scan: %w", err)
		}
		defer replica.Close()
		scanDB = replica
	}
	scanner, err := s.newScanner(scanDB, rt, cp)
	if err != nil {
		return "", err
	}
	rt.metrics.SnapshotRunning(true)
	defer rt.metrics.SnapshotRunning(false)
	if err := s.scan(ctx, rt, scanner); err != nil {
		return "", err
	}
	if !scanner.Done() {
		return "", ctx.Err()
	}
	return s.finishScan(ctx, sess, rt)
}

func (s *Supervisor) newScanner(scanDB *sql.DB, rt *streamRuntime, cp checkpoint.Checkpoint) (*snapshot.Snapshotter, error) {
	key, err := rt.meta.ResolveKey(rt.cfg.Mapping.Key)
	if err != nil {
		return nil, fmt.Errorf("supervisor: %w", err)
	}
	estimated := cp.SnapshotRowsTotal
	return snapshot.New(snapshot.Options{
		DB:            scanDB,
		Meta:          rt.meta,
		Key:           key,
		ChunkSize:     rt.cfg.Snapshot.ChunkSize,
		MaxRowsPerSec: rt.cfg.Snapshot.MaxRateRowsPerSec,
		Cursor:        cp.SnapshotCursor,
		BaseSeq:       cp.SnapshotBaseSeq,
		Observe: func(rows uint64) {
			rt.metrics.SnapshotProgress(rows, estimated)
			s.log.Info("table scan progress", "stream", rt.cfg.Name, "rows", rows, "estimated_total", estimated)
		},
		Logger: s.log,
	})
}

func (s *Supervisor) finishScan(ctx context.Context, sess *session, rt *streamRuntime) (string, error) {
	cp, err := sess.store.Load(ctx, rt.cfg.Name)
	if err != nil {
		return "", fmt.Errorf("supervisor: read checkpoint after the scan: %w", err)
	}
	cp.Stream = rt.cfg.Name
	cp.SnapshotDone = true
	cp.SnapshotRowsTotal = cp.SnapshotRowsDone
	if err := sess.store.Save(ctx, cp); err != nil {
		return "", fmt.Errorf("supervisor: record the completed scan: %w", err)
	}
	s.log.Info("table scan complete", "stream", rt.cfg.Name, "rows", cp.SnapshotRowsDone)
	if err := s.promoteAlias(ctx, rt); err != nil {
		return "", err
	}
	return cp.SnapshotStartGTID, nil
}

func (s *Supervisor) scan(ctx context.Context, rt *streamRuntime, scanner *snapshot.Snapshotter) error {
	load, err := s.beginBulkLoad(ctx, rt)
	if err != nil {
		s.log.Warn("could not prepare the destination for a bulk load",
			"stream", rt.cfg.Name, "error", err)
	}
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
	if load.Applied() {
		if err := rt.sink.(*elasticsearch.Sink).ForceMerge(ctx); err != nil {
			s.log.Warn("could not merge the scanned index's segments, which only makes it slower to search",
				"stream", rt.cfg.Name, "index", rt.cfg.Sink.Index, "error", err)
		}
	}
	return nil
}

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
