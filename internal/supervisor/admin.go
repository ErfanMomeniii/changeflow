package supervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/ErfanMomeniii/changeflow/internal/checkpoint"
	"github.com/ErfanMomeniii/changeflow/internal/config"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
	"github.com/ErfanMomeniii/changeflow/internal/sink/dlq"
	"github.com/ErfanMomeniii/changeflow/internal/source"
)

// Status prints each stream's position, lag, snapshot state, and refused documents.
func Status(ctx context.Context, w io.Writer, cfg *config.Config, dlqDir string) error {
	db, err := source.Open(ctx, cfg.Checkpoint.DSN)
	if err != nil {
		return err
	}
	defer db.Close()
	store, err := checkpoint.NewMySQLStore(db, cfg.Checkpoint.Table)
	if err != nil {
		return err
	}
	table := statusTable{w: w, dlqDir: dlqDir}
	table.header()
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
			fmt.Fprintf(w, "%-28s stopped: %s\n", "", cp.LastError)
		}
	}
	return nil
}

type statusTable struct {
	w      io.Writer
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
		fmt.Fprintf(t.w, t.format(), "STREAM", "LAG", "SNAPSHOT", "POSITION")
		return
	}
	fmt.Fprintf(t.w, t.format(), "STREAM", "LAG", "SNAPSHOT", "REFUSED", "POSITION")
}

func (t statusTable) row(name, lag, snapshot, position string) error {
	if t.dlqDir == "" {
		fmt.Fprintf(t.w, t.format(), name, lag, snapshot, position)
		return nil
	}
	refused, err := dlq.Count(t.dlqDir, name)
	if err != nil {
		return err
	}
	fmt.Fprintf(t.w, t.format(), name, lag, snapshot, strconv.Itoa(refused), position)
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

// Resnapshot asks a stream to scan its table again on next start.
func Resnapshot(ctx context.Context, w io.Writer, cfg *config.Config, streamName string, confirm bool) error {
	stream, err := cfg.Stream(streamName)
	if err != nil {
		return err
	}
	if !confirm {
		return fmt.Errorf("rescanning %s reads all of %s and rewrites the destination; pass --confirm to proceed",
			streamName, stream.Table)
	}
	db, err := source.Open(ctx, cfg.Checkpoint.DSN)
	if err != nil {
		return err
	}
	defer db.Close()
	store, err := checkpoint.NewMySQLStore(db, cfg.Checkpoint.Table)
	if err != nil {
		return err
	}
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
	fmt.Fprintf(w, "stream %s will scan %s again on next start\n", streamName, stream.Table)
	if previous > 0 {
		fmt.Fprintf(w, "  the previous scan had read %d rows\n", previous)
	}
	printRebuildAdvice(w, stream)
	return nil
}

func printRebuildAdvice(w io.Writer, stream *config.Stream) {
	switch {
	case stream.Sink.Type == config.SinkElasticsearch && stream.Sink.Alias != "":
		fmt.Fprintf(w, "  point sink.index at a new index before starting, and the read alias %q\n"+
			"  will be moved to it once the scan finishes\n", stream.Sink.Alias)
	case stream.Sink.Type == config.SinkClickHouse:
		fmt.Fprint(w, "  point sink.table at a new table before starting, then swap it in with\n"+
			"  EXCHANGE TABLES once the scan finishes\n")
	}
}

// GenerateSchema renders the destination schema a stream needs, for review and apply.
func GenerateSchema(ctx context.Context, cfg *config.Config, streamName string, shards, replicas int) (schema.Generated, error) {
	stream, err := cfg.Stream(streamName)
	if err != nil {
		return schema.Generated{}, err
	}
	db, err := source.Open(ctx, cfg.Source.DSN)
	if err != nil {
		return schema.Generated{}, err
	}
	defer db.Close()
	meta, err := schema.DBLoader{DB: db}.Load(ctx, stream.Schema(), stream.TableName())
	if err != nil {
		return schema.Generated{}, err
	}
	key, err := meta.ResolveKey(stream.Mapping.Key)
	if err != nil {
		return schema.Generated{}, err
	}
	switch stream.Sink.Type {
	case config.SinkElasticsearch:
		return schema.GenerateElasticsearch(meta,
			stream.Mapping.Include, stream.Mapping.Exclude, key, stream.Mapping.Rename,
			shards, replicas)
	case config.SinkClickHouse:
		return schema.GenerateClickHouse(meta,
			stream.Mapping.Include, stream.Mapping.Exclude, key, stream.Mapping.Rename,
			stream.Sink.Table)
	default:
		return schema.Generated{}, fmt.Errorf("no schema generator for sink type %q", stream.Sink.Type)
	}
}
