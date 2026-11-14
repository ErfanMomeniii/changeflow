package supervisor

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/erfanmomeniii/changeflow/internal/cdc"
	"github.com/erfanmomeniii/changeflow/internal/checkpoint"
	"github.com/erfanmomeniii/changeflow/internal/config"
	"github.com/erfanmomeniii/changeflow/internal/pipeline"
	"github.com/erfanmomeniii/changeflow/internal/schema"
	"github.com/erfanmomeniii/changeflow/internal/sink"
	"github.com/erfanmomeniii/changeflow/internal/sink/clickhouse"
	"github.com/erfanmomeniii/changeflow/internal/sink/dlq"
	"github.com/erfanmomeniii/changeflow/internal/sink/elasticsearch"
)

func (s *Supervisor) prepare(ctx context.Context, sess *session) ([]*streamRuntime, error) {
	zone, err := time.LoadLocation(s.cfg.Source.TimeZone)
	if err != nil {
		return nil, fmt.Errorf("supervisor: source.time_zone %q: %w", s.cfg.Source.TimeZone, err)
	}
	runtimes := make([]*streamRuntime, 0, len(s.streams))
	for _, stream := range s.streams {
		rt := &streamRuntime{cfg: stream, metrics: s.metrics[stream.Name]}
		runtimes = append(runtimes, rt)
		if err := s.buildStream(ctx, sess, rt, zone); err != nil {
			return runtimes, err
		}
	}
	return runtimes, nil
}

func (s *Supervisor) buildStream(ctx context.Context, sess *session, rt *streamRuntime, zone *time.Location) error {
	stream := rt.cfg
	lock, err := sess.store.Lock(ctx, stream.Name)
	if err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	rt.lock = lock
	rt.meta, err = sess.schemas.Table(ctx, stream.Schema(), stream.TableName())
	if err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	dialect, err := dialectFor(stream.Sink.Type)
	if err != nil {
		return err
	}
	rt.plan, err = pipeline.Compile(rt.meta, stream.Mapping, dialect, zone, stream.Mapping.OnZeroDate)
	if err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	if rt.sink, err = buildSink(stream); err != nil {
		return err
	}
	if err := s.validateDestination(ctx, stream, rt.sink, rt.meta); err != nil {
		return err
	}
	if rt.dlq, err = dlq.New(dlq.Options{Dir: s.dlqDir, Stream: stream.Name}); err != nil {
		return err
	}
	rt.alloc, err = checkpoint.NewAllocator(ctx, sess.store, stream.Name, seqBlockSize, time.Now)
	if err != nil {
		return err
	}
	if rt.runner, err = s.buildRunner(rt, sess.store); err != nil {
		return err
	}
	rt.events = make(chan cdc.ChangeEvent, s.cfg.Runtime.BufferSize)
	return nil
}

func (s *Supervisor) buildRunner(rt *streamRuntime, store *checkpoint.MySQLStore) (*pipeline.Runner, error) {
	stream := rt.cfg
	return pipeline.NewRunner(pipeline.RunnerOptions{
		Stream: stream.Name,
		Plan:   rt.plan,
		Sink:   rt.sink,
		DLQ:    rt.dlq,
		Store:  store,
		Limits: pipeline.Limits{
			MaxRows:       stream.Batch.MaxRows,
			MaxBytes:      stream.Batch.MaxBytes.Bytes(),
			FlushInterval: stream.Batch.FlushInterval.Duration(),
		},
		ShutdownGrace: s.cfg.Runtime.ShutdownGrace.Duration(),
		Observer:      &streamObserver{metrics: rt.metrics, state: &rt.state},
		Logger:        s.log,
	})
}

func (s *Supervisor) release(ctx context.Context, runtimes []*streamRuntime) {
	ctx = context.WithoutCancel(ctx)
	for _, rt := range runtimes {
		if rt.sink != nil {
			rt.sink.Close()
		}
		if rt.dlq != nil {
			rt.dlq.Close()
		}
		if rt.lock != nil {
			if err := rt.lock.Release(ctx); err != nil {
				s.log.Warn("could not release the stream lock", "stream", rt.cfg.Name, "error", err)
			}
		}
	}
}

func (s *Supervisor) validateDestination(ctx context.Context, stream *config.Stream, destination sink.Sink, meta *schema.TableMeta) error {
	es, ok := destination.(*elasticsearch.Sink)
	if !ok {
		return nil
	}
	key, err := meta.ResolveKey(stream.Mapping.Key)
	if err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	expected, err := schema.ExpectedElasticsearchFields(meta,
		stream.Mapping.Include, stream.Mapping.Exclude, key, stream.Mapping.Rename)
	if err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	if err := es.ValidateMapping(ctx, expected); err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	s.log.Info("destination schema matches the stream",
		"stream", stream.Name, "index", stream.Sink.Index, "fields", len(expected))
	return nil
}

func buildSink(stream *config.Stream) (sink.Sink, error) {
	switch stream.Sink.Type {
	case config.SinkElasticsearch:
		return elasticsearch.New(elasticsearch.Options{
			Addresses: stream.Sink.Addresses,
			Index:     stream.Sink.Index,
			Alias:     stream.Sink.Alias,
			Workers:   stream.Sink.Workers,
			Compress:  true,
		})
	case config.SinkClickHouse:
		return clickhouse.New(clickhouse.Options{
			DSN:      stream.Sink.DSN,
			Table:    stream.Sink.Table,
			Workers:  stream.Sink.Workers,
			Compress: true,
		})
	default:
		return nil, fmt.Errorf("supervisor: sink type %q is not implemented", stream.Sink.Type)
	}
}

func dialectFor(sinkType string) (pipeline.Dialect, error) {
	switch sinkType {
	case config.SinkElasticsearch:
		return pipeline.DialectElasticsearch, nil
	case config.SinkClickHouse:
		return pipeline.DialectClickHouse, nil
	default:
		return 0, fmt.Errorf("supervisor: no encoding known for sink type %q", sinkType)
	}
}

func estimateRows(ctx context.Context, db *sql.DB, meta *schema.TableMeta) uint64 {
	var rows sql.NullInt64
	err := db.QueryRowContext(ctx,
		"SELECT table_rows FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		meta.Schema, meta.Table).Scan(&rows)
	if err != nil || !rows.Valid || rows.Int64 < 0 {
		return 0
	}
	return uint64(rows.Int64)
}
