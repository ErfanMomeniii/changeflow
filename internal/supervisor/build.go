package supervisor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/ErfanMomeniii/changeflow/internal/cdc"
	"github.com/ErfanMomeniii/changeflow/internal/checkpoint"
	"github.com/ErfanMomeniii/changeflow/internal/config"
	"github.com/ErfanMomeniii/changeflow/internal/pipeline"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
	"github.com/ErfanMomeniii/changeflow/internal/sink"
	"github.com/ErfanMomeniii/changeflow/internal/sink/clickhouse"
	"github.com/ErfanMomeniii/changeflow/internal/sink/dlq"
	"github.com/ErfanMomeniii/changeflow/internal/sink/elasticsearch"
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

func currentPosition(ctx context.Context, db *sql.DB) (string, error) {
	var gtid string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&gtid); err != nil {
		return "", fmt.Errorf("supervisor: read source position: %w", err)
	}
	gtid = strings.ReplaceAll(gtid, "\n", "")
	if strings.TrimSpace(gtid) == "" {
		return "", errors.New("supervisor: the source has logged no transactions, so there is no position to start from")
	}
	return gtid, nil
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

func openMySQL(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	db.SetConnMaxLifetime(time.Hour)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func addressOf(dsn string) (string, uint16, error) {
	cfg, err := driver.ParseDSN(dsn)
	if err != nil {
		return "", 0, fmt.Errorf("supervisor: parse dsn: %w", err)
	}
	if cfg.Net != "tcp" {
		return "", 0, fmt.Errorf("supervisor: replication needs a tcp dsn, got %q", cfg.Net)
	}
	host, portStr, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return strings.Trim(cfg.Addr, "[]"), 3306, nil
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return "", 0, fmt.Errorf("supervisor: parse port in %q: %w", cfg.Addr, err)
	}
	return host, uint16(port), nil
}

func usernameOf(dsn string) string {
	if cfg, err := driver.ParseDSN(dsn); err == nil {
		return cfg.User
	}
	return ""
}

func passwordOf(dsn string) string {
	if cfg, err := driver.ParseDSN(dsn); err == nil {
		return cfg.Passwd
	}
	return ""
}
