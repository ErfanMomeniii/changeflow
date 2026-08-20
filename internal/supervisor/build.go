package supervisor

// Construction of the pieces a stream is assembled from, and the source lookups
// that assembly needs.

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

	"github.com/ErfanMomeniii/changeflow/internal/config"
	"github.com/ErfanMomeniii/changeflow/internal/pipeline"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
	"github.com/ErfanMomeniii/changeflow/internal/sink"
	"github.com/ErfanMomeniii/changeflow/internal/sink/clickhouse"
	"github.com/ErfanMomeniii/changeflow/internal/sink/elasticsearch"
)

// validateDestination compares a destination's schema against what its stream will write.
func (s *Supervisor) validateDestination(ctx context.Context, stream *config.Stream, destination sink.Sink, meta *schema.TableMeta) error {
	es, ok := destination.(*elasticsearch.Sink)
	if !ok {
		// Nothing to check for a destination whose schema changeflow cannot read.
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

// estimateRows reads the optimiser's row estimate, which drives a progress percentage only.
// It is approximate by nature, and nothing depends on its accuracy.
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
	// One connection per stream for locks, plus a few for schema and checkpoint reads.
	db.SetMaxOpenConns(16)
	db.SetConnMaxLifetime(time.Hour)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// addressOf extracts the replication endpoint from a DSN. Replication uses its own
// connection rather than the SQL driver, so it needs the parts rather than the DSN.
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
