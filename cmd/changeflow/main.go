// Command changeflow replicates MySQL changes into downstream stores.
//
// Subcommands: "run" replicates a configured stream, "status" reports each
// stream's position and lag, "validate" checks a configuration file, and
// "preflight" and "tail" are diagnostics against a source server.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/ErfanMomeniii/changeflow/internal/checkpoint"
	"github.com/ErfanMomeniii/changeflow/internal/config"
	"github.com/ErfanMomeniii/changeflow/internal/preflight"
	"github.com/ErfanMomeniii/changeflow/internal/schema"
	"github.com/ErfanMomeniii/changeflow/internal/supervisor"
	"github.com/ErfanMomeniii/changeflow/internal/tail"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "run":
		err = runStream(ctx, os.Args[2:])
	case "status":
		err = runStatus(ctx, os.Args[2:])
	case "validate":
		err = runValidate(os.Args[2:])
	case "generate-schema":
		err = runGenerateSchema(ctx, os.Args[2:])
	case "resnapshot":
		err = runResnapshot(ctx, os.Args[2:])
	case "tail":
		err = runTail(ctx, os.Args[2:])
	case "preflight":
		err = runPreflight(ctx, os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `changeflow - MySQL change data capture

Usage:
  changeflow run -c <config.yaml> --stream <name>
        Replicate one configured stream until interrupted.

  changeflow status -c <config.yaml>
        Report each stream's position, lag, and snapshot state.

  changeflow validate -c <config.yaml>
        Check a configuration file without connecting to anything.

  changeflow generate-schema -c <config.yaml> --stream <name>
        Print the destination schema for a stream, to review and apply.

  changeflow resnapshot -c <config.yaml> --stream <name> --confirm
        Ask a stream to scan its table again on next start.

  changeflow preflight --dsn <dsn>
        Check whether a MySQL server is configured for CDC.

  changeflow tail --dsn <dsn> [--table db.table] [--server-id N]
                  [--capture DIR] [--for 60s]
        Register as a replica and print decoded row changes.

DSN format:
  user:password@tcp(host:3306)/

`)
}

func runValidate(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	path := fs.String("c", "changeflow.yaml", "path to the configuration file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	// A configuration whose buffers and in-flight batches exceed the process memory
	// limit is a scheduled crash, so it is better refused here.
	if err := cfg.CheckMemoryLimit(debug.SetMemoryLimit(-1)); err != nil {
		return err
	}

	fmt.Printf("configuration is valid: %d stream(s), about %s of buffers and in-flight batches\n",
		len(cfg.Streams), config.ByteSize(cfg.EstimatedMemory()))
	for _, name := range cfg.StreamNames() {
		s := cfg.Streams[name]
		fmt.Printf("  %-28s %s -> %s\n", name, s.Table, s.Sink.Type)
	}
	return nil
}

// runGenerateSchema prints the destination schema for a stream.
//
// It is deliberately a printer rather than an applier: changeflow never issues DDL to
// a destination. The output is committed and applied by whatever migration tooling the
// project already uses, so a schema change is reviewed like any other change.
func runGenerateSchema(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("generate-schema", flag.ExitOnError)
	path := fs.String("c", "changeflow.yaml", "path to the configuration file")
	streamName := fs.String("stream", "", "which configured stream to generate for")
	shards := fs.Int("shards", 1, "number_of_shards for a generated Elasticsearch index")
	replicas := fs.Int("replicas", 1, "number_of_replicas for a generated Elasticsearch index")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *streamName == "" {
		return errors.New("--stream is required")
	}

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	stream, err := cfg.Stream(*streamName)
	if err != nil {
		return err
	}

	db, err := open(ctx, cfg.Source.DSN)
	if err != nil {
		return err
	}
	defer db.Close()

	// Read from the live source, so what is generated matches what will be replicated
	// rather than what someone remembers the table looking like.
	meta, err := schema.DBLoader{DB: db}.Load(ctx, stream.Schema(), stream.TableName())
	if err != nil {
		return err
	}
	key, err := meta.ResolveKey(stream.Mapping.Key)
	if err != nil {
		return err
	}

	var generated schema.Generated
	switch stream.Sink.Type {
	case config.SinkElasticsearch:
		generated, err = schema.GenerateElasticsearch(meta,
			stream.Mapping.Include, stream.Mapping.Exclude, key, stream.Mapping.Rename,
			*shards, *replicas)
	case config.SinkClickHouse:
		generated, err = schema.GenerateClickHouse(meta,
			stream.Mapping.Include, stream.Mapping.Exclude, key, stream.Mapping.Rename,
			stream.Sink.Table)
	default:
		return fmt.Errorf("no schema generator for sink type %q", stream.Sink.Type)
	}
	if err != nil {
		return err
	}

	// The schema goes to stdout so it can be redirected into a file, while the notes
	// go to stderr so they do not end up inside it.
	fmt.Print(generated.Body)
	for _, warning := range generated.Warnings {
		fmt.Fprintf(os.Stderr, "note: %s\n", warning)
	}
	return nil
}

// runResnapshot asks a stream to scan its table again.
//
// The scan itself happens on the next start, not here: this only clears the state that
// records one as finished. Rebuilding is how a mapping change is applied and how a lost
// checkpoint is recovered from, and it is deliberately a separate, explicit step rather
// than something a running process decides to do.
func runResnapshot(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("resnapshot", flag.ExitOnError)
	path := fs.String("c", "changeflow.yaml", "path to the configuration file")
	streamName := fs.String("stream", "", "which configured stream to rescan")
	confirm := fs.Bool("confirm", false, "required: rescanning reads the whole table")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *streamName == "" {
		return errors.New("--stream is required")
	}

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	stream, err := cfg.Stream(*streamName)
	if err != nil {
		return err
	}

	if !*confirm {
		// A full table read costs real time and load on the source, so it is not
		// something to trigger by mistyping a stream name.
		return fmt.Errorf("rescanning %s reads all of %s and rewrites the destination; pass --confirm to proceed",
			*streamName, stream.Table)
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
	lock, err := store.Lock(ctx, *streamName)
	if err != nil {
		if errors.Is(err, checkpoint.ErrStreamLocked) {
			return fmt.Errorf("stream %s is running; stop it before asking for a rescan", *streamName)
		}
		return err
	}
	defer lock.Release(ctx)

	cp, err := store.Load(ctx, *streamName)
	switch {
	case errors.Is(err, checkpoint.ErrNotFound), errors.Is(err, checkpoint.ErrNotInitialized):
		return fmt.Errorf("stream %s has never run, so its next start will scan anyway", *streamName)
	case err != nil:
		return err
	}

	previous := cp.SnapshotRowsDone
	cp.ClearSnapshot()
	if err := store.Save(ctx, cp); err != nil {
		return err
	}

	fmt.Printf("stream %s will scan %s again on next start\n", *streamName, stream.Table)
	if previous > 0 {
		fmt.Printf("  the previous scan had read %d rows\n", previous)
	}
	if stream.Sink.Type == config.SinkElasticsearch && stream.Sink.Alias != "" {
		fmt.Printf("  point sink.index at a new index before starting, and the read alias %q\n"+
			"  will be moved to it once the scan finishes\n", stream.Sink.Alias)
	}
	if stream.Sink.Type == config.SinkClickHouse {
		fmt.Printf("  point sink.table at a new table before starting, then swap it in with\n" +
			"  EXCHANGE TABLES once the scan finishes\n")
	}
	return nil
}

func runStream(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	path := fs.String("c", "changeflow.yaml", "path to the configuration file")
	stream := fs.String("stream", "", "which configured stream to run")
	dlqDir := fs.String("dlq-dir", "dlq", "directory for records of documents a destination refused")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *stream == "" {
		return errors.New("--stream is required; run one stream per process")
	}

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	if err := cfg.CheckMemoryLimit(debug.SetMemoryLimit(-1)); err != nil {
		return err
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	sup, err := supervisor.New(cfg, *stream, *dlqDir, log)
	if err != nil {
		return err
	}
	return sup.Run(ctx)
}

func runStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	path := fs.String("c", "changeflow.yaml", "path to the configuration file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*path)
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
	fmt.Printf("%-28s %-10s %-12s %s\n", "STREAM", "LAG", "SNAPSHOT", "POSITION")

	// Before any stream has run the table does not exist yet, which is worth
	// reporting plainly rather than as a failure.
	if _, err := store.Load(ctx, cfg.StreamNames()[0]); errors.Is(err, checkpoint.ErrNotInitialized) {
		for _, name := range cfg.StreamNames() {
			fmt.Printf("%-28s %-10s %-12s %s\n", name, "-", "not started", "-")
		}
		return nil
	}
	for _, name := range cfg.StreamNames() {
		cp, err := store.Load(ctx, name)
		if errors.Is(err, checkpoint.ErrNotFound) {
			fmt.Printf("%-28s %-10s %-12s %s\n", name, "-", "not started", "-")
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
		fmt.Printf("%-28s %-10s %-12s %s\n", name, lag, snapshot, cp.GTIDSet)
	}
	return nil
}

type commonFlags struct {
	dsn      string
	serverID uint
}

func (c *commonFlags) bind(fs *flag.FlagSet) {
	fs.StringVar(&c.dsn, "dsn", os.Getenv("CHANGEFLOW_DSN"), "MySQL DSN, e.g. user:pass@tcp(host:3306)/ (env: CHANGEFLOW_DSN)")
	fs.UintVar(&c.serverID, "server-id", 1001, "replica server_id; must be unique among replicas of this master")
}

func runPreflight(ctx context.Context, args []string) error {
	var cf commonFlags
	fs := flag.NewFlagSet("preflight", flag.ExitOnError)
	cf.bind(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if cf.dsn == "" {
		return fmt.Errorf("--dsn is required (or set CHANGEFLOW_DSN)")
	}

	db, err := open(ctx, cf.dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	report, err := preflight.Run(ctx, preflight.DBReader{DB: db})
	if err != nil {
		return err
	}
	printReport(report)
	if !report.OK() {
		return fmt.Errorf("%d required check(s) failed", len(report.Failures()))
	}
	return nil
}

func runTail(ctx context.Context, args []string) error {
	var (
		cf         commonFlags
		tables     stringList
		captureDir string
		duration   time.Duration
		skipCheck  bool
	)
	fs := flag.NewFlagSet("tail", flag.ExitOnError)
	cf.bind(fs)
	fs.Var(&tables, "table", "db.table to watch; repeatable, default all tables")
	fs.StringVar(&captureDir, "capture", "", "directory to write raw event fixtures into")
	fs.DurationVar(&duration, "for", 0, "stop after this long; 0 means run until interrupted")
	fs.BoolVar(&skipCheck, "skip-preflight", false, "stream even if required checks fail")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if cf.dsn == "" {
		return fmt.Errorf("--dsn is required (or set CHANGEFLOW_DSN)")
	}

	cfg, err := driver.ParseDSN(cf.dsn)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	// Replication runs over its own TCP connection rather than through the SQL
	// driver, so a unix socket DSN cannot be used for it.
	if cfg.Net != "tcp" {
		return fmt.Errorf("replication needs a tcp dsn, got %q; use user:pass@tcp(host:3306)/", cfg.Net)
	}
	host, port, err := splitHostPort(cfg.Addr)
	if err != nil {
		return err
	}

	db, err := open(ctx, cf.dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	report, err := preflight.Run(ctx, preflight.DBReader{DB: db})
	if err != nil {
		return err
	}
	printReport(report)
	if !report.OK() && !skipCheck {
		return fmt.Errorf("%d required check(s) failed; fix the server or pass --skip-preflight to stream anyway", len(report.Failures()))
	}

	// A replica claiming the source's own id is rejected by the server, and the
	// resulting error names neither side, so catch it here.
	if sourceID, ok := report.Get("server_id"); ok && sourceID == strconv.FormatUint(uint64(cf.serverID), 10) {
		return fmt.Errorf("--server-id %d is the source's own server_id; pick a different one", cf.serverID)
	}

	// Start from the server's current position. Rows written before now produce
	// no further binlog events, so they are invisible here; reading existing rows
	// requires a table scan rather than the binlog.
	var gtid string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&gtid); err != nil {
		return fmt.Errorf("read gtid_executed: %w", err)
	}
	// A multi-server GTID set is returned across several lines.
	gtid = strings.ReplaceAll(gtid, "\n", "")
	if strings.TrimSpace(gtid) == "" {
		return errors.New("gtid_executed is empty: the server has logged no transactions yet, so there is no position to stream from; write a row first")
	}

	return tail.Tail(ctx, tail.Config{
		Host:       host,
		Port:       port,
		User:       cfg.User,
		Password:   cfg.Passwd,
		ServerID:   uint32(cf.serverID),
		Tables:     tables,
		CaptureDir: captureDir,
		Duration:   duration,
		StartGTID:  gtid,
		Out:        os.Stdout,
	})
}

func open(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(2)
	// Ping under the caller's context so an unreachable host does not hang past
	// an interrupt.
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect to mysql: %w", err)
	}
	return db, nil
}

func printReport(r preflight.Report) {
	for _, c := range r.Checks {
		mark := "✓"
		if !c.OK {
			mark = "✗"
		}
		fmt.Printf("%s %-28s want=%-10s got=%s\n", mark, c.Name, c.Want, c.Got)
	}
	for _, c := range r.Warnings() {
		fmt.Printf("\nwarning: %s (%s)\n  %s\n", c.Name, c.Severity, c.Why)
	}
	for _, c := range r.Failures() {
		fmt.Printf("\nFAIL: %s want=%s got=%s\n  %s\n", c.Name, c.Want, c.Got, c.Why)
	}
	fmt.Println()
}

// splitHostPort separates a "host:port" address, defaulting the port and handling
// bracketed IPv6 literals, where splitting on the first colon would truncate the
// address.
func splitHostPort(addr string) (string, uint16, error) {
	if addr == "" {
		return "", 0, errors.New("dsn has no server address")
	}

	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		// No port present: treat the whole value as the host. Strip brackets so a
		// bare IPv6 literal is usable.
		host = strings.TrimSuffix(strings.TrimPrefix(addr, "["), "]")
		return host, 3306, nil
	}

	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return "", 0, fmt.Errorf("parse port in %q: %w", addr, err)
	}
	if port == 0 {
		return "", 0, fmt.Errorf("port 0 in %q", addr)
	}
	return host, uint16(port), nil
}

// stringList collects a repeatable flag.
type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
}
