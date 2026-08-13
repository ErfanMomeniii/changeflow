# changeflow

Keeps Elasticsearch and ClickHouse in step with MySQL by reading the binlog.

changeflow registers with a MySQL master as a replica, decodes row changes, and applies them
to a destination. It is the same mechanism a replica uses, so the source does no work beyond
serving the binlog it already writes: no triggers, no polling on `updated_at`, and no
timestamp column anyone has to keep accurate.

```
MySQL ──binlog──> changeflow ──batches──> Elasticsearch
                       │                  ClickHouse
                       └── position ──> checkpoint table (MySQL)
```

## What it guarantees

- **Nothing is lost.** A position advances only after the destination has acknowledged the
  write. A crash in between replays the batch rather than skipping it.
- **A replay is harmless.** Every document carries a version that only increases.
  Elasticsearch discards a stale version through `version_type=external`; ClickHouse keeps
  the highest with `ReplacingMergeTree(_version, _is_deleted)`.
- **Deletes propagate**, including the delete implied by a row's key changing.
- **Existing rows arrive.** A first run scans the table before streaming, so a destination
  holds the whole table rather than only what changed after it was switched on.
- **Two processes cannot fight.** A stream holds an advisory lock for its lifetime, so a
  second process running the same stream refuses to start instead of both writing.

Each of those has an end-to-end test that kills or restarts the process and checks the
outcome against a real MySQL and a real destination.

## Requirements

On the source:

| Setting | Value | Why |
|---|---|---|
| `binlog_format` | `ROW` | Statement logging carries SQL, not values |
| `binlog_row_image` | `FULL` | A partial image cannot fill a document |
| `binlog_row_metadata` | `FULL` | Column names come from the binlog rather than from guessing |
| `gtid_mode` | `ON` | A GTID set is a position that survives a failover |
| `enforce_gtid_consistency` | `ON` | Required alongside `gtid_mode` |
| `server_id` | unique | Colliding with another replica disconnects both |

Check a server before configuring anything else:

```
changeflow preflight --dsn 'cdc:cdc@tcp(127.0.0.1:3306)/'
```

Grants, which are the least a stream needs:

```sql
CREATE USER 'cdc'@'%' IDENTIFIED BY '...';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'%';
GRANT SELECT ON `shop`.* TO 'cdc'@'%';
-- The checkpoint store, which can be any MySQL the process may write to.
GRANT SELECT, INSERT, UPDATE, DELETE ON `changeflow_meta`.* TO 'cdc'@'%';
```

The checkpoint table is created on first run if the user may issue DDL. In production, apply
that DDL with a migration tool and leave the running service only the rights above.

## Quick start

```
make dev-up            # a CDC-configured MySQL on port 13306, seeded
make dev-sinks         # Elasticsearch on 19200 and ClickHouse on 18123
make build

./bin/changeflow validate -c deploy/dev/changeflow.yaml
./bin/changeflow generate-schema -c deploy/dev/changeflow.yaml --stream orders_to_es > /tmp/orders-v1.json
curl -XPUT localhost:19200/orders-v1 -H 'Content-Type: application/json' -d @/tmp/orders-v1.json
./bin/changeflow run -c deploy/dev/changeflow.yaml --stream orders_to_es
```

The seeded table already has rows, which no binlog event mentions; they arrive from the
initial scan. Then change one and watch it follow:

```
mysql -h127.0.0.1 -P13306 -ucdc -pcdc shop -e "UPDATE orders SET status='shipped' WHERE id=1"
curl -s localhost:19200/orders-v1/_doc/1 | jq .
```

## Configuration

```yaml
source:
  dsn: "${MYSQL_DSN}"             # cdc:secret@tcp(mysql:3306)/shop
  server_id: 1001                 # unique across every replica of this master
  snapshot_dsn: "${REPLICA_DSN}"   # optional: read table scans from a replica instead
  time_zone: UTC                  # how DATETIME columns are interpreted
  heartbeat_period: 5s            # how quickly a silent connection is noticed
  read_timeout: 90s
  tls: preferred                  # passed to the MySQL driver: preferred, true, skip-verify, false

checkpoint:
  dsn: "${META_DSN}"
  table: changeflow_checkpoints

runtime:
  buffer_size: 8192               # events queued per stream before the reader waits
  shutdown_grace: 30s
  metrics_addr: "127.0.0.1:9187"  # "off" disables the endpoint
  assumed_row_bytes: 1KiB         # only for the memory projection validate prints

streams:
  orders_to_es:
    table: shop.orders
    snapshot:
      enabled: true
      chunk_size: 5000
      max_rate_rows_per_sec: 20000  # a ceiling on what a backfill costs the source
    batch:
      max_rows: 1000
      max_bytes: 5MiB
      flush_interval: 500ms
    sink:
      type: elasticsearch
      addresses: ["${ES_URL}"]
      index: orders-v1            # the concrete index written to
      alias: orders               # what readers query; moved after a rebuild
      workers: 4
    mapping:
      key: [id]                   # defaults to the primary key
      include: []                 # empty means every column
      exclude: [internal_note]
      rename: {total_amount: total}
      on_zero_date: "null"        # null, error, or epoch, for MySQL's 0000-00-00
```

An unknown field is an error, not a setting silently ignored. `changeflow validate` checks
the whole file, reports every problem at once, and connects to nothing.

### Where the mapping comes from

Column types, the primary key, `ENUM` and `SET` members, and generated columns are read from
`information_schema` at startup. `generate-schema` prints the destination schema those types
imply, to review and apply by hand — changeflow never issues DDL to a destination:

```
changeflow generate-schema -c changeflow.yaml --stream orders_to_es          # an ES mapping
changeflow generate-schema -c changeflow.yaml --stream orders_to_clickhouse  # ClickHouse DDL
```

The choices that matter, and what getting them wrong costs:

| MySQL | Elasticsearch | ClickHouse | Why |
|---|---|---|---|
| `BIGINT UNSIGNED` | `unsigned_long` | `UInt64` | `long` wraps every value above 2^63 |
| `DECIMAL(p,s)` | `keyword` | `Decimal(p,s)` | A float loses the exact amount |
| `DATETIME` | `date` | `DateTime64` | A wall clock reading, interpreted in `source.time_zone` |
| `TIMESTAMP` | `date` | `DateTime64(3,'UTC')` | An instant, always UTC |
| `ENUM` | `keyword` | `LowCardinality(String)` | The label, not the member number |
| `SET` | `keyword` array | `Array(String)` | The members, not the bitmask |

Generated columns never appear in a binlog row image, so they are not replicated; declare
them in the destination if it needs them. At startup the destination's actual mapping is
compared with what the stream will write, and a mismatch is a refusal to start rather than a
field silently dropped.

## Running

```
changeflow run -c changeflow.yaml                  # every stream, one connection to the source
changeflow run -c changeflow.yaml --stream orders  # one stream alone
changeflow status -c changeflow.yaml               # position, lag and snapshot state per stream
```

Every stream on a source shares one binlog connection. Ten streams reading separately would
cost the master ten dump threads and ten copies of the same data. The cost of sharing is
shared back pressure: a slow destination on one stream slows the others, which
`changeflow_queue_depth` makes visible. `--stream` is the remedy — run the noisy one in its
own process.

`SIGTERM` finishes the batch in flight, records the position, and exits within
`shutdown_grace`.

## Operating

### Metrics and health

Prometheus at `runtime.metrics_addr`, alongside `/healthz` (the process is alive) and
`/readyz` (every stream is streaming and has applied something recently).

| Metric | Meaning |
|---|---|
| `changeflow_binlog_lag_seconds` | Age of the last applied change. The number to alert on |
| `changeflow_queue_depth` | Events waiting for a destination. Sustained high means the sink is the bottleneck |
| `changeflow_events_total` | Changes read, by operation |
| `changeflow_documents_written_total` | Documents the destination accepted |
| `changeflow_documents_stale_total` | Replays discarded as older than what is stored. Expected after a restart |
| `changeflow_sink_errors_total` | Failed writes, before retries |
| `changeflow_dead_lettered_total` | Documents the destination refused. Above zero means it is missing data |
| `changeflow_checkpoint_age_seconds` | Time since the position was last recorded |
| `changeflow_snapshot_rows_done` / `_estimated` | Backfill progress |

Alert rules: [`deploy/prometheus/alerts.yml`](deploy/prometheus/alerts.yml).

### Lag is rising

1. `changeflow_queue_depth` high — the destination is the bottleneck. Raise `sink.workers`
   or `batch.max_rows`, or move the stream into its own process with `--stream`.
2. Queue depth near zero — the source or the network is the bottleneck. Look for a large
   transaction or a schema change being processed.
3. Neither — check `changeflow_sink_errors_total` for writes being refused and retried.

A stream that is not streaming at all reports why in `status`, and its last error is kept in
the checkpoint row.

### A document was refused

A refused document is written to the dead letter directory (`--dlq-dir`) and flushed to disk
before the position advances, so the stream continues and nothing is dropped silently. Each
line records the stream, key, version, status, and reason.

```
ls /var/lib/changeflow/dlq/
```

`changeflow_dead_lettered_total` above zero means those rows are missing from the
destination. Fix the cause — usually a mapping that cannot hold a value — then rebuild.

### Changing a mapping

A mapping change is a rebuild into a new index, with readers moved across at the end:

```
changeflow generate-schema -c changeflow.yaml --stream orders_to_es > orders-v2.json
# review the diff against orders-v1, then create the index
curl -XPUT $ES/orders-v2 -H 'Content-Type: application/json' -d @orders-v2.json
# point sink.index at orders-v2, leaving sink.alias as orders
changeflow resnapshot -c changeflow.yaml --stream orders_to_es --confirm
changeflow run -c changeflow.yaml --stream orders_to_es
```

While the scan fills `orders-v2`, readers stay on `orders-v1` through the alias. Refreshing
and replication are turned off for the scan and restored afterwards, which is a large part of
how long a rebuild takes. When the scan finishes, one atomic request moves the alias, and
`orders-v1` is still there to move back to:

```
curl -XPOST $ES/_aliases -H 'Content-Type: application/json' -d '{"actions":[
  {"remove":{"index":"orders-v2","alias":"orders"}},
  {"add":{"index":"orders-v1","alias":"orders","is_write_index":true}}]}'
```

An interrupted rebuild leaves readers where they were: the alias moves only after a complete
scan.

A rebuild is also how a lost checkpoint is recovered from, and the only way to remove rows
deleted while the process was stopped for longer than the binlog is retained. Those deletes
are no longer in the binlog, so only a rescan can reconcile them.

### Adding a column

1. Add it in MySQL. Streams keep running: columns are matched by name, so a width change
   does not stall anything.
2. If a stream should carry it, regenerate the schema, apply it, and rebuild as above. Until
   then the column is simply not written.

### Restoring a stream after a long outage

`changeflow status` shows the recorded position. If the binlog holding it has been purged,
the stream cannot resume and says so; recover with `resnapshot`. Otherwise it catches up on
its own, and `--stream` lets it do that without its siblings competing for the destination.

## Development

```
make check              # format, vet, and unit tests with the race detector
make dev-up             # MySQL
make dev-sinks          # Elasticsearch and ClickHouse (needs ~3 GB of free disk)
make test-integration   # tests that need a real MySQL
make test-e2e           # the full simulation against all three
make bench              # throughput and allocation benchmarks
make budgets            # assert the performance budgets
```

`changeflow tail --dsn ... --table shop.orders` prints decoded row changes without writing
anywhere, which is the quickest way to see what a source actually emits.

## PHP companion

`sdk/php` reads what the daemon writes, with no Go involved:

```php
$status = new Changeflow\Status($pdo, 'changeflow_meta.changeflow_checkpoints');
foreach ($status->streams() as $stream) {
    printf("%s lag=%.1fs snapshot=%s\n",
        $stream->stream, $stream->lagSeconds() ?? 0.0,
        $stream->snapshotDone
            ? 'done'
            : sprintf('%.0f%%', 100 * ($stream->snapshotProgress() ?? 0.0)));
}

$yaml = (new Changeflow\ConfigBuilder())
    ->source('${MYSQL_DSN}', 1001)
    ->checkpoint('${META_DSN}')
    ->elasticsearchStream('orders_to_es', 'shop.orders', 'orders-v1', ['${ES_URL}'], ['id'])
    ->toYaml();
```

Generated configuration is validated in CI by `changeflow validate`, so a model change that
breaks the export fails the build rather than production. The package needs only `SELECT` on
the checkpoint table: positions belong to the daemon.
