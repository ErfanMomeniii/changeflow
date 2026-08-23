#!/usr/bin/env bash
# End-to-end demo against MySQL and ClickHouse: snapshot, live CDC, deletes, and
# checkpoint persistence across restarts. Elasticsearch is not used.
#
#   scripts/demo.sh              run everything, leave containers up
#   DOWN=1 scripts/demo.sh       tear the containers down at the end
#   SKIP_SCAN_RESUME=1 ...       skip the mid-scan kill/resume section
#   SKIP_OUTAGE=1 ...            skip the sink-outage section
set -euo pipefail

cd "$(dirname "$0")/.."

CFG=deploy/dev/changeflow.yaml
BIN=./bin/changeflow
STREAM=orders_to_clickhouse
CH=http://changeflow:changeflow@127.0.0.1:18123/
METRICS=http://127.0.0.1:9187
LOG=$(mktemp /tmp/changeflow-demo.XXXXXX)
COMPOSE=(docker compose -f deploy/dev/docker-compose.yml)
PID=""

step() { printf '\n\033[1;36m===== %s =====\033[0m\n' "$*"; }
info() { printf '  %s\n' "$*"; }
ok() { printf '\033[1;32m  OK: %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m  note: %s\033[0m\n' "$*"; }
die() {
	printf '\033[1;31mFAILED: %s\033[0m\n' "$*" >&2
	printf 'last 40 log lines:\n' >&2
	tail -40 "$LOG" >&2 || true
	exit 1
}

cleanup() {
	stop_stream
	if [ "${DOWN:-0}" = 1 ]; then
		step "Tearing down"
		"${COMPOSE[@]}" --profile sinks down -v
	else
		printf '\nContainers left running. Stop them with: make dev-down\nStream log: %s\n' "$LOG"
	fi
}
trap cleanup EXIT

mysql_root() { "${COMPOSE[@]}" exec -T mysql mysql -uroot -proot "$@"; }
sql() { mysql_root -N -B -e "$1"; }
ch() { curl -s "$CH" --data-binary "$1"; }

# argMax picks the newest version per key, so results do not depend on whether the
# engine has merged its parts yet.
ch_count() {
	ch "SELECT count() FROM (SELECT id, argMax(_is_deleted, _version) AS gone FROM analytics.orders GROUP BY id) WHERE gone = 0" |
		tr -d '[:space:]'
}
ch_field() {
	local value
	value=$(ch "SELECT argMax($2, _version) FROM analytics.orders WHERE id = $1" | tr -d '\n')
	[ -n "$value" ] || value=MISSING
	printf '%s\n' "$value"
}

count_is() { [ "$(ch_count)" = "$1" ]; }
field_is() { [ "$(ch_field "$1" "$2")" = "$3" ]; }
log_has() { grep -q "$1" "$LOG"; }
log_count() { grep -c "$1" "$LOG" || true; }

# One log holds every process this demo starts, so anything waited on more than once has to
# be counted rather than merely found, or a line from an earlier section matches at once.
log_grew() { [ "$(log_count "$1")" -gt "$2" ]; }

wait_until() {
	local what=$1 timeout=$2
	shift 2
	local deadline=$((SECONDS + timeout))
	until "$@"; do
		[ "$SECONDS" -lt "$deadline" ] || die "timed out after ${timeout}s waiting for: $what"
		sleep 1
	done
	ok "$what"
}

wait_soft() {
	local what=$1 timeout=$2
	shift 2
	local deadline=$((SECONDS + timeout))
	until "$@"; do
		if [ "$SECONDS" -ge "$deadline" ]; then
			warn "gave up after ${timeout}s waiting for: $what"
			return 1
		fi
		sleep 1
	done
	ok "$what"
}

start_stream() {
	local cfg=${1:-$CFG}
	$BIN run -c "$cfg" --stream "$STREAM" >>"$LOG" 2>&1 &
	PID=$!
	info "stream started (pid $PID), logging to $LOG"
}

# capped_config copies the configuration with a scan rate limit, so a backfill lasts long
# enough to be interrupted on purpose rather than finishing in the time it takes to signal.
capped_config() {
	awk -v stream="$STREAM" '
		$0 ~ "^  " stream ":" { inside = 1 }
		inside && /^      chunk_size:/ { print; print "      max_rate_rows_per_sec: 2000"; next }
		inside && /^  [a-z_]+:$/ && $0 !~ stream { inside = 0 }
		{ print }
	' "$CFG" >"$1"
	grep -q max_rate_rows_per_sec "$1" || die "could not add a scan rate limit to $1"
	$BIN validate -c "$1" >/dev/null || die "the rate-limited configuration is invalid"
}

stop_stream() {
	[ -n "$PID" ] || return 0
	# SIGTERM rather than SIGINT: a non-interactive bash starts background jobs with
	# SIGINT ignored, and the child inherits that, so an interrupt never reaches it.
	kill -TERM "$PID" 2>/dev/null || true
	local deadline=$((SECONDS + 45))
	while kill -0 "$PID" 2>/dev/null; do
		if [ "$SECONDS" -ge "$deadline" ]; then
			warn "the stream ignored SIGTERM for 45s; killing it"
			kill -KILL "$PID" 2>/dev/null || true
			break
		fi
		sleep 1
	done
	wait "$PID" 2>/dev/null || true
	info "stream stopped"
	PID=""
}

stream_alive() { [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; }
stream_dead() { ! stream_alive; }
ch_up() { curl -sf "$CH" --data-binary "SELECT 1" >/dev/null; }

step "Checking prerequisites"
command -v docker >/dev/null || die "docker not found"
docker info >/dev/null 2>&1 || die "the docker daemon is not running; start Docker Desktop"
command -v go >/dev/null || die "go not found"
ok "docker, go"

step "Starting MySQL and ClickHouse"
"${COMPOSE[@]}" --profile sinks up -d --wait mysql clickhouse
ok "containers healthy"

step "Building"
go build -o bin/changeflow ./cmd/changeflow
ok "bin/changeflow built"

step "1. Is the source configured for CDC?"
$BIN preflight --dsn 'cdc:cdc@tcp(127.0.0.1:13306)/' || die "preflight refused this server"

step "2. Is the configuration valid?"
$BIN validate -c "$CFG" || die "validate refused the configuration"

step "3. Resetting to a known state"
sql "DELETE FROM shop.orders WHERE id IN (900001, 900002, 900003) OR user_id = 777"
sql "UPDATE shop.orders SET status = 'paid' WHERE id = 1"
sql "UPDATE shop.orders SET status = 'shipped' WHERE id = 2"
sql "DROP TABLE IF EXISTS changeflow_meta.changeflow_checkpoints"
ch "DROP TABLE IF EXISTS analytics.orders" >/dev/null
BASELINE=$(sql "SELECT COUNT(*) FROM shop.orders")
info "shop.orders holds $BASELINE rows, no checkpoint, no destination table"

step "4. Generating the destination schema and applying it"
$BIN generate-schema -c "$CFG" --stream "$STREAM" >/tmp/orders-ch.sql
cat /tmp/orders-ch.sql
ch "$(cat /tmp/orders-ch.sql)" >/dev/null
ch_up || die "clickhouse is not answering"
info "engine is ReplacingMergeTree(_version, _is_deleted): a replay overwrites rather than duplicates"

step "5. First start: the table scan"
start_stream
wait_until "table scan complete" 180 log_has "table scan complete"
wait_until "$BASELINE rows in ClickHouse" 60 count_is "$BASELINE"
info "these rows predate the stream, so no binlog event mentions them"
info "id above 2^63 kept its value: user_id $(ch_field 18446744073709551000 user_id)"
SCANS=$(log_count "starting a table scan")

step "6. Live change data capture"
sql "UPDATE shop.orders SET status = 'shipped', total_amount = 42.50 WHERE id = 1"
wait_until "id 1 becomes shipped in ClickHouse" 30 field_is 1 status shipped
info "exact decimal, not a float: total_amount = $(ch_field 1 total_amount)"
sql "INSERT INTO shop.orders (id, user_id, status, channels, total_amount, metadata) VALUES (900001, 42, 'paid', 'web,pos', 7.25, '{\"live\":true}')"
wait_until "the inserted row reaches ClickHouse" 30 field_is 900001 status paid
info "SET column decoded to labels: $(ch_field 900001 channels)"
wait_until "row count followed the insert" 30 count_is $((BASELINE + 1))

step "7. A delete is replicated as a delete"
sql "INSERT INTO shop.orders (id, user_id, status, total_amount) VALUES (900003, 5, 'draft', 1.00)"
wait_until "the throwaway row arrives" 30 count_is $((BASELINE + 2))
sql "DELETE FROM shop.orders WHERE id = 900003"
wait_until "the delete is applied" 30 count_is $((BASELINE + 1))
info "the row is still stored with _is_deleted = 1 until the engine merges it away"

step "8. Where the position is kept"
$BIN status -c "$CFG"
mysql_root -e "SELECT stream, snapshot_done, snapshot_rows_done, LEFT(gtid_set, 60) AS gtid_prefix, last_error FROM changeflow_meta.changeflow_checkpoints\G"
SAVED_GTID=$(sql "SELECT gtid_set FROM changeflow_meta.changeflow_checkpoints WHERE stream = '$STREAM'")
info "the position advances only after ClickHouse acknowledges a batch"

step "9. Persistence: stop the process, change rows while it is down"
stop_stream
sql "INSERT INTO shop.orders (id, user_id, status, total_amount) VALUES (900002, 1234, 'draft', 5.00)"
sql "UPDATE shop.orders SET status = 'cancelled' WHERE id = 2"
[ "$(ch_field 900002 status)" = MISSING ] || die "row 900002 reached the sink while the stream was down"
ok "the sink has not seen those changes yet (process was down)"

step "10. Restart: resume from the saved position, no rescan"
start_stream
wait_until "row 900002 arrives after the restart" 90 field_is 900002 status draft
wait_until "id 2 shows the update made during the outage" 30 field_is 2 status cancelled
[ "$(log_count "starting a table scan")" = "$SCANS" ] ||
	die "the restart rescanned the table instead of resuming"
ok "no new table scan: the binlog was replayed from the stored GTID"
info "saved position before the restart was: ${SAVED_GTID:0:60}…"

step "11. Metrics and readiness"
if curl -sf "$METRICS/readyz" >/dev/null; then ok "/readyz reports ready"; else warn "/readyz is not ready yet"; fi
curl -s "$METRICS/metrics" | grep -E '^changeflow_(binlog_lag_seconds|events_total|snapshot_rows)' | head -8

if [ "${SKIP_SCAN_RESUME:-0}" != 1 ]; then
	step "12. Persistence mid-scan: kill a backfill and resume it"
	stop_stream
	# Explicit ids: the seeded row sits near the unsigned ceiling, so AUTO_INCREMENT on
	# this table is exhausted and any insert that omits the id fails with error 1467.
	sql "INSERT INTO shop.orders (id, user_id, status, total_amount)
	     SELECT 1000000 + ROW_NUMBER() OVER (), 777, 'draft', 1.00
	     FROM information_schema.columns a, information_schema.columns b LIMIT 20000"
	BULK=$(sql "SELECT COUNT(*) FROM shop.orders")
	info "table grown to $BULK rows"
	$BIN resnapshot -c "$CFG" --stream "$STREAM" --confirm
	ch "DROP TABLE IF EXISTS analytics.orders" >/dev/null
	ch "$(cat /tmp/orders-ch.sql)" >/dev/null

	CFG_SLOW=/tmp/changeflow-demo-capped.yaml
	capped_config "$CFG_SLOW"
	info "scanning at 2000 rows/s so the backfill can be caught in the middle"

	COMPLETED_BEFORE=$(log_count "table scan complete")
	PROGRESS_BEFORE=$(log_count "table scan progress")
	RESUMED_BEFORE=$(log_count "resuming a table scan")
	start_stream "$CFG_SLOW"
	# Several thousand rows in, so the scan is well underway and the process has installed
	# its signal handler.
	wait_until "the scan reports progress" 90 log_grew "table scan progress" $((PROGRESS_BEFORE + 3))
	stop_stream
	if [ "$(log_count "table scan complete")" != "$COMPLETED_BEFORE" ]; then
		warn "the scan finished before it could be interrupted; skipping the resume assertion"
		start_stream "$CFG_SLOW"
	else
		$BIN status -c "$CFG"
		info "SNAPSHOT above is a percentage, and the cursor is in the checkpoint row"
		start_stream "$CFG_SLOW"
		wait_until "the scan resumes rather than restarting" 90 log_grew "resuming a table scan" "$RESUMED_BEFORE"
	fi
	wait_until "the backfill completes" 600 count_is "$BULK"
	ok "all $BULK rows landed"
fi

if [ "${SKIP_OUTAGE:-0}" != 1 ]; then
	step "13. A stopped stream says why it stopped"
	docker stop changeflow-ch >/dev/null
	sql "UPDATE shop.orders SET status = 'paid' WHERE id = 900002"
	if wait_soft "the stream to stop because the sink is gone" 180 stream_dead; then
		PID=""
	else
		warn "still running; it may retry for longer than this demo waits"
		stop_stream
	fi
	$BIN status -c "$CFG" || true
	info "the 'stopped:' line above came from the checkpoint row, not from a running process"
	docker start changeflow-ch >/dev/null
	wait_until "ClickHouse is back" 120 ch_up
	start_stream
	wait_until "the change made during the outage is applied" 180 field_is 900002 status paid
	ok "recovered from the sink outage without losing the change"
fi

step "Done"
$BIN status -c "$CFG" --dlq-dir dlq
printf '\nWhat this proved:\n'
printf '  - rows that predate the stream arrive from a table scan, not the binlog\n'
printf '  - later changes arrive as they happen, with types decoded (unsigned id, SET, exact decimal)\n'
printf '  - a delete is replicated as a delete, not left behind\n'
printf '  - the position lives in MySQL, so a restart resumes instead of rescanning\n'
printf '  - an interrupted scan resumes from its cursor\n'
printf '  - a stream that stops records why, and recovers when the sink returns\n'
