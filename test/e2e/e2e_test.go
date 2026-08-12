// Package e2e drives the real binary against a real MySQL and a real
// Elasticsearch.
//
// Everything here is deliberately black box: it starts the compiled process,
// writes to MySQL, and reads from Elasticsearch. Nothing imports changeflow's
// packages, so the test cannot accidentally assert against an internal shortcut,
// and killing the process outright is a genuine crash rather than a simulated one.
//
// The tests skip unless CHANGEFLOW_E2E is set, so an ordinary `go test ./...` stays
// hermetic.
package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	defaultMySQLDSN = "root:root@tcp(127.0.0.1:13306)/shop"
	defaultMetaDSN  = "root:root@tcp(127.0.0.1:13306)/changeflow_meta"
	defaultESURL    = "http://127.0.0.1:19200"
	defaultCHDSN    = "http://changeflow:changeflow@127.0.0.1:18123/?database=analytics"
	// Elasticsearch is not searchable the instant a write is acknowledged, and a
	// restart replays a batch, so assertions poll rather than assume.
	settleTimeout = 45 * time.Second
)

type env struct {
	t *testing.T
	// running is the process under test, so a failed assertion can report what it
	// was doing rather than only that nothing happened.
	running  *process
	db       *sql.DB
	esURL    string
	index    string
	binary   string
	config   string
	dlqDir   string
	metaDSN  string
	mysqlDSN string
	chDSN    string
	// indexCreated records whether this test created an index, so teardown touches
	// only what was used and a restart does not wipe it.
	indexCreated bool
}

func setup(t *testing.T) *env {
	t.Helper()

	if os.Getenv("CHANGEFLOW_E2E") == "" {
		t.Skip("set CHANGEFLOW_E2E=1 to run end-to-end tests against MySQL and Elasticsearch")
	}

	e := &env{
		t:        t,
		esURL:    envOr("CHANGEFLOW_E2E_ES_URL", defaultESURL),
		mysqlDSN: envOr("CHANGEFLOW_E2E_MYSQL_DSN", defaultMySQLDSN),
		metaDSN:  envOr("CHANGEFLOW_E2E_META_DSN", defaultMetaDSN),
		chDSN:    envOr("CHANGEFLOW_E2E_CH_DSN", defaultCHDSN),
		// A per-test index and stream keep runs independent, so one failure does not
		// leave state that breaks the next. The name is a digest rather than the test
		// name: stream names are capped at 48 characters by the checkpoint column, and
		// some of these test names are longer than that on their own.
		index:  "orders_e2e_" + shortHash(t.Name()),
		dlqDir: t.TempDir(),
	}

	db, err := sql.Open("mysql", e.mysqlDSN)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("connect to mysql: %v", err)
	}
	e.db = db

	e.binary = buildBinary(t)
	// The configuration comes first: a generated schema is derived from it.
	e.config = e.writeConfig()

	t.Cleanup(func() {
		// Only the destination this test actually used needs tidying, so a test for one
		// does not fail on the absence of the other.
		if e.indexCreated {
			e.deleteIndex()
		}
		e.exec("DELETE FROM orders WHERE id >= 20000")
		if _, err := db.Exec("DELETE FROM "+tableFor(e.metaDSN)+" WHERE stream = ?", e.streamName()); err != nil {
			t.Logf("could not clear the checkpoint: %v", err)
		}
	})

	return e
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// shortHash gives a stable, lowercase, bounded name for a test. Elasticsearch
// requires lowercase index names, and changeflow bounds stream names.
func shortHash(name string) string {
	sum := sha256.Sum256([]byte(name))
	return hex.EncodeToString(sum[:6])
}

func tableFor(string) string { return "changeflow_meta.changeflow_checkpoints" }

func (e *env) streamName() string { return e.index }

// buildBinary compiles the command under test, so the test exercises what would
// actually be deployed.
func buildBinary(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "changeflow")
	cmd := exec.Command("go", "build", "-o", path, "./cmd/changeflow")
	cmd.Dir = repoRoot(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build binary: %v\n%s", err, out)
	}
	return path
}

func repoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find the repository root")
		}
		dir = parent
	}
}

// createIndex installs the mapping the binary generates for this stream.
//
// Generated rather than hand-written, so the index cannot drift from the type map
// replication uses: a mapping that guesses `long` for an unsigned identifier would
// wrap every value above 2^63. Applying it separately is also how it works in
// production, where changeflow never issues DDL.
// ensureIndex creates the index on first use.
//
// Idempotent because a test may start the process more than once, and recreating the
// index between restarts would discard the documents the test is about to assert on.
func (e *env) ensureIndex() {
	e.t.Helper()

	if e.indexCreated {
		return
	}
	e.requireElasticsearch()
	e.createIndex()
	e.indexCreated = true
}

// requireElasticsearch skips when Elasticsearch is not reachable.
func (e *env) requireElasticsearch() {
	e.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.esURL+"/_cluster/health", nil)
	if err != nil {
		e.t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Skipf("Elasticsearch is not reachable at %s: %v", e.esURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		e.t.Skipf("Elasticsearch returned %d", resp.StatusCode)
	}
}

func (e *env) createIndex() {
	e.t.Helper()

	cmd := exec.Command(e.binary, "generate-schema",
		"-c", e.config, "--stream", e.streamName(), "--replicas", "0")
	cmd.Dir = repoRoot(e.t)
	mapping, err := cmd.Output()
	if err != nil {
		e.t.Fatalf("generate schema: %v", err)
	}

	e.deleteIndex()
	status, payload := e.esRequest(http.MethodPut, "/"+e.index, string(mapping))
	if status >= 300 {
		e.t.Fatalf("create index: status %d: %s\nmapping was:\n%s", status, payload, mapping)
	}
}

// createIndexManually installs a hand-written mapping for the one test that needs an
// unsuitable one: user_id is declared as a byte, so 7 indexes and 99999 does not.
// That makes the destination refuse a specific row rather than a whole field, which is
// what a dead letter test needs.
func (e *env) createIndexManually() {
	e.t.Helper()

	body := `{
	  "settings": {"number_of_shards": 1, "number_of_replicas": 0},
	  "mappings": {
	    "dynamic": "strict",
	    "properties": {
	      "id":           {"type": "unsigned_long"},
	      "user_id":      {"type": "byte"},
	      "status":       {"type": "keyword"},
	      "channels":     {"type": "keyword"},
	      "total_amount": {"type": "keyword"},
	      "is_gift":      {"type": "boolean"},
	      "note_latin1":  {"type": "keyword"},
	      "metadata":     {"type": "object", "enabled": false},
	      "placed_at":    {"type": "date"},
	      "updated_at":   {"type": "date"}
	    }
	  }
	}`

	e.deleteIndex()
	status, payload := e.esRequest(http.MethodPut, "/"+e.index, body)
	if status >= 300 {
		e.t.Fatalf("create index: status %d: %s", status, payload)
	}
}

func (e *env) deleteIndex() {
	e.esRequest(http.MethodDelete, "/"+e.index, "")
}

func (e *env) writeConfig() string {
	e.t.Helper()
	return e.writeConfigWith(false)
}

// writeConfigWith produces a configuration with or without the initial table scan.
func (e *env) writeConfigWith(snapshot bool) string {
	e.t.Helper()
	return e.writeConfigFor(sinkElasticsearch, snapshot)
}

type sinkKind int

const (
	sinkElasticsearch sinkKind = iota
	sinkClickHouse
)

// writeConfigFor produces a configuration for either destination.
func (e *env) writeConfigFor(kind sinkKind, snapshot bool) string {
	e.t.Helper()

	if kind == sinkClickHouse {
		return e.writeClickHouseConfig(snapshot)
	}

	path := filepath.Join(e.t.TempDir(), "changeflow.yaml")
	body := fmt.Sprintf(`
source:
  dsn: %q
  server_id: %d
checkpoint:
  dsn: %q
runtime:
  buffer_size: 256
  metrics_addr: "off"
streams:
  %s:
    table: shop.orders
    snapshot:
      enabled: %t
      chunk_size: 20
    batch:
      max_rows: 50
      max_bytes: 1MiB
      flush_interval: 250ms
    sink:
      type: elasticsearch
      addresses: [%q]
      index: %s
      workers: 2
    mapping:
      key: [id]
      exclude: [internal_note]
`, e.mysqlDSN, 7000+time.Now().UnixNano()%900, e.metaDSN, e.streamName(), snapshot, e.esURL, e.index)

	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		e.t.Fatalf("write config: %v", err)
	}
	return path
}

func (e *env) writeClickHouseConfig(snapshot bool) string {
	e.t.Helper()

	path := filepath.Join(e.t.TempDir(), "changeflow-clickhouse.yaml")
	body := fmt.Sprintf(`
source:
  dsn: %q
  server_id: %d
checkpoint:
  dsn: %q
runtime:
  buffer_size: 256
  metrics_addr: "off"
streams:
  %s:
    table: shop.orders
    snapshot:
      enabled: %t
      chunk_size: 20
    batch:
      max_rows: 50
      max_bytes: 1MiB
      flush_interval: 250ms
    sink:
      type: clickhouse
      dsn: %q
      table: %s
      workers: 1
    mapping:
      key: [id]
      exclude: [internal_note]
`, e.mysqlDSN, 7500+time.Now().UnixNano()%400, e.metaDSN, e.streamName(), snapshot, e.chDSN, e.chTable())

	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		e.t.Fatalf("write config: %v", err)
	}
	return path
}

// process is a running changeflow.
type process struct {
	cmd *exec.Cmd
	log *bytes.Buffer
}

// startWithSnapshot launches the binary with the initial table scan enabled.
func (e *env) startWithSnapshot() *process {
	e.t.Helper()
	e.config = e.writeConfigWith(true)
	e.ensureIndex()
	return e.launch()
}

// start launches the binary against Elasticsearch. The returned process must be
// stopped by the caller.
func (e *env) start() *process {
	e.t.Helper()
	e.ensureIndex()
	return e.launch()
}

// launch starts the process without assuming a destination, so a ClickHouse test does
// not create an index it will never use.
func (e *env) launch() *process {
	e.t.Helper()

	log := &bytes.Buffer{}
	cmd := exec.Command(e.binary, "run",
		"-c", e.config,
		"--stream", e.streamName(),
		"--dlq-dir", e.dlqDir)
	cmd.Stdout, cmd.Stderr = log, log
	// A process group lets the test kill the whole thing outright.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		e.t.Fatalf("start changeflow: %v", err)
	}
	p := &process{cmd: cmd, log: log}
	e.running = p
	e.t.Cleanup(func() { p.stop() })

	// Wait for replication to be registered, otherwise the test's writes would
	// happen before the process is listening and never appear in the stream.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(log.String(), "replication started") {
			return p
		}
		if p.exited() {
			e.t.Fatalf("changeflow exited during startup:\n%s", log.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	e.t.Fatalf("changeflow did not start replicating within 30s:\n%s", log.String())
	return nil
}

func (p *process) exited() bool {
	return p.cmd.ProcessState != nil && p.cmd.ProcessState.Exited()
}

// stop asks the process to finish, which flushes and checkpoints.
func (p *process) stop() {
	if p.cmd.Process == nil || p.exited() {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() { _, _ = p.cmd.Process.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
	}
}

// kill terminates the process without warning, which is the failure the design's
// checkpoint ordering exists to survive.
func (p *process) kill() {
	if p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	_, _ = p.cmd.Process.Wait()
}

func (e *env) exec(query string, args ...any) {
	e.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := e.db.ExecContext(ctx, query, args...); err != nil {
		e.t.Fatalf("exec %q: %v", query, err)
	}
}

func (e *env) esRequest(method, path, body string) (int, string) {
	e.t.Helper()

	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	// Deliberately not the test's context: cleanup runs after it is cancelled, and
	// every teardown would otherwise fail the test it was tidying up after.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, e.esURL+path, reader)
	if err != nil {
		e.t.Fatalf("build request: %v", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(payload)
}

// document fetches one document, reporting whether it exists.
func (e *env) document(id string) (map[string]any, bool) {
	e.t.Helper()

	status, payload := e.esRequest(http.MethodGet, "/"+e.index+"/_doc/"+id, "")
	if status == http.StatusNotFound {
		return nil, false
	}
	if status >= 300 {
		e.t.Fatalf("get document %s: status %d: %s", id, status, payload)
	}

	var envelope struct {
		Found  bool           `json:"found"`
		Source map[string]any `json:"_source"`
	}
	// UseNumber keeps large integers exact. Decoded as float64, an unsigned 64-bit
	// identifier comes back as 1.8446744073709552e+19, which would make this test
	// report a corruption that only happened in the test.
	dec := json.NewDecoder(strings.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&envelope); err != nil {
		e.t.Fatalf("decode document %s: %v", id, err)
	}
	return envelope.Source, envelope.Found
}

// waitFor polls until the condition holds, which is how a test tolerates
// replication lag and index refresh without guessing at a sleep.
func (e *env) waitFor(what string, condition func() bool) {
	e.t.Helper()

	deadline := time.Now().Add(settleTimeout)
	for time.Now().Before(deadline) {
		e.refresh()
		if condition() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	e.t.Fatalf("timed out waiting for %s\nchangeflow log:\n%s", what, e.processLog())
}

// processLog returns the tail of the running process's output.
func (e *env) processLog() string {
	if e.running == nil {
		return "(no process was started)"
	}
	out := e.running.log.String()
	const tail = 4000
	if len(out) > tail {
		return "..." + out[len(out)-tail:]
	}
	return out
}

func (e *env) refresh() {
	e.esRequest(http.MethodPost, "/"+e.index+"/_refresh", "")
}

func (e *env) waitForDocument(id string) map[string]any {
	e.t.Helper()

	var doc map[string]any
	e.waitFor("document "+id+" to appear", func() bool {
		var found bool
		doc, found = e.document(id)
		return found
	})
	return doc
}

func (e *env) countDocuments() int {
	e.t.Helper()

	e.refresh()
	status, payload := e.esRequest(http.MethodGet, "/"+e.index+"/_count", "")
	if status >= 300 {
		e.t.Fatalf("count: status %d: %s", status, payload)
	}
	var result struct{ Count int }
	if err := json.Unmarshal([]byte(payload), &result); err != nil {
		e.t.Fatalf("decode count: %v", err)
	}
	return result.Count
}

// documentIDs returns every document id in the index, for diagnosing a count that
// does not match expectations.
func (e *env) documentIDs() []string {
	e.t.Helper()

	e.refresh()
	status, payload := e.esRequest(http.MethodGet, "/"+e.index+"/_search?size=200&_source=false&sort=_doc", "")
	if status >= 300 {
		e.t.Fatalf("search: status %d: %s", status, payload)
	}

	var result struct {
		Hits struct {
			Hits []struct {
				ID string `json:"_id"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal([]byte(payload), &result); err != nil {
		e.t.Fatalf("decode search: %v", err)
	}

	ids := make([]string, 0, len(result.Hits.Hits))
	for _, hit := range result.Hits.Hits {
		ids = append(ids, hit.ID)
	}
	return ids
}

// countInRange counts documents whose identifier falls in a range, which is how a
// test asserts on the rows it wrote rather than on whatever else the table holds.
func (e *env) countInRange(from, to uint64) int {
	e.t.Helper()

	e.refresh()
	query := fmt.Sprintf(`{"query":{"range":{"id":{"gte":%d,"lte":%d}}}}`, from, to)
	status, payload := e.esRequest(http.MethodPost, "/"+e.index+"/_count", query)
	if status >= 300 {
		e.t.Fatalf("count in range: status %d: %s", status, payload)
	}

	var result struct{ Count int }
	if err := json.Unmarshal([]byte(payload), &result); err != nil {
		e.t.Fatalf("decode count: %v", err)
	}
	return result.Count
}

func TestInsertReachesElasticsearch(t *testing.T) {
	e := setup(t)
	e.start()

	e.exec(`INSERT INTO orders (id,user_id,status,channels,total_amount,is_gift,note_latin1,placed_at)
	        VALUES (20001, 18446744073709551001, 'paid', 'web,ios', 19.90, 1, 'café', '2026-08-11 10:00:00.000')`)

	doc := e.waitForDocument("20001")

	// Each of these is a decision the type map makes, checked against a real index
	// rather than a stub.
	if got := fmt.Sprint(doc["user_id"]); got != "18446744073709551001" {
		t.Errorf("user_id = %v, want the exact unsigned value", doc["user_id"])
	}
	if doc["status"] != "paid" {
		t.Errorf("status = %v, want the enum label", doc["status"])
	}
	if got := doc["channels"]; fmt.Sprint(got) != "[web ios]" {
		t.Errorf("channels = %v, want the set members", got)
	}
	if doc["total_amount"] != "19.90" {
		t.Errorf("total_amount = %v, want the exact decimal including its trailing zero", doc["total_amount"])
	}
	if doc["is_gift"] != true {
		t.Errorf("is_gift = %v, want true", doc["is_gift"])
	}
	if doc["note_latin1"] != "café" {
		t.Errorf("note_latin1 = %v, want the latin1 column converted to UTF-8", doc["note_latin1"])
	}
	if doc["placed_at"] != "2026-08-11T10:00:00Z" {
		t.Errorf("placed_at = %v, want the wall clock read in the source's zone", doc["placed_at"])
	}
}

func TestUpdateAndDeletePropagate(t *testing.T) {
	e := setup(t)
	e.start()

	e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (20010,7,'draft',1.00)")
	e.waitForDocument("20010")

	e.exec("UPDATE orders SET status='shipped', total_amount=2.50 WHERE id=20010")
	e.waitFor("the update to be applied", func() bool {
		doc, found := e.document("20010")
		return found && doc["status"] == "shipped" && doc["total_amount"] == "2.50"
	})

	e.exec("DELETE FROM orders WHERE id=20010")
	e.waitFor("the delete to be applied", func() bool {
		_, found := e.document("20010")
		return !found
	})
}

// A key change has to remove the old document. Nothing later mentions that key
// again, so a miss here leaves it in the index forever.
func TestKeyChangeRemovesTheOldDocument(t *testing.T) {
	e := setup(t)
	e.start()

	e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (20020,7,'paid',5.00)")
	e.waitForDocument("20020")

	e.exec("UPDATE orders SET id=20021 WHERE id=20020")

	e.waitFor("the new key to appear and the old one to be gone", func() bool {
		_, oldFound := e.document("20020")
		_, newFound := e.document("20021")
		return newFound && !oldFound
	})
}

// The design's central claim: a crash between writing to the destination and
// recording the position must lose nothing, because the position only advances
// after an acknowledgement and a replayed write is discarded as stale.
func TestConvergesAfterAnAbruptKill(t *testing.T) {
	e := setup(t)
	p := e.start()

	const total = 300
	for i := 0; i < total; i++ {
		e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,7,'paid',1.00)", 21000+i)
	}

	// Kill while batches are still in flight, without warning.
	e.waitFor("some documents to arrive", func() bool { return e.countDocuments() > 0 })
	p.kill()

	before := e.countDocuments()
	t.Logf("killed with %d of %d documents written", before, total)

	// A restart resumes from the last acknowledged position.
	e.start()

	e.waitFor(fmt.Sprintf("all %d documents to converge", total), func() bool {
		return e.countDocuments() == total
	})

	// Spot check the boundaries: a gap would most likely appear at the point of the
	// kill, and an off-by-one at the ends.
	for _, id := range []string{"21000", fmt.Sprint(21000 + total/2), fmt.Sprint(21000 + total - 1)} {
		if _, found := e.document(id); !found {
			t.Errorf("document %s is missing after recovery", id)
		}
	}
}

// Restarting cleanly must not duplicate or lose anything, and the replayed batch
// must be recognised as already applied rather than written again.
func TestRestartIsIdempotent(t *testing.T) {
	e := setup(t)
	p := e.start()

	const (
		firstID = 22000
		rows    = 20
	)
	for i := 0; i < rows; i++ {
		e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,7,'paid',1.00)", firstID+i)
	}
	e.waitFor("the rows to arrive", func() bool {
		return e.countInRange(firstID, firstID+rows-1) == rows
	})

	p.stop()
	e.start()

	// Long enough for the restarted process to replay whatever it re-reads.
	time.Sleep(3 * time.Second)

	// The rows this test wrote are what it is entitled to assert on: one document
	// per key, no more and no fewer.
	if got := e.countInRange(firstID, firstID+rows-1); got != rows {
		t.Errorf("documents in the written range = %d, want %d", got, rows)
	}

	// Anything else in the index did not come from this test, and naming it is more
	// useful than reporting a total that does not add up.
	if ids := e.documentIDs(); len(ids) != rows {
		var strays []string
		for _, id := range ids {
			if n, err := strconv.ParseUint(id, 10, 64); err != nil || n < firstID || n > firstID+rows-1 {
				strays = append(strays, id)
			}
		}
		t.Errorf("index holds %d documents, want %d; documents outside the written range: %v",
			len(ids), rows, strays)
	}
}

// Rows written before the stream existed produce no binlog events, so only a table
// scan can deliver them. This is the whole reason the snapshot phase exists.
func TestPreexistingRowsAreBackfilled(t *testing.T) {
	e := setup(t)

	// Written before anything is running, and never touched again, so nothing about
	// them will ever appear in the binlog.
	const firstID = 23000
	for i := 0; i < 25; i++ {
		e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,7,'paid',1.00)", firstID+i)
	}

	e.startWithSnapshot()

	e.waitFor("the pre-existing rows to be backfilled", func() bool {
		return e.countInRange(firstID, firstID+24) == 25
	})

	// A change made after the scan must still be applied on top of it.
	e.exec("UPDATE orders SET status='shipped' WHERE id=?", firstID)
	e.waitFor("a later change to be applied over the scanned row", func() bool {
		doc, found := e.document(fmt.Sprint(firstID))
		return found && doc["status"] == "shipped"
	})
}

// A scan interrupted partway must resume rather than restart, and must still deliver
// every row.
func TestInterruptedBackfillResumes(t *testing.T) {
	e := setup(t)

	const firstID = 23500
	for i := 0; i < 120; i++ {
		e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,7,'paid',1.00)", firstID+i)
	}

	p := e.startWithSnapshot()
	// Kill once some rows have landed but before the scan can have finished.
	e.waitFor("the scan to start delivering rows", func() bool {
		return e.countInRange(firstID, firstID+119) > 0
	})
	p.kill()
	killedAt := e.countInRange(firstID, firstID+119)
	t.Logf("killed with %d of 120 rows backfilled", killedAt)

	e.startWithSnapshot()
	e.waitFor("the backfill to complete after a restart", func() bool {
		return e.countInRange(firstID, firstID+119) == 120
	})
}

// A change made while the scan is running must win over the scanned copy of the same
// row, which is what makes scanning without a table lock safe.
func TestConcurrentChangeWinsOverTheScannedRow(t *testing.T) {
	e := setup(t)

	const firstID = 24500
	for i := 0; i < 200; i++ {
		e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,7,'draft',1.00)", firstID+i)
	}

	e.startWithSnapshot()

	// Change a row while the scan is in progress. Whichever order the two reach the
	// destination, the change must be the version that survives.
	e.exec("UPDATE orders SET status='cancelled' WHERE id=?", firstID+150)

	e.waitFor("the whole table to be backfilled", func() bool {
		return e.countInRange(firstID, firstID+199) == 200
	})
	e.waitFor("the concurrent change to be the surviving version", func() bool {
		doc, found := e.document(fmt.Sprint(firstID + 150))
		return found && doc["status"] == "cancelled"
	})
}

func TestStatusReportsProgress(t *testing.T) {
	e := setup(t)
	e.start()

	e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (24000,7,'paid',1.00)")
	e.waitForDocument("24000")

	out, err := exec.CommandContext(e.t.Context(), e.binary, "status", "-c", e.config).CombinedOutput()
	if err != nil {
		t.Fatalf("status: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), e.streamName()) {
		t.Errorf("status does not mention the stream:\n%s", out)
	}
	// A position means something was acknowledged and recorded.
	if strings.Contains(string(out), "not started") {
		t.Errorf("status still reports the stream as not started:\n%s", out)
	}
}

// A document the index refuses permanently must be recorded and must not stop the
// stream, or one bad row would block every later change.
// A document the index refuses permanently must be recorded and must not stop the
// stream, or one bad row would block every later change.
//
// The refusal has to be specific to one row rather than to a field, so the index
// declares user_id as a byte: 7 fits, 99999 does not. A value MySQL itself would
// reject teaches nothing, because changeflow would never see it.
func TestRefusedDocumentGoesToTheDeadLetterQueueAndStreamContinues(t *testing.T) {
	e := setup(t)
	// A deliberately unsuitable mapping, in place of the generated one.
	e.requireElasticsearch()
	e.deleteIndex()
	e.createIndexManually()
	e.indexCreated = true

	e.start()

	// Refused: 99999 does not fit a byte.
	e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (25000,99999,'paid',1.00)")
	// Accepted: the stream must carry on past the refusal.
	e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (25001,7,'paid',1.00)")

	e.waitForDocument("25001")

	if _, found := e.document("25000"); found {
		t.Fatal("the refused document was indexed after all, so this test proves nothing")
	}

	var recorded string
	e.waitFor("the refused document to be recorded", func() bool {
		entries, err := os.ReadDir(e.dlqDir)
		if err != nil || len(entries) == 0 {
			return false
		}
		body, err := os.ReadFile(filepath.Join(e.dlqDir, entries[0].Name()))
		if err != nil {
			return false
		}
		recorded = string(body)
		return strings.Contains(recorded, "25000")
	})

	if !strings.Contains(recorded, "\"status\":400") {
		t.Errorf("expected the record to carry the refusal status:\n%s", recorded)
	}
	// The body is withheld unless asked for, since row values can hold personal data.
	if strings.Contains(recorded, "\"body\":") {
		t.Errorf("the document body should not be recorded by default:\n%s", recorded)
	}
}

// chTable is this test's destination table.
func (e *env) chTable() string { return "analytics." + e.index }

// clickhouse sends a statement over the HTTP interface and returns the response body.
func (e *env) clickhouse(statement string) string {
	e.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.chDSN, strings.NewReader(statement))
	if err != nil {
		e.t.Fatalf("build request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("clickhouse %q: %v", firstLine(statement), err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 300 {
		e.t.Fatalf("clickhouse %q: status %d: %s", firstLine(statement), resp.StatusCode, body)
	}
	return strings.TrimSpace(string(body))
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i] + "..."
	}
	return s
}

// createClickHouseTable applies the DDL the binary generates, so the destination schema
// cannot drift from what replication sends.
func (e *env) createClickHouseTable(config string) {
	e.t.Helper()

	cmd := exec.Command(e.binary, "generate-schema", "-c", config, "--stream", e.streamName())
	cmd.Dir = repoRoot(e.t)
	ddl, err := cmd.Output()
	if err != nil {
		e.t.Fatalf("generate schema: %v", err)
	}

	e.clickhouse("DROP TABLE IF EXISTS " + e.chTable())
	e.clickhouse(string(ddl))
	e.t.Cleanup(func() { e.clickhouse("DROP TABLE IF EXISTS " + e.chTable()) })
}

// requireClickHouse skips when ClickHouse is not reachable, so the Elasticsearch
// scenarios can still run on their own.
func (e *env) requireClickHouse() {
	e.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.chDSN, strings.NewReader("SELECT 1"))
	if err != nil {
		e.t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Skipf("ClickHouse is not reachable at %s: %v", e.chDSN, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		e.t.Skipf("ClickHouse returned %d: %s", resp.StatusCode, body)
	}
}

// startClickHouse launches the binary against ClickHouse.
func (e *env) startClickHouse(snapshot bool) *process {
	e.t.Helper()
	e.requireClickHouse()

	e.config = e.writeConfigFor(sinkClickHouse, snapshot)
	e.createClickHouseTable(e.config)
	return e.launch()
}

// chCount counts rows through FINAL, which is how a reader must query a
// ReplacingMergeTree until background merges have collapsed duplicate versions.
func (e *env) chCount(where string) int {
	e.t.Helper()

	body := e.clickhouse(fmt.Sprintf(
		"SELECT count() FROM %s FINAL WHERE _is_deleted = 0 AND %s", e.chTable(), where))
	n, err := strconv.Atoi(body)
	if err != nil {
		e.t.Fatalf("count returned %q: %v", body, err)
	}
	return n
}

func (e *env) chValue(query string) string {
	e.t.Helper()
	return e.clickhouse(query)
}

func TestClickHouseReceivesInsertsUpdatesAndDeletes(t *testing.T) {
	e := setup(t)
	e.startClickHouse(false)

	e.exec(`INSERT INTO orders (id,user_id,status,channels,total_amount,is_gift,placed_at)
	        VALUES (26000, 18446744073709551001, 'paid', 'web,ios', 19.90, 1, '2026-08-11 10:00:00.000')`)

	e.waitFor("the row to arrive", func() bool { return e.chCount("id = 26000") == 1 })

	// The same value decisions as the other destination, checked against a real server:
	// an exact decimal, an enum label, a set as an array, an unsigned identifier.
	row := e.chValue(fmt.Sprintf(
		"SELECT toString(user_id), status, toString(total_amount), arrayStringConcat(channels, ',') "+
			"FROM %s FINAL WHERE id = 26000 FORMAT TSV", e.chTable()))
	for _, want := range []string{"18446744073709551001", "paid", "19.90", "web,ios"} {
		if !strings.Contains(row, want) {
			t.Errorf("expected %q in the stored row: %s", want, row)
		}
	}

	e.exec("UPDATE orders SET status='shipped' WHERE id=26000")
	e.waitFor("the update to win", func() bool {
		return strings.Contains(e.chValue(fmt.Sprintf(
			"SELECT status FROM %s FINAL WHERE id = 26000", e.chTable())), "shipped")
	})
	// The engine keeps one row per key, so an update must not leave two behind.
	if got := e.chCount("id = 26000"); got != 1 {
		t.Errorf("rows for the key = %d, want 1 after an update", got)
	}

	// A delete is a tombstone, and FINAL is what makes it disappear from reads.
	e.exec("DELETE FROM orders WHERE id=26000")
	e.waitFor("the delete to take effect", func() bool { return e.chCount("id = 26000") == 0 })
}

func TestClickHouseBackfillsPreexistingRows(t *testing.T) {
	e := setup(t)

	const firstID = 26100
	for i := 0; i < 25; i++ {
		e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,7,'paid',1.00)", firstID+i)
	}

	e.startClickHouse(true)

	e.waitFor("the scan to deliver every row", func() bool {
		return e.chCount(fmt.Sprintf("id BETWEEN %d AND %d", firstID, firstID+24)) == 25
	})

	// A change made after the scan must still be applied over the scanned copy.
	e.exec("UPDATE orders SET status='cancelled' WHERE id=?", firstID)
	e.waitFor("a later change to win over the scanned row", func() bool {
		return strings.Contains(e.chValue(fmt.Sprintf(
			"SELECT status FROM %s FINAL WHERE id = %d", e.chTable(), firstID)), "cancelled")
	})
}

func TestClickHouseConvergesAfterAnAbruptKill(t *testing.T) {
	e := setup(t)
	p := e.startClickHouse(false)

	const firstID = 26200
	const rows = 200
	for i := 0; i < rows; i++ {
		e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,7,'paid',1.00)", firstID+i)
	}

	e.waitFor("some rows to arrive", func() bool {
		return e.chCount(fmt.Sprintf("id BETWEEN %d AND %d", firstID, firstID+rows-1)) > 0
	})
	p.kill()

	e.startClickHouse(false)
	e.waitFor("every row to converge after the restart", func() bool {
		return e.chCount(fmt.Sprintf("id BETWEEN %d AND %d", firstID, firstID+rows-1)) == rows
	})
}
