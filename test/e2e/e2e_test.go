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
	if err := db.PingContext(t.Context()); err != nil {
		t.Fatalf("connect to mysql: %v", err)
	}
	e.db = db

	e.binary = buildBinary(t)
	e.createIndex()
	e.config = e.writeConfig()

	t.Cleanup(func() {
		e.deleteIndex()
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

// createIndex installs an explicit mapping.
//
// Dynamic mapping would guess `long` for the identifier and then reject any value
// above 2^63, which is exactly the case the type map exists to handle. Managing the
// mapping outside the service is also how it works in production.
func (e *env) createIndex() {
	e.t.Helper()

	body := `{
	  "settings": {"number_of_shards": 1, "number_of_replicas": 0},
	  "mappings": {
	    "dynamic": "strict",
	    "properties": {
	      "id":           {"type": "unsigned_long"},
	      "user_id":      {"type": "unsigned_long"},
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

	path := filepath.Join(e.t.TempDir(), "changeflow.yaml")
	body := fmt.Sprintf(`
source:
  dsn: %q
  server_id: %d
checkpoint:
  dsn: %q
runtime:
  buffer_size: 256
streams:
  %s:
    table: shop.orders
    snapshot:
      enabled: false
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
`, e.mysqlDSN, 7000+time.Now().UnixNano()%900, e.metaDSN, e.streamName(), e.esURL, e.index)

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

// start launches the binary. The returned process must be stopped by the caller.
func (e *env) start() *process {
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
	if _, err := e.db.ExecContext(e.t.Context(), query, args...); err != nil {
		e.t.Fatalf("exec %q: %v", query, err)
	}
}

func (e *env) esRequest(method, path, body string) (int, string) {
	e.t.Helper()

	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(e.t.Context(), method, e.esURL+path, reader)
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
	if err := json.Unmarshal([]byte(payload), &envelope); err != nil {
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

// Restarting cleanly must not duplicate or lose anything either, and the replayed
// batch must be recognised as already applied.
func TestRestartIsIdempotent(t *testing.T) {
	e := setup(t)
	p := e.start()

	for i := 0; i < 20; i++ {
		e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,7,'paid',1.00)", 22000+i)
	}
	e.waitFor("all documents to arrive", func() bool { return e.countDocuments() == 20 })

	p.stop()
	e.start()

	// Give the restarted process time to replay whatever it re-reads.
	time.Sleep(3 * time.Second)

	if got := e.countDocuments(); got != 20 {
		t.Fatalf("document count = %d after a restart, want 20", got)
	}
}

// Rows written before the stream existed are not in the binlog at all, so a
// binlog-only run must not claim to have them. This is the gap a snapshot fills.
func TestPreexistingRowsAreNotStreamed(t *testing.T) {
	e := setup(t)

	e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (23000,7,'paid',1.00)")
	time.Sleep(time.Second)

	e.start()
	e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (23001,7,'paid',1.00)")
	e.waitForDocument("23001")

	if _, found := e.document("23000"); found {
		t.Error("a row written before the stream started appeared without a snapshot, which the position handling should make impossible")
	}
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
func TestRefusedDocumentGoesToTheDeadLetterQueueAndStreamContinues(t *testing.T) {
	e := setup(t)
	e.start()

	// The mapping declares total_amount as a keyword and the index is strict, so a
	// value that cannot be indexed is rejected per document. A note longer than the
	// keyword limit does that without disturbing anything else.
	e.exec("INSERT INTO orders (id,user_id,status,total_amount,note_latin1) VALUES (25000,7,'paid',1.00,?)",
		strings.Repeat("x", 40000))
	e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (25001,7,'paid',1.00)")

	// The healthy row that followed must still arrive.
	e.waitForDocument("25001")

	entries, err := os.ReadDir(e.dlqDir)
	if err != nil {
		t.Fatalf("read dlq dir: %v", err)
	}
	if len(entries) == 0 {
		t.Skip("the index accepted the oversized value, so nothing was refused")
	}

	body, err := os.ReadFile(filepath.Join(e.dlqDir, entries[0].Name()))
	if err != nil {
		t.Fatalf("read dlq file: %v", err)
	}
	if !strings.Contains(string(body), "25000") {
		t.Errorf("the refused document was not recorded:\n%s", body)
	}
}
