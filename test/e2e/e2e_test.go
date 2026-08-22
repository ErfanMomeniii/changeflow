package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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
	settleTimeout   = 45 * time.Second
)

type env struct {
	t            *testing.T
	running      *process
	db           *sql.DB
	esURL        string
	index        string
	binary       string
	config       string
	dlqDir       string
	metaDSN      string
	mysqlDSN     string
	chDSN        string
	writeIndex   string
	alias        string
	created      []string
	indexCreated bool
	snapshotRate int
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
		index:    "orders_e2e_" + shortHash(t.Name()),
		dlqDir:   t.TempDir(),
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
	e.writeIndex = e.index
	e.alias = e.index + "_read"
	e.binary = buildBinary(t)
	e.config = e.writeConfig()
	t.Cleanup(func() {
		for _, index := range e.created {
			e.esRequest(http.MethodDelete, "/"+index, "")
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

func shortHash(name string) string {
	sum := sha256.Sum256([]byte(name))
	return hex.EncodeToString(sum[:6])
}

func tableFor(string) string { return "changeflow_meta.changeflow_checkpoints" }

func (e *env) streamName() string { return e.index }

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

func (e *env) ensureIndex() {
	e.t.Helper()
	if e.indexCreated {
		return
	}
	e.requireElasticsearch()
	e.createIndex()
	e.indexCreated = true
}

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
	e.createIndexFor(e.streamName(), e.writeIndex)
}

func (e *env) createIndexFor(stream, index string) {
	e.t.Helper()
	cmd := exec.Command(e.binary, "generate-schema",
		"-c", e.config, "--stream", stream, "--replicas", "0")
	cmd.Dir = repoRoot(e.t)
	mapping, err := cmd.Output()
	if err != nil {
		e.t.Fatalf("generate schema: %v", err)
	}
	e.esRequest(http.MethodDelete, "/"+index, "")
	status, payload := e.esRequest(http.MethodPut, "/"+index, string(mapping))
	if status >= 300 {
		e.t.Fatalf("create index: status %d: %s\nmapping was:\n%s", status, payload, mapping)
	}
	e.created = append(e.created, index)
}

func (e *env) createIndexManually() {
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
	      "placed_at":    {"type": "date", "format": "strict_date"},
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
	e.esRequest(http.MethodDelete, "/"+e.writeIndex, "")
}

func (e *env) writeConfig() string {
	e.t.Helper()
	return e.writeConfigWith(false)
}

func (e *env) writeConfigWith(snapshot bool) string {
	e.t.Helper()
	return e.writeConfigFor(sinkElasticsearch, snapshot)
}

type sinkKind int

const (
	sinkElasticsearch sinkKind = iota
	sinkClickHouse
)

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
      chunk_size: 20%s
    batch:
      max_rows: 50
      max_bytes: 1MiB
      flush_interval: 250ms
    sink:
      type: elasticsearch
      addresses: [%q]
      index: %s
      alias: %s
      workers: 2
    mapping:
      key: [id]
      exclude: [internal_note]
`, e.mysqlDSN, 7000+time.Now().UnixNano()%900, e.metaDSN, e.streamName(), snapshot, e.rateLimit(), e.esURL, e.writeIndex, e.alias)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		e.t.Fatalf("write config: %v", err)
	}
	return path
}

func (e *env) rateLimit() string {
	if e.snapshotRate <= 0 {
		return ""
	}
	return fmt.Sprintf("\n      max_rate_rows_per_sec: %d", e.snapshotRate)
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

func (e *env) secondStreamName() string { return e.index + "_b" }

func (e *env) writeFanoutConfig() string {
	e.t.Helper()
	path := filepath.Join(e.t.TempDir(), "changeflow-fanout.yaml")
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
      enabled: false
    batch:
      max_rows: 50
      flush_interval: 250ms
    sink:
      type: elasticsearch
      addresses: [%q]
      index: %s
      workers: 2
    mapping:
      key: [id]
      exclude: [internal_note]
  %s:
    table: shop.orders
    snapshot:
      enabled: false
    batch:
      max_rows: 50
      flush_interval: 250ms
    sink:
      type: elasticsearch
      addresses: [%q]
      index: %s
      workers: 1
    mapping:
      key: [id]
      exclude: [internal_note, note_latin1]
`,
		e.mysqlDSN, 7000+time.Now().UnixNano()%900, e.metaDSN,
		e.streamName(), e.esURL, e.writeIndex,
		e.secondStreamName(), e.esURL, e.secondStreamName())
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		e.t.Fatalf("write config: %v", err)
	}
	return path
}

func (e *env) dumpThreads() int {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var count int
	err := e.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.processlist WHERE command LIKE 'Binlog Dump%'`,
	).Scan(&count)
	if err != nil {
		e.t.Fatalf("count dump threads: %v", err)
	}
	return count
}

func (e *env) checkpointPosition(stream string) string {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var gtid string
	err := e.db.QueryRowContext(ctx,
		"SELECT gtid_set FROM "+tableFor(e.metaDSN)+" WHERE stream = ?", stream,
	).Scan(&gtid)
	if errors.Is(err, sql.ErrNoRows) {
		return ""
	}
	if err != nil {
		e.t.Fatalf("read checkpoint for %s: %v", stream, err)
	}
	return gtid
}

type process struct {
	cmd *exec.Cmd
	log *bytes.Buffer
}

func (e *env) startWithSnapshot() *process {
	e.t.Helper()
	e.config = e.writeConfigWith(true)
	e.ensureIndex()
	return e.launch()
}

func (e *env) start() *process {
	e.t.Helper()
	e.ensureIndex()
	return e.launch()
}

func (e *env) launch() *process {
	e.t.Helper()
	return e.launchStream(e.streamName())
}

func (e *env) launchAll() *process {
	e.t.Helper()
	return e.launchStream("")
}

func (e *env) launchStream(stream string) *process {
	e.t.Helper()
	return e.launchStreamUntil(stream, "replication started")
}

func (e *env) launchScanning() *process {
	e.t.Helper()
	e.ensureIndex()
	return e.launchStreamUntil(e.streamName(), "starting a table scan", "resuming a table scan")
}

func (e *env) startScanning() *process {
	e.t.Helper()
	e.config = e.writeConfigWith(true)
	return e.launchScanning()
}

func (e *env) launchStreamUntil(stream string, markers ...string) *process {
	e.t.Helper()
	args := []string{"run", "-c", e.config, "--dlq-dir", e.dlqDir}
	if stream != "" {
		args = append(args, "--stream", stream)
	}
	log := &bytes.Buffer{}
	cmd := exec.Command(e.binary, args...)
	cmd.Stdout, cmd.Stderr = log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		e.t.Fatalf("start changeflow: %v", err)
	}
	p := &process{cmd: cmd, log: log}
	e.running = p
	e.t.Cleanup(func() { p.stop() })
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		out := log.String()
		for _, marker := range markers {
			if strings.Contains(out, marker) {
				return p
			}
		}
		if p.exited() {
			e.t.Fatalf("changeflow exited during startup:\n%s", out)
		}
		time.Sleep(100 * time.Millisecond)
	}
	e.t.Fatalf("changeflow did not reach %v within 30s:\n%s", markers, log.String())
	return nil
}

func (p *process) exited() bool {
	return p.cmd.ProcessState != nil && p.cmd.ProcessState.Exited()
}

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

func (e *env) document(id string) (map[string]any, bool) {
	e.t.Helper()
	return e.documentIn(e.writeIndex, id)
}

func (e *env) documentIn(index, id string) (map[string]any, bool) {
	e.t.Helper()
	status, payload := e.esRequest(http.MethodGet, "/"+index+"/_doc/"+id, "")
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
	dec := json.NewDecoder(strings.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&envelope); err != nil {
		e.t.Fatalf("decode document %s: %v", id, err)
	}
	return envelope.Source, envelope.Found
}

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
	e.refreshIndex(e.writeIndex)
}

func (e *env) refreshIndex(index string) {
	e.esRequest(http.MethodPost, "/"+index+"/_refresh", "")
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
	status, payload := e.esRequest(http.MethodGet, "/"+e.writeIndex+"/_count", "")
	if status >= 300 {
		e.t.Fatalf("count: status %d: %s", status, payload)
	}
	var result struct{ Count int }
	if err := json.Unmarshal([]byte(payload), &result); err != nil {
		e.t.Fatalf("decode count: %v", err)
	}
	return result.Count
}

func (e *env) documentIDs() []string {
	e.t.Helper()
	e.refresh()
	status, payload := e.esRequest(http.MethodGet, "/"+e.writeIndex+"/_search?size=200&_source=false&sort=_doc", "")
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

func (e *env) countInRange(from, to uint64) int {
	e.t.Helper()
	e.refresh()
	query := fmt.Sprintf(`{"query":{"range":{"id":{"gte":%d,"lte":%d}}}}`, from, to)
	status, payload := e.esRequest(http.MethodPost, "/"+e.writeIndex+"/_count", query)
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

// Two streams over one table must each receive every change, from a single connection
// to the source. Ten streams reading the same binlog separately would cost the master
// ten dump threads and ten copies of the same data.
func TestOneConnectionServesTwoStreams(t *testing.T) {
	e := setup(t)
	e.requireElasticsearch()
	second := e.secondStreamName()
	e.config = e.writeFanoutConfig()
	e.createIndexFor(e.streamName(), e.writeIndex)
	e.createIndexFor(second, second)
	t.Cleanup(func() {
		if _, err := e.db.Exec("DELETE FROM "+tableFor(e.metaDSN)+" WHERE stream = ?", second); err != nil {
			t.Logf("could not clear the checkpoint for %s: %v", second, err)
		}
	})
	p := e.launchAll()
	e.exec(`INSERT INTO orders (id,user_id,status,total_amount,note_latin1)
	        VALUES (20300,7,'paid',3.50,'café')`)
	e.exec("UPDATE orders SET status='shipped' WHERE id=20300")
	e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (20301,7,'draft',1.00)")
	e.exec("DELETE FROM orders WHERE id=20301")
	for _, index := range []string{e.writeIndex, second} {
		e.waitFor("both changes to reach "+index, func() bool {
			e.refreshIndex(index)
			doc, found := e.documentIn(index, "20300")
			if !found || doc["status"] != "shipped" {
				return false
			}
			_, deletedStillPresent := e.documentIn(index, "20301")
			return !deletedStillPresent
		})
	}
	if doc, _ := e.documentIn(e.writeIndex, "20300"); doc["note_latin1"] != "café" {
		t.Errorf("note_latin1 = %v in %s, want the column the first stream includes", doc["note_latin1"], e.writeIndex)
	}
	if doc, _ := e.documentIn(second, "20300"); doc["note_latin1"] != nil {
		t.Errorf("note_latin1 = %v in %s, want it absent: the second stream excludes it", doc["note_latin1"], second)
	}
	e.waitFor("the source to be serving exactly one replication connection", func() bool {
		return e.dumpThreads() == 1
	})
	if got := strings.Count(p.log.String(), "replication started"); got != 1 {
		t.Errorf("replication was started %d times, want once\nlog:\n%s", got, e.processLog())
	}
	p.stop()
	for _, stream := range []string{e.streamName(), second} {
		if e.checkpointPosition(stream) == "" {
			t.Errorf("stream %s recorded no position", stream)
		}
	}
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
	e.waitFor("some documents to arrive", func() bool { return e.countDocuments() > 0 })
	p.kill()
	before := e.countDocuments()
	t.Logf("killed with %d of %d documents written", before, total)
	e.start()
	e.waitFor(fmt.Sprintf("all %d documents to converge", total), func() bool {
		return e.countDocuments() == total
	})
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
	time.Sleep(3 * time.Second)
	if got := e.countInRange(firstID, firstID+rows-1); got != rows {
		t.Errorf("documents in the written range = %d, want %d", got, rows)
	}
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
	const firstID = 23000
	for i := 0; i < 25; i++ {
		e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,7,'paid',1.00)", firstID+i)
	}
	e.startWithSnapshot()
	e.waitFor("the pre-existing rows to be backfilled", func() bool {
		return e.countInRange(firstID, firstID+24) == 25
	})
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
	e.snapshotRate = 60
	p := e.startScanning()
	e.waitFor("the scan to start delivering rows", func() bool {
		return e.countInRange(firstID, firstID+119) > 0
	})
	p.kill()
	killedAt := e.countInRange(firstID, firstID+119)
	t.Logf("killed with %d of 120 rows backfilled", killedAt)
	if killedAt >= 120 {
		t.Fatalf("the scan had already finished, so nothing was interrupted")
	}
	e.startScanning()
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
	e.snapshotRate = 60
	e.startScanning()
	e.waitFor("the scan to start delivering rows", func() bool {
		return e.countInRange(firstID, firstID+199) > 0
	})
	if scanned := e.countInRange(firstID, firstID+199); scanned >= 200 {
		t.Fatalf("the scan finished before the change could be made, so nothing raced")
	}
	e.exec("UPDATE orders SET status='cancelled' WHERE id=?", firstID+199)
	e.waitFor("the whole table to be backfilled", func() bool {
		return e.countInRange(firstID, firstID+199) == 200
	})
	e.waitFor("the concurrent change to be the surviving version", func() bool {
		doc, found := e.document(fmt.Sprint(firstID + 199))
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
	e.requireElasticsearch()
	e.deleteIndex()
	e.createIndexManually()
	e.indexCreated = true
	e.start()
	e.exec(`INSERT INTO orders (id,user_id,status,total_amount,placed_at)
	        VALUES (25000,7,'paid',1.00,'2026-08-11 10:30:00.000')`)
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
	if strings.Contains(recorded, "\"body\":") {
		t.Errorf("the document body should not be recorded by default:\n%s", recorded)
	}
}

func (e *env) chTable() string { return "analytics." + e.index }

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

func (e *env) startClickHouse(snapshot bool) *process {
	e.t.Helper()
	e.requireClickHouse()
	e.config = e.writeConfigFor(sinkClickHouse, snapshot)
	e.createClickHouseTable(e.config)
	return e.launch()
}

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
	if got := e.chCount("id = 26000"); got != 1 {
		t.Errorf("rows for the key = %d, want 1 after an update", got)
	}
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

func (e *env) countViaAlias(from, to uint64) int {
	e.t.Helper()
	e.esRequest(http.MethodPost, "/"+e.alias+"/_refresh", "")
	query := fmt.Sprintf(`{"query":{"range":{"id":{"gte":%d,"lte":%d}}}}`, from, to)
	status, payload := e.esRequest(http.MethodPost, "/"+e.alias+"/_count", query)
	if status >= 300 {
		e.t.Fatalf("count via alias: status %d: %s", status, payload)
	}
	var result struct{ Count int }
	if err := json.Unmarshal([]byte(payload), &result); err != nil {
		e.t.Fatalf("decode count: %v", err)
	}
	return result.Count
}

func (e *env) indexSettings(index string) map[string]any {
	e.t.Helper()
	status, payload := e.esRequest(http.MethodGet, "/"+index+"/_settings", "")
	if status >= 300 {
		e.t.Fatalf("read settings of %s: status %d: %s", index, status, payload)
	}
	var byIndex map[string]struct {
		Settings struct {
			Index map[string]any `json:"index"`
		} `json:"settings"`
	}
	if err := json.Unmarshal([]byte(payload), &byIndex); err != nil {
		e.t.Fatalf("decode settings of %s: %v", index, err)
	}
	for _, entry := range byIndex {
		return entry.Settings.Index
	}
	e.t.Fatalf("no settings returned for %s", index)
	return nil
}

func (e *env) aliasTargets() []string {
	e.t.Helper()
	status, payload := e.esRequest(http.MethodGet, "/_alias/"+e.alias, "")
	if status == http.StatusNotFound {
		return nil
	}
	if status >= 300 {
		e.t.Fatalf("read alias: status %d: %s", status, payload)
	}
	var byIndex map[string]any
	if err := json.Unmarshal([]byte(payload), &byIndex); err != nil {
		e.t.Fatalf("decode alias: %v", err)
	}
	targets := make([]string, 0, len(byIndex))
	for index := range byIndex {
		targets = append(targets, index)
	}
	sort.Strings(targets)
	return targets
}

func (e *env) resnapshot() {
	e.t.Helper()
	out, err := exec.CommandContext(context.Background(), e.binary,
		"resnapshot", "-c", e.config, "--stream", e.streamName(), "--confirm").CombinedOutput()
	if err != nil {
		e.t.Fatalf("resnapshot: %v\n%s", err, out)
	}
}

// A mapping change is applied by filling a new index and moving readers to it. Readers
// must see a complete table throughout: never a partly built one, and never nothing.
func TestRebuildFillsANewIndexAndMovesReaders(t *testing.T) {
	e := setup(t)
	const firstID = 27000
	const rows = 40
	for i := 0; i < rows; i++ {
		e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,7,'paid',1.00)", firstID+i)
	}
	first := e.startWithSnapshot()
	e.waitFor("the original index to be filled", func() bool {
		return e.countInRange(firstID, firstID+rows-1) == rows
	})
	e.waitFor("readers to be pointed at the original index", func() bool {
		targets := e.aliasTargets()
		return len(targets) == 1 && targets[0] == e.index
	})
	originalIndex := e.writeIndex
	first.stop()
	e.resnapshot()
	e.writeIndex = e.index + "_v2"
	e.config = e.writeConfigWith(true)
	e.createIndex()
	e.launch()
	e.waitFor("the new index to be filled", func() bool {
		return e.countInRange(firstID, firstID+rows-1) == rows
	})
	e.waitFor("readers to be moved to the new index", func() bool {
		targets := e.aliasTargets()
		return len(targets) == 1 && targets[0] == e.writeIndex
	})
	if got := e.countViaAlias(firstID, firstID+rows-1); got != rows {
		t.Errorf("readers see %d of %d rows through the alias", got, rows)
	}
	status, _ := e.esRequest(http.MethodGet, "/"+originalIndex, "")
	if status >= 300 {
		t.Errorf("the previous index was removed, leaving nothing to roll back to (status %d)", status)
	}
	e.exec("UPDATE orders SET status='shipped' WHERE id=?", firstID)
	e.waitFor("a change after the rebuild to be applied", func() bool {
		doc, found := e.document(fmt.Sprint(firstID))
		return found && doc["status"] == "shipped"
	})
}

// A scan turns off refreshing and replication to fill an index quickly. Leaving either off
// would be worse than the time it saved: an index that never refreshes answers searches
// with nothing, and one with no replicas has no redundancy.
func TestASnapshotRestoresTheSettingsItRelaxed(t *testing.T) {
	e := setup(t)
	const firstID = 28000
	const rows = 40
	for i := 0; i < rows; i++ {
		e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,7,'paid',1.00)", firstID+i)
	}
	p := e.startWithSnapshot()
	e.waitFor("the scan to fill the index", func() bool {
		return e.countInRange(firstID, firstID+rows-1) == rows
	})
	e.waitFor("readers to be pointed at the scanned index", func() bool {
		targets := e.aliasTargets()
		return len(targets) == 1 && targets[0] == e.writeIndex
	})
	p.stop()
	settings := e.indexSettings(e.writeIndex)
	if got := settings["refresh_interval"]; got != nil {
		t.Errorf("refresh_interval = %v, want it back to the cluster default the index was created with", got)
	}
	if got := fmt.Sprint(settings["number_of_replicas"]); got != "0" {
		t.Errorf("number_of_replicas = %v, want the 0 the index was created with", got)
	}
	e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,7,'paid',1.00)", firstID+rows)
	e.start()
	deadline := time.Now().Add(settleTimeout)
	found := false
	for time.Now().Before(deadline) && !found {
		_, found = e.document(fmt.Sprint(firstID + rows))
		time.Sleep(500 * time.Millisecond)
	}
	if !found {
		t.Errorf("a document was not searchable without an explicit refresh:\n%s", e.processLog())
	}
}

// A rebuild that is interrupted must leave readers exactly where they were. Moving them to
// a half-filled index would show a partial table, and there would be nothing to move back
// to that is any better.
func TestAnInterruptedRebuildLeavesReadersOnTheOldIndex(t *testing.T) {
	e := setup(t)
	const firstID = 29000
	const rows = 300
	for i := 0; i < rows; i++ {
		e.exec("INSERT INTO orders (id,user_id,status,total_amount) VALUES (?,7,'paid',1.00)", firstID+i)
	}
	first := e.startWithSnapshot()
	e.waitFor("the original index to be filled", func() bool {
		return e.countInRange(firstID, firstID+rows-1) == rows
	})
	e.waitFor("readers to be pointed at the original index", func() bool {
		targets := e.aliasTargets()
		return len(targets) == 1 && targets[0] == e.index
	})
	originalIndex := e.writeIndex
	first.stop()
	e.resnapshot()
	e.snapshotRate = 50
	e.writeIndex = e.index + "_v2"
	e.config = e.writeConfigWith(true)
	e.createIndex()
	rebuild := e.launchScanning()
	interrupted := false
	deadline := time.Now().Add(settleTimeout)
	for time.Now().Before(deadline) && !interrupted {
		e.refreshIndex(e.writeIndex)
		if n := e.countInRange(firstID, firstID+rows-1); n > 0 && n < rows {
			interrupted = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	rebuild.kill()
	if !interrupted {
		t.Fatalf("the scan never observed as partial, so the interruption this test needs did not happen:\n%s",
			e.processLog())
	}
	if targets := e.aliasTargets(); len(targets) != 1 || targets[0] != originalIndex {
		t.Fatalf("readers are on %v, want them left on %s until the rebuild finished", targets, originalIndex)
	}
	if got := e.countViaAlias(firstID, firstID+rows-1); got != rows {
		t.Errorf("readers see %d of %d rows through the alias during a rebuild", got, rows)
	}
	e.launchScanning()
	e.waitFor("the rebuild to finish and readers to be moved", func() bool {
		targets := e.aliasTargets()
		return len(targets) == 1 && targets[0] == e.writeIndex
	})
	if got := e.countViaAlias(firstID, firstID+rows-1); got != rows {
		t.Errorf("readers see %d of %d rows after the rebuild finished", got, rows)
	}
	if got := e.indexSettings(e.writeIndex)["refresh_interval"]; got != nil {
		t.Errorf("refresh_interval = %v after the rebuild, want the cluster default", got)
	}
}
