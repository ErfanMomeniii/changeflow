// Package preflight validates that a MySQL server is configured for change data
// capture before changeflow attempts to replicate from it.
//
// Every check exists because getting it wrong produces either an immediate
// failure or, worse, silently incorrect data. The checks are split into two
// severities: Required failures mean changeflow refuses to start, Advisory
// failures are reported and logged but do not block.
package preflight

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// Severity distinguishes "refuse to start" from "tell the operator".
type Severity int

const (
	// Required means changeflow cannot replicate correctly without it.
	Required Severity = iota
	// Advisory means it is a risk worth knowing about, not a blocker.
	Advisory
)

func (s Severity) String() string {
	if s == Required {
		return "required"
	}
	return "advisory"
}

// Check is the result of validating one server setting or grant.
type Check struct {
	Name     string
	Want     string
	Got      string
	OK       bool
	Severity Severity
	// Why explains the consequence of this check failing. An operator reading a
	// failure at 3am needs the reason, not just the setting name.
	Why string
}

// Report is the outcome of a full preflight run.
type Report struct {
	Checks []Check
}

// OK reports whether every Required check passed. Advisory failures do not
// affect it.
func (r Report) OK() bool {
	return len(r.Failures()) == 0
}

// Failures returns the Required checks that did not pass.
func (r Report) Failures() []Check {
	return r.filter(Required)
}

// Warnings returns the Advisory checks that did not pass.
func (r Report) Warnings() []Check {
	return r.filter(Advisory)
}

// Get returns the observed value of a named check.
func (r Report) Get(name string) (string, bool) {
	for _, c := range r.Checks {
		if c.Name == name {
			return c.Got, true
		}
	}
	return "", false
}

func (r Report) filter(sev Severity) []Check {
	var out []Check
	for _, c := range r.Checks {
		if !c.OK && c.Severity == sev {
			out = append(out, c)
		}
	}
	return out
}

// Vars is the set of global variables preflight needs. Values missing from the
// server are simply absent from the map, which is meaningful: an absent
// binlog_row_metadata means the server is too old to support it.
var Vars = []string{
	"version",
	"log_bin",
	"binlog_format",
	"binlog_row_image",
	"binlog_row_metadata",
	"gtid_mode",
	"enforce_gtid_consistency",
	"server_id",
	"binlog_expire_logs_seconds",
	"log_replica_updates",
	"binlog_row_value_options",
}

// Evaluate applies every check to a set of global variables and SHOW GRANTS
// lines. It performs no I/O, which is what makes the rules testable without a
// database.
func Evaluate(vars map[string]string, grants []string) Report {
	get := func(name string) (string, bool) {
		v, ok := vars[strings.ToLower(name)]
		return v, ok
	}

	// equals builds a check that a variable holds an expected value,
	// case-insensitively, treating an absent variable as unsupported.
	equals := func(name, want string, sev Severity, why string) Check {
		got, ok := get(name)
		if !ok {
			got = "<unsupported>"
		}
		return Check{
			Name:     name,
			Want:     want,
			Got:      got,
			OK:       ok && strings.EqualFold(strings.TrimSpace(got), want),
			Severity: sev,
			Why:      why,
		}
	}

	r := Report{}

	r.Checks = append(r.Checks, serverCheck(get))

	r.Checks = append(r.Checks,
		equals("log_bin", "ON", Required,
			"Without a binary log there is nothing to replicate from."),

		equals("binlog_format", "ROW", Required,
			"STATEMENT and MIXED log SQL text rather than row images, so there are no before/after values to apply to a sink."),

		equals("binlog_row_image", "FULL", Required,
			"MINIMAL and NOBLOB omit unchanged columns, so an UPDATE would silently drop fields from the sink document, and a DELETE may not carry the key."),

		equals("binlog_row_metadata", "FULL", Required,
			"Carries column names, signedness, and ENUM/SET labels in the binlog. Without it unsigned columns arrive negative and ENUMs arrive as integers - corruption that is invisible until someone compares values. Requires MySQL 8.0.1 or newer."),

		equals("gtid_mode", "ON", Required,
			"GTID positions survive binlog rotation and master failover; file+position does not."),

		equals("enforce_gtid_consistency", "ON", Advisory,
			"Without it, statements unsafe for GTID replication are allowed, which can produce a GTID set we cannot reason about."),
	)

	r.Checks = append(r.Checks, partialJSONCheck(get))
	r.Checks = append(r.Checks, serverIDCheck(get))
	r.Checks = append(r.Checks, retentionCheck(get))
	r.Checks = append(r.Checks,
		equals("log_replica_updates", "ON", Advisory,
			"Only matters when pointing changeflow at a read replica: without it the replica's own binlog omits writes replicated from its master, so we would miss them."),
	)

	for _, priv := range []string{"REPLICATION SLAVE", "REPLICATION CLIENT", "SELECT"} {
		r.Checks = append(r.Checks, grantCheck(priv, grants))
	}

	return r
}

// partialJSONCheck rejects a server that logs JSON updates as diffs.
//
// Applying a diff needs the previous document, which a sink does not give back, so the
// column would silently hold a description of a change instead of a document. A server
// without the variable cannot be logging diffs, so its absence passes.
func partialJSONCheck(get func(string) (string, bool)) Check {
	const why = "PARTIAL_JSON logs an UPDATE to a JSON column as a diff rather than the new value, " +
		"which cannot be applied to a destination that holds only the current document. " +
		"The result is a column carrying a description of a change instead of a document, with nothing reporting it."

	got, ok := get("binlog_row_value_options")
	if !ok {
		// Older servers have no such setting, so there is nothing to be wrong.
		return Check{Name: "binlog_row_value_options", Want: "empty", Got: "<unsupported>", OK: true, Severity: Required, Why: why}
	}

	trimmed := strings.TrimSpace(got)
	shown := trimmed
	if shown == "" {
		shown = "empty"
	}
	return Check{
		Name:     "binlog_row_value_options",
		Want:     "empty",
		Got:      shown,
		OK:       trimmed == "",
		Severity: Required,
		Why:      why,
	}
}

// serverCheck rejects servers whose replication protocol we do not speak.
// MariaDB reports itself through the same version variable but uses a different
// GTID format and has no binlog_row_metadata, so it fails here with a clear
// message rather than as a confusing GTID parse error mid-stream.
func serverCheck(get func(string) (string, bool)) Check {
	const minVersion = "8.0.1"

	got, ok := get("version")
	if !ok {
		got = "<unknown>"
	}

	why := "MariaDB uses a different GTID format and does not implement binlog_row_metadata. MySQL below " + minVersion + " has no row metadata either, so column names, signedness, and ENUM labels would be missing."

	switch {
	case !ok:
		return Check{Name: "server version", Want: "MySQL >= " + minVersion, Got: got, OK: false, Severity: Required, Why: why}
	case strings.Contains(strings.ToLower(got), "mariadb"):
		return Check{Name: "server version", Want: "MySQL >= " + minVersion, Got: got + " (MariaDB)", OK: false, Severity: Required, Why: why}
	default:
		return Check{Name: "server version", Want: "MySQL >= " + minVersion, Got: got, OK: atLeastVersion(got, minVersion), Severity: Required, Why: why}
	}
}

// atLeastVersion compares dotted version prefixes numerically, ignoring any
// suffix such as "-log" or "-0ubuntu0.22.04.1".
func atLeastVersion(got, min string) bool {
	parse := func(s string) []int {
		// Keep only the leading dotted-numeric portion.
		end := strings.IndexFunc(s, func(r rune) bool {
			return r != '.' && (r < '0' || r > '9')
		})
		if end >= 0 {
			s = s[:end]
		}
		var out []int
		for _, part := range strings.Split(s, ".") {
			n, err := strconv.Atoi(part)
			if err != nil {
				return out
			}
			out = append(out, n)
		}
		return out
	}

	g, m := parse(got), parse(min)
	if len(g) == 0 {
		return false
	}
	for i := range m {
		if i >= len(g) {
			return false // fewer components than required, e.g. "8" vs "8.0.1"
		}
		if g[i] != m[i] {
			return g[i] > m[i]
		}
	}
	return true
}

func serverIDCheck(get func(string) (string, bool)) Check {
	got, ok := get("server_id")
	if !ok {
		got = "<unsupported>"
	}
	id, err := strconv.ParseUint(strings.TrimSpace(got), 10, 32)
	return Check{
		Name:     "server_id",
		Want:     "non-zero",
		Got:      got,
		OK:       ok && err == nil && id != 0,
		Severity: Required,
		Why:      "A server with server_id=0 has binary logging effectively disabled and refuses replica connections.",
	}
}

// retentionCheck warns when binlogs expire sooner than a day. Retention must
// exceed both the longest expected outage and the longest snapshot, because the
// binlog is not consumed while a snapshot runs.
func retentionCheck(get func(string) (string, bool)) Check {
	const minSeconds = 24 * 60 * 60

	why := "The binlog is not consumed for a stream while its snapshot runs, so retention shorter than a snapshot purges our start position before streaming begins - a loop that never converges. It also bounds how long changeflow can be down."

	got, ok := get("binlog_expire_logs_seconds")
	if !ok {
		return Check{Name: "binlog retention", Want: ">= 24h", Got: "<unknown>", OK: false, Severity: Advisory, Why: why}
	}

	secs, err := strconv.ParseUint(strings.TrimSpace(got), 10, 64)
	display := got + "s"
	if err == nil && secs == 0 {
		display = "never expires"
	}

	return Check{
		Name:     "binlog retention",
		Want:     ">= 24h",
		Got:      display,
		OK:       err == nil && (secs == 0 || secs >= minSeconds),
		Severity: Advisory,
		Why:      why,
	}
}

// grantCheck looks for a privilege in SHOW GRANTS output. ALL PRIVILEGES
// satisfies everything. The match is textual, so a privilege granted on a
// database or table scope counts - which is correct for SELECT, and harmless for
// the replication privileges since MySQL only accepts those globally.
func grantCheck(priv string, grants []string) Check {
	found := false
	for _, line := range grants {
		up := strings.ToUpper(line)
		if !strings.HasPrefix(strings.TrimSpace(up), "GRANT") {
			continue
		}
		if strings.Contains(up, "ALL PRIVILEGES") || strings.Contains(up, priv) {
			found = true
			break
		}
	}

	why := map[string]string{
		"REPLICATION SLAVE":  "Required to open a binlog dump stream as a replica.",
		"REPLICATION CLIENT": "Required to read the server's replication position and status.",
		"SELECT":             "Required to read table schemas and to snapshot existing rows.",
	}[priv]

	return Check{
		Name:     "grant:" + priv,
		Want:     "granted",
		Got:      map[bool]string{true: "granted", false: "missing"}[found],
		OK:       found,
		Severity: Required,
		Why:      why,
	}
}

// Reader reads the facts preflight needs from a live server. Splitting this from
// Evaluate is what keeps the rules unit-testable without a database.
type Reader interface {
	GlobalVars(ctx context.Context, names []string) (map[string]string, error)
	Grants(ctx context.Context) ([]string, error)
}

// DBReader reads preflight facts over an existing database/sql connection.
type DBReader struct {
	DB *sql.DB
}

// GlobalVars returns the requested global variables, lowercased by name.
// Variables the server does not know are omitted rather than defaulted, which is
// how an old server's missing settings stay distinguishable from unset ones.
func (r DBReader) GlobalVars(ctx context.Context, names []string) (map[string]string, error) {
	want := make(map[string]bool, len(names))
	for _, n := range names {
		want[strings.ToLower(n)] = true
	}

	// One unfiltered SHOW rather than a query per name: SHOW does not accept
	// placeholders, and interpolating names into SQL is not worth it when the
	// whole set is a few hundred rows.
	rows, err := r.DB.QueryContext(ctx, "SHOW GLOBAL VARIABLES")
	if err != nil {
		return nil, fmt.Errorf("read global variables: %w", err)
	}
	defer rows.Close()

	out := make(map[string]string, len(names))
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("scan global variable: %w", err)
		}
		if k = strings.ToLower(k); want[k] {
			out[k] = v
		}
	}
	return out, rows.Err()
}

// Grants returns the raw SHOW GRANTS lines for the connected user.
func (r DBReader) Grants(ctx context.Context) ([]string, error) {
	rows, err := r.DB.QueryContext(ctx, "SHOW GRANTS FOR CURRENT_USER()")
	if err != nil {
		return nil, fmt.Errorf("show grants: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, fmt.Errorf("scan grant: %w", err)
		}
		out = append(out, line)
	}
	return out, rows.Err()
}

// Run reads the server's configuration and evaluates every check against it.
func Run(ctx context.Context, r Reader) (Report, error) {
	vars, err := r.GlobalVars(ctx, Vars)
	if err != nil {
		return Report{}, err
	}
	grants, err := r.Grants(ctx)
	if err != nil {
		return Report{}, err
	}
	return Evaluate(vars, grants), nil
}
