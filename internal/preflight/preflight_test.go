package preflight

import "testing"

// goodVars is a MySQL 8 configured exactly the way changeflow needs it.
func goodVars() map[string]string {
	return map[string]string{
		"version":                    "8.0.36",
		"log_bin":                    "ON",
		"binlog_format":              "ROW",
		"binlog_row_image":           "FULL",
		"binlog_row_metadata":        "FULL",
		"gtid_mode":                  "ON",
		"enforce_gtid_consistency":   "ON",
		"server_id":                  "1",
		"binlog_expire_logs_seconds": "604800",
		"log_replica_updates":        "ON",
		"binlog_row_value_options":   "",
	}
}

func goodGrants() []string {
	return []string{"GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `cdc`@`%`"}
}

func find(t *testing.T, r Report, name string) Check {
	t.Helper()
	for _, c := range r.Checks {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("report has no check named %q", name)
	return Check{}
}

func TestEvaluateAcceptsCorrectlyConfiguredServer(t *testing.T) {
	r := Evaluate(goodVars(), goodGrants())

	if !r.OK() {
		t.Fatalf("expected OK, got failures: %v", r.Failures())
	}
	if len(r.Failures()) != 0 {
		t.Fatalf("expected no failures, got %v", r.Failures())
	}
}

func TestEvaluateRejectsStatementBinlogFormat(t *testing.T) {
	vars := goodVars()
	vars["binlog_format"] = "STATEMENT"

	r := Evaluate(vars, goodGrants())

	if r.OK() {
		t.Fatal("expected failure for binlog_format=STATEMENT")
	}
	c := find(t, r, "binlog_format")
	if c.OK || c.Severity != Required {
		t.Fatalf("expected required failure, got %+v", c)
	}
	if c.Got != "STATEMENT" || c.Want != "ROW" {
		t.Fatalf("expected got=STATEMENT want=ROW, got %+v", c)
	}
}

func TestEvaluateRejectsMinimalRowImage(t *testing.T) {
	vars := goodVars()
	vars["binlog_row_image"] = "MINIMAL"

	r := Evaluate(vars, goodGrants())

	if c := find(t, r, "binlog_row_image"); c.OK {
		t.Fatalf("expected failure for MINIMAL row image, got %+v", c)
	}
	if r.OK() {
		t.Fatal("expected report to fail")
	}
}

// A 5.7 server does not expose binlog_row_metadata at all. That must fail loudly
// rather than being treated as "not set", because without it unsigned columns
// arrive negative and ENUMs arrive as integers.
func TestEvaluateRejectsMissingRowMetadata(t *testing.T) {
	vars := goodVars()
	delete(vars, "binlog_row_metadata")
	vars["version"] = "5.7.44"

	r := Evaluate(vars, goodGrants())

	c := find(t, r, "binlog_row_metadata")
	if c.OK || c.Severity != Required {
		t.Fatalf("expected required failure, got %+v", c)
	}
	if c.Got != "<unsupported>" {
		t.Fatalf("expected got=<unsupported>, got %q", c.Got)
	}
	if r.OK() {
		t.Fatal("expected report to fail")
	}
}

func TestEvaluateRejectsGtidModeOff(t *testing.T) {
	vars := goodVars()
	vars["gtid_mode"] = "OFF"

	r := Evaluate(vars, goodGrants())

	if c := find(t, r, "gtid_mode"); c.OK {
		t.Fatalf("expected failure, got %+v", c)
	}
	if r.OK() {
		t.Fatal("expected report to fail")
	}
}

func TestEvaluateAcceptsLowercaseValues(t *testing.T) {
	vars := goodVars()
	vars["binlog_format"] = "row"
	vars["gtid_mode"] = "on"
	vars["log_bin"] = "on"
	vars["binlog_row_image"] = "full"
	vars["binlog_row_metadata"] = "full"

	r := Evaluate(vars, goodGrants())

	if !r.OK() {
		t.Fatalf("expected case-insensitive comparison to pass, got %v", r.Failures())
	}
}

func TestEvaluateRejectsZeroServerID(t *testing.T) {
	vars := goodVars()
	vars["server_id"] = "0"

	r := Evaluate(vars, goodGrants())

	if c := find(t, r, "server_id"); c.OK {
		t.Fatalf("expected failure for server_id=0, got %+v", c)
	}
}

func TestEvaluateAcceptsAllPrivileges(t *testing.T) {
	r := Evaluate(goodVars(), []string{"GRANT ALL PRIVILEGES ON *.* TO `root`@`localhost` WITH GRANT OPTION"})

	for _, name := range []string{"grant:REPLICATION SLAVE", "grant:REPLICATION CLIENT", "grant:SELECT"} {
		if c := find(t, r, name); !c.OK {
			t.Fatalf("expected %s satisfied by ALL PRIVILEGES, got %+v", name, c)
		}
	}
}

func TestEvaluateRejectsMissingReplicationClientGrant(t *testing.T) {
	r := Evaluate(goodVars(), []string{"GRANT SELECT, REPLICATION SLAVE ON *.* TO `cdc`@`%`"})

	if c := find(t, r, "grant:REPLICATION CLIENT"); c.OK {
		t.Fatalf("expected missing REPLICATION CLIENT to fail, got %+v", c)
	}
	if c := find(t, r, "grant:REPLICATION SLAVE"); !c.OK {
		t.Fatalf("expected REPLICATION SLAVE to pass, got %+v", c)
	}
	if r.OK() {
		t.Fatal("expected report to fail")
	}
}

func TestEvaluateAcceptsTableScopedSelectGrant(t *testing.T) {
	r := Evaluate(goodVars(), []string{
		"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `cdc`@`%`",
		"GRANT SELECT ON `shop`.* TO `cdc`@`%`",
	})

	if c := find(t, r, "grant:SELECT"); !c.OK {
		t.Fatalf("expected scoped SELECT grant to satisfy the check, got %+v", c)
	}
}

// Retention shorter than a day is a real risk but not a reason to refuse to
// start, so it must be advisory: reported, counted, and not fatal.
func TestShortBinlogRetentionIsAdvisoryNotFatal(t *testing.T) {
	vars := goodVars()
	vars["binlog_expire_logs_seconds"] = "3600"

	r := Evaluate(vars, goodGrants())

	c := find(t, r, "binlog retention")
	if c.OK {
		t.Fatalf("expected advisory warning for 1h retention, got %+v", c)
	}
	if c.Severity != Advisory {
		t.Fatalf("expected Advisory severity, got %v", c.Severity)
	}
	if !r.OK() {
		t.Fatalf("advisory failures must not make the report fail: %v", r.Failures())
	}
	if len(r.Warnings()) != 1 {
		t.Fatalf("expected exactly 1 warning, got %v", r.Warnings())
	}
}

func TestZeroRetentionMeansNeverExpireAndIsFine(t *testing.T) {
	vars := goodVars()
	vars["binlog_expire_logs_seconds"] = "0"

	r := Evaluate(vars, goodGrants())

	if c := find(t, r, "binlog retention"); !c.OK {
		t.Fatalf("0 means binlogs never expire, which is safe for us: %+v", c)
	}
}

func TestEnforceGtidConsistencyIsAdvisory(t *testing.T) {
	vars := goodVars()
	vars["enforce_gtid_consistency"] = "OFF"

	r := Evaluate(vars, goodGrants())

	c := find(t, r, "enforce_gtid_consistency")
	if c.OK || c.Severity != Advisory {
		t.Fatalf("expected advisory failure, got %+v", c)
	}
	if !r.OK() {
		t.Fatal("enforce_gtid_consistency must not be fatal")
	}
}

func TestEveryCheckExplainsWhyItMatters(t *testing.T) {
	r := Evaluate(goodVars(), goodGrants())

	for _, c := range r.Checks {
		if c.Why == "" {
			t.Errorf("check %q has no Why; an operator reading a failure needs the reason", c.Name)
		}
	}
}

func TestEvaluateRejectsMariaDB(t *testing.T) {
	vars := goodVars()
	vars["version"] = "11.4.2-MariaDB-1:11.4.2+maria~ubu2404"

	r := Evaluate(vars, goodGrants())

	c := find(t, r, "server version")
	if c.OK || c.Severity != Required {
		t.Fatalf("expected required failure for MariaDB, got %+v", c)
	}
	if r.OK() {
		t.Fatal("expected report to fail")
	}
}

func TestEvaluateRejectsMySQLTooOldForRowMetadata(t *testing.T) {
	vars := goodVars()
	vars["version"] = "5.7.44-log"
	delete(vars, "binlog_row_metadata")

	r := Evaluate(vars, goodGrants())

	if c := find(t, r, "server version"); c.OK {
		t.Fatalf("expected 5.7 to fail the version check, got %+v", c)
	}
}

func TestEvaluateAcceptsVersionSuffixes(t *testing.T) {
	for _, v := range []string{"8.0.36", "8.0.1", "8.4.9", "9.0.0", "8.0.36-log", "8.4.0-0ubuntu0.24.04.1"} {
		vars := goodVars()
		vars["version"] = v

		if c := find(t, Evaluate(vars, goodGrants()), "server version"); !c.OK {
			t.Errorf("expected %q to be accepted, got %+v", v, c)
		}
	}
}

func TestEvaluateRejectsVersionsBelowMinimum(t *testing.T) {
	for _, v := range []string{"8.0.0", "5.7.44", "5.6.51", "8", "unknown"} {
		vars := goodVars()
		vars["version"] = v

		if c := find(t, Evaluate(vars, goodGrants()), "server version"); c.OK {
			t.Errorf("expected %q to be rejected, got %+v", v, c)
		}
	}
}

func TestEvaluateRejectsMissingVersion(t *testing.T) {
	vars := goodVars()
	delete(vars, "version")

	if c := find(t, Evaluate(vars, goodGrants()), "server version"); c.OK {
		t.Fatalf("expected failure when version is unknown, got %+v", c)
	}
}

func TestReportGetExposesObservedValues(t *testing.T) {
	r := Evaluate(goodVars(), goodGrants())

	got, ok := r.Get("server_id")
	if !ok {
		t.Fatal("expected server_id to be present")
	}
	if got != "1" {
		t.Fatalf("got %q, want 1", got)
	}
	if _, ok := r.Get("no_such_check"); ok {
		t.Fatal("expected missing check to report absent")
	}
}

// PARTIAL_JSON turns an UPDATE to a JSON column into a diff. Nothing downstream can
// apply one, and nothing would report that a document had been replaced by a
// description of a change, so this has to be caught before a stream starts.
func TestEvaluateRejectsPartialJSONLogging(t *testing.T) {
	vars := goodVars()
	vars["binlog_row_value_options"] = "PARTIAL_JSON"

	r := Evaluate(vars, goodGrants())

	if r.OK() {
		t.Fatal("a server logging JSON diffs was accepted")
	}
	c := find(t, r, "binlog_row_value_options")
	if c.OK || c.Severity != Required {
		t.Errorf("check = %+v, want a required failure", c)
	}
	if c.Got != "PARTIAL_JSON" {
		t.Errorf("got = %q, want the offending value reported back", c.Got)
	}
}

// The default is empty, and an empty value has to read as such rather than as a blank
// in the output.
func TestEvaluateReportsAnEmptyRowValueOptionsReadably(t *testing.T) {
	c := find(t, Evaluate(goodVars(), goodGrants()), "binlog_row_value_options")

	if !c.OK {
		t.Errorf("the default was reported as a failure: %+v", c)
	}
	if c.Got != "empty" {
		t.Errorf("got = %q, want it spelled out", c.Got)
	}
}

// A server too old to have the setting cannot be logging diffs, so its absence is not a
// problem to report.
func TestEvaluateAcceptsAServerWithoutRowValueOptions(t *testing.T) {
	vars := goodVars()
	delete(vars, "binlog_row_value_options")

	r := Evaluate(vars, goodGrants())

	if c := find(t, r, "binlog_row_value_options"); !c.OK {
		t.Errorf("an absent setting was treated as a failure: %+v", c)
	}
}
