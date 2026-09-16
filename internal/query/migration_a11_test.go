package query

import (
	"strings"
	"testing"
	"time"
)

func checksByOp(checks []MigrationCheck, op string) []MigrationCheck {
	var out []MigrationCheck
	for _, c := range checks {
		if c.Operation == op {
			out = append(out, c)
		}
	}
	return out
}

// A11.1: IF NOT EXISTS on a table the snapshot already has is a no-op, and the
// facts must come from the snapshot, not a hardcoded empty table.
func TestA11CreateTableIfNotExistsPresentReportsRealFacts(t *testing.T) {
	c := checkByOp(t, mustCheck(t, "CREATE TABLE IF NOT EXISTS users (id bigint, order_id bigint REFERENCES orders(id))"), "CREATE TABLE")
	if c.Safety != SafetySafe {
		t.Fatalf("no-op CREATE must stay safe, got %q", c.Safety)
	}
	if c.TableSize == nil || *c.TableSize != formatBytes(512<<20) {
		t.Errorf("table_size = %v, want the snapshot's %s", c.TableSize, formatBytes(512<<20))
	}
	if c.RowEstimate == nil || *c.RowEstimate != 2_000_000 {
		t.Errorf("row_estimate = %v, want 2000000", c.RowEstimate)
	}
	if !strings.Contains(c.Recommendation, "No-op") || !strings.Contains(c.Recommendation, "snapshot is current") {
		t.Errorf("expected no-op wording with the snapshot caveat, got %q", c.Recommendation)
	}
	// Live PG18 check: a skipped IF NOT EXISTS takes no lock on the relation.
	if c.LockType != "none" {
		t.Errorf("no-op lock_type = %q, want none", c.LockType)
	}
	// No FK is installed when the create is skipped, so no referenced-table lock.
	if c.Rationale != nil && strings.Contains(c.Rationale.Note, "SHARE ROW EXCLUSIVE") {
		t.Errorf("no FK is installed on a skipped create, got note %q", c.Rationale.Note)
	}
	if c.Statement == "" {
		t.Error("Statement must stay set for migration_sql passthrough")
	}
}

// A11.1: on a table absent from the snapshot the create still runs: unchanged.
func TestA11CreateTableIfNotExistsAbsentUnchanged(t *testing.T) {
	c := checkByOp(t, mustCheck(t, "CREATE TABLE IF NOT EXISTS new_t (id bigint)"), "CREATE TABLE")
	if c.Safety != SafetySafe {
		t.Fatalf("create must stay safe, got %q", c.Safety)
	}
	if c.TableSize == nil || *c.TableSize != "0 bytes" {
		t.Errorf("table_size = %v, want 0 bytes", c.TableSize)
	}
	if c.RowEstimate == nil || *c.RowEstimate != 0 {
		t.Errorf("row_estimate = %v, want 0", c.RowEstimate)
	}
	if strings.Contains(c.Recommendation, "No-op") {
		t.Errorf("an absent table is created, not a no-op: %q", c.Recommendation)
	}
}

// A8 shape: a stale capture cannot size the no-op, but it is still a no-op.
func TestA11CreateTableIfNotExistsStalePlanner(t *testing.T) {
	snap := migrationTestAnnotated()
	snap.Planner.Timestamp = time.Now().Add(-8 * 24 * time.Hour)
	checks, err := CheckMigration("CREATE TABLE IF NOT EXISTS users (id bigint)", snap)
	if err != nil {
		t.Fatal(err)
	}
	c := checkByOp(t, checks, "CREATE TABLE")
	if c.Safety != SafetySafe || !strings.Contains(c.Recommendation, "No-op") {
		t.Fatalf("stale capture must keep the no-op verdict, got %q / %q", c.Safety, c.Recommendation)
	}
	if c.TableSize != nil || c.RowEstimate != nil {
		t.Errorf("stale capture must not report size/rows, got %v / %v", c.TableSize, c.RowEstimate)
	}
	if c.Rationale == nil || !strings.Contains(c.Rationale.Note, "planner capture") {
		t.Errorf("expected a stale-capture note, got %v", c.Rationale)
	}
}

// A11.1: a second IF NOT EXISTS after a data load is a no-op too -- today it
// re-reports an empty table.
func TestA11CreateTableIfNotExistsIntrafileRecreate(t *testing.T) {
	ddl := "CREATE TABLE IF NOT EXISTS t (id bigint);\n" +
		"INSERT INTO t VALUES (1);\n" +
		"CREATE TABLE IF NOT EXISTS t (id bigint);"
	creates := checksByOp(mustCheck(t, ddl), "CREATE TABLE")
	if len(creates) != 2 {
		t.Fatalf("expected 2 CREATE TABLE checks, got %d", len(creates))
	}
	c := creates[1]
	if c.Safety != SafetySafe || !strings.Contains(c.Recommendation, "earlier in the file") {
		t.Errorf("recreate must be a no-op naming the earlier create, got %q / %q", c.Safety, c.Recommendation)
	}
	if c.TableSize != nil || c.RowEstimate != nil {
		t.Errorf("a file-created table has no captured size, got %v / %v", c.TableSize, c.RowEstimate)
	}
}

// A11.2: ANALYZE is SHARE UPDATE EXCLUSIVE and carries the target's size.
func TestA11AnalyzeClassified(t *testing.T) {
	c := checkByOp(t, mustCheck(t, "ANALYZE users;"), "ANALYZE")
	if c.Safety != SafetySafe || c.LockType != "SHARE UPDATE EXCLUSIVE" {
		t.Errorf("ANALYZE = %q / %q, want safe / SHARE UPDATE EXCLUSIVE", c.Safety, c.LockType)
	}
	if c.Table == nil || *c.Table != "users" {
		t.Errorf("table = %v, want users", c.Table)
	}
	if c.TableSize == nil || c.RowEstimate == nil {
		t.Errorf("ANALYZE should carry the snapshot size, got %v / %v", c.TableSize, c.RowEstimate)
	}
	if c.Statement == "" {
		t.Error("Statement must be set for migration_sql passthrough")
	}
}

func TestA11AnalyzeBareDatabaseForm(t *testing.T) {
	c := checkByOp(t, mustCheck(t, "ANALYZE;"), "ANALYZE")
	if c.Safety != SafetySafe {
		t.Fatalf("bare ANALYZE = %q, want safe", c.Safety)
	}
	if c.Table != nil {
		t.Errorf("bare ANALYZE has no single table, got %v", *c.Table)
	}
}

// VACUUM (FULL) takes stronger locks: it must not be swept into the safe path.
func TestA11VacuumStaysUnrecognized(t *testing.T) {
	for _, ddl := range []string{"VACUUM users;", "VACUUM FULL users;"} {
		c := checkByOp(t, mustCheck(t, ddl), "UNRECOGNIZED")
		if c.Safety == SafetySafe {
			t.Errorf("%s must not be rated safe", ddl)
		}
	}
}

// A11.2: CREATE STATISTICS is SHARE UPDATE EXCLUSIVE.
func TestA11CreateStatisticsClassified(t *testing.T) {
	c := checkByOp(t, mustCheck(t, "CREATE STATISTICS s1 (dependencies) ON id, user_id FROM orders;"), "CREATE STATISTICS")
	if c.Safety != SafetySafe || c.LockType != "SHARE UPDATE EXCLUSIVE" {
		t.Errorf("CREATE STATISTICS = %q / %q, want safe / SHARE UPDATE EXCLUSIVE", c.Safety, c.LockType)
	}
	if c.Table == nil || *c.Table != "orders" {
		t.Errorf("table = %v, want orders", c.Table)
	}
	if c.Statement == "" {
		t.Error("Statement must be set for migration_sql passthrough")
	}
}

// A11.2: SELECT setval is a sequence reset, safe, and passes through.
func TestA11SetvalClassified(t *testing.T) {
	for _, ddl := range []string{
		"SELECT setval('users_id_seq', 42);",
		"SELECT setval('users_id_seq', (SELECT max(id) FROM users));",
		"SELECT pg_catalog.setval('users_id_seq', 42);",
	} {
		c := checkByOp(t, mustCheck(t, ddl), "SELECT setval")
		if c.Safety != SafetySafe {
			t.Errorf("%s = %q, want safe", ddl, c.Safety)
		}
		if c.Statement == "" {
			t.Errorf("%s: Statement must be set for migration_sql passthrough", ddl)
		}
	}
}

// Any other SELECT still gets the manual-review verdict.
func TestA11NonSetvalSelectStaysUnrecognized(t *testing.T) {
	c := checkByOp(t, mustCheck(t, "SELECT max(id) FROM users;"), "UNRECOGNIZED")
	if c.Safety == SafetySafe {
		t.Error("a non-setval SELECT must not be rated safe")
	}
}

// A11.2: CREATE SCHEMA is catalog-only; embedded DDL stays unmodeled.
func TestA11CreateSchemaClassified(t *testing.T) {
	c := checkByOp(t, mustCheck(t, "CREATE SCHEMA reporting;"), "CREATE SCHEMA")
	if c.Safety != SafetySafe || c.LockType != "none" {
		t.Errorf("CREATE SCHEMA = %q / %q, want safe / none", c.Safety, c.LockType)
	}
	if c.Statement == "" {
		t.Error("Statement must be set for migration_sql passthrough")
	}
	c = checkByOp(t, mustCheck(t, "CREATE SCHEMA reporting CREATE TABLE reporting.t (id int);"), "UNRECOGNIZED")
	if c.Safety == SafetySafe {
		t.Error("CREATE SCHEMA with embedded DDL must not be rated safe")
	}
}

// A11.2: CREATE EXTENSION is caution (the script's DDL is unknown) and it
// invalidates same-file emptiness assumptions because it may write rows.
func TestA11CreateExtensionClassified(t *testing.T) {
	checks := mustCheck(t, "CREATE EXTENSION IF NOT EXISTS pgcrypto;")
	c := checkByOp(t, checks, "CREATE EXTENSION")
	if c.Safety != SafetyCaution {
		t.Fatalf("CREATE EXTENSION = %q, want caution", c.Safety)
	}
	if strings.Contains(c.Recommendation, "DANGEROUS") {
		t.Errorf("caution prose must not say DANGEROUS: %q", c.Recommendation)
	}
	if got := ComposeMigrationSQL(checks); got != "" {
		t.Errorf("caution with no rewrite must suppress migration_sql, got:\n%s", got)
	}
}

func TestA11CreateExtensionInvalidatesCreatedTable(t *testing.T) {
	without := checkByOp(t, mustCheck(t, "CREATE TABLE IF NOT EXISTS t (id bigint);\nCREATE INDEX ON t (id)"), "CREATE INDEX")
	if without.Safety != SafetySafe {
		t.Fatalf("baseline: index on a file-created table should be safe, got %q", without.Safety)
	}
	with := checkByOp(t, mustCheck(t, "CREATE TABLE IF NOT EXISTS t (id bigint);\nCREATE EXTENSION hstore;\nCREATE INDEX ON t (id)"), "CREATE INDEX")
	if with.Safety == SafetySafe {
		t.Error("the extension script may write rows, so the created table is no longer known empty")
	}
}

// A11.3: RENAME VALUE is caution; ADD VALUE stays unmodeled.
func TestA11AlterEnumRenameValue(t *testing.T) {
	c := checkByOp(t, mustCheck(t, "ALTER TYPE mood RENAME VALUE 'sad' TO 'blue';"), "RENAME")
	if c.Safety != SafetyCaution {
		t.Fatalf("RENAME VALUE = %q, want caution", c.Safety)
	}
	if !strings.Contains(c.Recommendation, "'sad'") {
		t.Errorf("prose must name the old literal, got %q", c.Recommendation)
	}
	if c.RollbackDDL == nil || !strings.Contains(*c.RollbackDDL, "'blue'") || !strings.Contains(*c.RollbackDDL, "'sad'") {
		t.Errorf("rollback_ddl must reverse the rename, got %v", c.RollbackDDL)
	}
}

func TestA11AlterEnumAddValueStaysUnrecognized(t *testing.T) {
	for _, ddl := range []string{
		"ALTER TYPE mood ADD VALUE 'neutral';",
		"ALTER TYPE mood ADD VALUE IF NOT EXISTS 'neutral' BEFORE 'sad';",
	} {
		c := checkByOp(t, mustCheck(t, ddl), "UNRECOGNIZED")
		if c.Safety == SafetySafe {
			t.Errorf("%s must not be rated safe", ddl)
		}
	}
}

// The point of classifying these was to un-suppress migration_sql: a file with
// one dangerous statement plus ANALYZE/setval must pass the safe ones through.
func TestA11ComposePassthroughIncludesNewStatements(t *testing.T) {
	checks := mustCheck(t, "CREATE INDEX idx ON users (id); ANALYZE users; SELECT setval('users_id_seq', 42);")
	sql := ComposeMigrationSQL(checks)
	if !strings.Contains(sql, "ANALYZE users") {
		t.Errorf("migration_sql missing ANALYZE:\n%s", sql)
	}
	if !strings.Contains(sql, "setval") {
		t.Errorf("migration_sql missing setval:\n%s", sql)
	}
	if !strings.Contains(sql, "CREATE INDEX CONCURRENTLY idx") {
		t.Errorf("migration_sql missing the rewritten index:\n%s", sql)
	}
}
