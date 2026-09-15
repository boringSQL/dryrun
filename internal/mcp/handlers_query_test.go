package mcp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/pkg/lint"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// Smoke tests for query-family tools: validate_query, check_migration,
// advise. Each subtest issues one representative call against the
// demo schema; failures here mean handler wiring or arg parsing has drifted.
func TestQueryHandlers_OfflineSmoke(t *testing.T) {
	c := setupOfflineTest(t)

	t.Run("validate_query", func(t *testing.T) {
		out := callTool(t, c, "validate_query", map[string]any{
			"sql": "SELECT * FROM users WHERE email = 'test@example.com'",
		})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("check_migration", func(t *testing.T) {
		out := callTool(t, c, "check_migration", map[string]any{
			"ddl": "ALTER TABLE users ADD COLUMN phone TEXT",
		})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("advise", func(t *testing.T) {
		out := callTool(t, c, "advise", map[string]any{
			"sql": "SELECT * FROM tasks WHERE status = 'open'",
		})
		if out == "" {
			t.Fatal("empty result")
		}
	})
}

// Pins that validate_query output is JSON with an _meta block carrying
// mode=offline. Without this, clients can't tell which mode produced the
// validation result, which matters for actual diagnostics on the user side.
func TestValidateQuery_InjectsMeta(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "validate_query", map[string]any{
		"sql": "SELECT 1",
	})
	var decoded map[string]any
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("expected JSON output: %v\n%s", err, out)
	}
	meta, ok := decoded["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("expected _meta in validate_query output, got: %s", out)
	}
	if meta["mode"] != "offline" {
		t.Errorf("expected mode=offline, got %v", meta["mode"])
	}
}

// A misspelled name should come back as a patch, not only as prose: the
// corrected query, the list of substitutions, and a follow-up that uses it.
func TestValidateQuery_ReturnsCorrectedSQL(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "validate_query", map[string]any{
		"sql": "SELECT u.emial FROM users u WHERE u.user_id = 1",
	})

	var decoded map[string]any
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("expected JSON output: %v\n%s", err, out)
	}
	if decoded["valid"] != false {
		t.Fatalf("expected the query to be invalid: %s", out)
	}
	if got := decoded["corrected_sql"]; got != "SELECT u.email FROM users u WHERE u.user_id = 1" {
		t.Fatalf("corrected_sql = %v\n%s", got, out)
	}

	fixes, _ := decoded["fixes"].([]any)
	if len(fixes) != 1 {
		t.Fatalf("expected one fix, got %v", decoded["fixes"])
	}
	fix, _ := fixes[0].(map[string]any)
	if fix["kind"] != "column" || fix["from"] != "emial" || fix["to"] != "email" {
		t.Errorf("unexpected fix: %v", fix)
	}

	meta, _ := decoded["_meta"].(map[string]any)
	next, _ := meta["next"].([]any)
	if len(next) != 1 {
		t.Fatalf("expected a follow-up call, got %v", meta["next"])
	}
	call, _ := next[0].(map[string]any)
	args, _ := call["args"].(map[string]any)
	if call["tool"] != "advise" || args["sql"] != decoded["corrected_sql"] {
		t.Errorf("follow-up should run advise on the corrected query, got %v", call)
	}
}

// Named parameters are the boringSQL/queries dialect: they must parse, and a
// correction must come back in the caller's dialect, not as $1.
func TestValidateQuery_AcceptsNamedParams(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "validate_query", map[string]any{
		"sql": "SELECT u.emial FROM users u WHERE u.user_id = :user_id",
	})

	var decoded map[string]any
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("expected JSON output: %v\n%s", err, out)
	}
	if decoded["valid"] != false {
		t.Fatalf("expected the query to be invalid: %s", out)
	}
	if got := decoded["corrected_sql"]; got != "SELECT u.email FROM users u WHERE u.user_id = :user_id" {
		t.Fatalf("corrected_sql = %v\n%s", got, out)
	}
}

// Nothing is offered unless every error has one candidate; a half-corrected
// query still fails and an agent would run it anyway.
func TestValidateQuery_NoCorrectionWhenNotMechanical(t *testing.T) {
	c := setupOfflineTest(t)
	for _, sql := range []string{
		"SELECT * FROM warehouses",                  // nothing close
		"SELECT u.emial FROM users u, warehouses w", // one fixable, one not
		"SELECT * FROM users",                       // already valid
	} {
		out := callTool(t, c, "validate_query", map[string]any{"sql": sql})
		var decoded map[string]any
		if err := json.Unmarshal([]byte(out), &decoded); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if _, ok := decoded["corrected_sql"]; ok {
			t.Errorf("%s: unexpected corrected_sql %v", sql, decoded["corrected_sql"])
		}
	}
}

// advise validates too, and its hint tells the agent to fix names first; the
// correction has to be there for that to be actionable.
func TestAdvise_ReturnsCorrectedSQL(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "advise", map[string]any{
		"sql": "SELECT u.emial FROM users u",
	})
	var decoded map[string]any
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("expected JSON output: %v\n%s", err, out)
	}
	if got := decoded["corrected_sql"]; got != "SELECT u.email FROM users u" {
		t.Fatalf("corrected_sql = %v\n%s", got, out)
	}
}

// The stuck case is the one worth a pointer: names dryrun cannot guess.
func TestValidateQuery_HintsWhenNothingIsGuessable(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "validate_query", map[string]any{
		"sql": "SELECT * FROM warehouses",
	})
	var decoded map[string]any
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatal(err)
	}
	meta, _ := decoded["_meta"].(map[string]any)
	hint, _ := meta["hint"].(string)
	if !strings.Contains(hint, "find_objects") {
		t.Fatalf("expected the hint to point at find_objects, got %q", hint)
	}
}

// The point of safer_sql: the agent gets statements to put in a migration,
// not a paragraph telling it how to write them.
func TestCheckMigration_ReturnsSaferSQL(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "check_migration", map[string]any{
		"ddl": "ALTER TABLE tasks ADD CONSTRAINT tasks_project_fk FOREIGN KEY (project_id) REFERENCES projects(project_id)",
	})

	var decoded map[string]any
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("expected JSON output: %v\n%s", err, out)
	}
	checks, _ := decoded["checks"].([]any)
	if len(checks) != 1 {
		t.Fatalf("expected one check, got %v", decoded["checks"])
	}
	check, _ := checks[0].(map[string]any)
	if check["safety"] != "dangerous" {
		t.Fatalf("expected dangerous, got %v", check["safety"])
	}

	safer, _ := check["safer_sql"].([]any)
	if len(safer) != 2 {
		t.Fatalf("expected the two-step rewrite, got %v", check["safer_sql"])
	}
	if !strings.Contains(safer[0].(string), "NOT VALID") {
		t.Errorf("first statement should add the constraint NOT VALID: %v", safer[0])
	}
	if !strings.Contains(safer[1].(string), "VALIDATE CONSTRAINT tasks_project_fk") {
		t.Errorf("second statement should validate it by name: %v", safer[1])
	}

	meta, _ := decoded["_meta"].(map[string]any)
	hint, _ := meta["hint"].(string)
	if !strings.Contains(hint, "safer_sql") {
		t.Errorf("hint should point at safer_sql, got %q", hint)
	}
	if strings.Contains(hint, "CONCURRENTLY") {
		t.Errorf("transaction caveat fired without an index rewrite: %q", hint)
	}
}

// The hint is where the statement-boundary contract lives, and a rewrite split
// into steps is worthless if the runner wraps it in one transaction.
func TestCheckMigration_Hints(t *testing.T) {
	tests := []struct {
		name    string
		ddl     string
		wants   []string
		unwants []string
	}{
		{
			name:    "multi-step rewrite",
			ddl:     "ALTER TABLE tasks ADD CONSTRAINT tasks_project_fk FOREIGN KEY (project_id) REFERENCES projects(project_id)",
			wants:   []string{"safer_sql holds the rewrite", "its own transaction"},
			unwants: []string{"CONCURRENTLY"},
		},
		{
			name:    "index rewrite",
			ddl:     "CREATE INDEX idx_tasks_status ON tasks (status)",
			wants:   []string{"safer_sql holds the rewrite", "cannot run inside a transaction"},
			unwants: []string{"its own transaction"},
		},
		{
			name:    "some rewritten, some not",
			ddl:     "ALTER TABLE tasks ADD CHECK (priority > 0); ALTER TABLE tasks ALTER COLUMN title TYPE varchar(200)",
			wants:   []string{"The rest carry a recommendation only"},
			unwants: []string{},
		},
		{
			name:    "nothing rewritable",
			ddl:     "ALTER TABLE tasks ALTER COLUMN title TYPE varchar(200)",
			wants:   []string{"No mechanical rewrite"},
			unwants: []string{"safer_sql holds"},
		},
		{
			name:    "nothing unsafe",
			ddl:     "ALTER TABLE tasks ADD COLUMN note text",
			wants:   []string{},
			unwants: []string{"safer_sql", "rewrite"},
		},
	}

	c := setupOfflineTest(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := callTool(t, c, "check_migration", map[string]any{"ddl": tc.ddl})
			var decoded map[string]any
			if err := json.Unmarshal([]byte(out), &decoded); err != nil {
				t.Fatal(err)
			}
			meta, _ := decoded["_meta"].(map[string]any)
			hint, _ := meta["hint"].(string)
			for _, w := range tc.wants {
				if !strings.Contains(hint, w) {
					t.Errorf("hint missing %q: %q", w, hint)
				}
			}
			for _, w := range tc.unwants {
				if strings.Contains(hint, w) {
					t.Errorf("hint should not mention %q: %q", w, hint)
				}
			}
		})
	}
}

// A plain CREATE INDEX on a table the same migration creates is safe: the
// blocking build touches zero rows, and the wrapping transaction is what keeps
// it consistent. The handler must not fire the CONCURRENTLY/NO TRANSACTION
// lecture at it -- the hint used to key off Operation == "CREATE INDEX" for
// every plain index, including safe ones, telling authors to drop the very
// transaction that makes them safe.
func TestCheckMigration_PlainIndexOnNewTableNoConcurrencyHint(t *testing.T) {
	c := setupOfflineTest(t)
	ddl := "-- +goose Up\nCREATE TABLE brand_new_table (id bigint);\nCREATE INDEX idx_brand_new_table_id ON brand_new_table (id);\n"
	decoded := decodeCheckMigration(t, callTool(t, c, "check_migration", map[string]any{"ddl": ddl}))

	checks, _ := decoded["checks"].([]any)
	if len(checks) != 2 {
		t.Fatalf("expected two checks (CREATE TABLE, CREATE INDEX), got %v", decoded["checks"])
	}
	idxCheck, _ := checks[1].(map[string]any)
	if idxCheck["safety"] != "safe" {
		t.Fatalf("index on a same-file table should be safe, got %v", idxCheck["safety"])
	}
	meta, _ := decoded["_meta"].(map[string]any)
	hint, _ := meta["hint"].(string)
	if strings.Contains(hint, "NO TRANSACTION") || strings.Contains(hint, "CONCURRENTLY") {
		t.Errorf("plain index on a same-file table must not get the concurrency hint, got %q", hint)
	}
}

// migration_sql is present in the wrapper, parses, and holds both the rewrite
// and the safe passthrough -- when every unsafe statement has a rewrite.
func TestCheckMigration_MigrationSQLPresentWhenAllRewritable(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "check_migration", map[string]any{
		"ddl": "ALTER TABLE tasks VALIDATE CONSTRAINT tasks_priority_check, ADD CHECK (priority > 0)",
	})

	var decoded map[string]any
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("expected JSON output: %v\n%s", err, out)
	}
	migrationSQL, ok := decoded["migration_sql"].(string)
	if !ok || migrationSQL == "" {
		t.Fatalf("expected migration_sql in the wrapper, got: %s", out)
	}
	if !strings.Contains(migrationSQL, "VALIDATE CONSTRAINT tasks_priority_check") {
		t.Errorf("migration_sql lost the safe passthrough:\n%s", migrationSQL)
	}
	if !strings.Contains(migrationSQL, "NOT VALID") {
		t.Errorf("migration_sql lost the unsafe statement's rewrite:\n%s", migrationSQL)
	}
	if _, err := pg_query.Parse(migrationSQL); err != nil {
		t.Fatalf("migration_sql does not parse: %v\n%s", err, migrationSQL)
	}

	meta, _ := decoded["_meta"].(map[string]any)
	hint, _ := meta["hint"].(string)
	if !strings.Contains(hint, "migration_sql") {
		t.Errorf("hint should point at migration_sql, got %q", hint)
	}
}

// The gate: an unsafe statement with no mechanical rewrite suppresses
// migration_sql for the whole input, not just that statement.
func TestCheckMigration_MigrationSQLAbsentWhenAnyUnsafeHasNoRewrite(t *testing.T) {
	c := setupOfflineTest(t)
	for _, tc := range []struct {
		name string
		ddl  string
	}{
		{"mixed with an unrewritable statement", "ALTER TABLE tasks ADD CHECK (priority > 0); ALTER TABLE tasks ALTER COLUMN title TYPE varchar(200)"},
		{"nothing rewritable", "ALTER TABLE tasks ALTER COLUMN title TYPE varchar(200)"},
		{"nothing unsafe", "ALTER TABLE tasks ADD COLUMN note text"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := callTool(t, c, "check_migration", map[string]any{"ddl": tc.ddl})
			var decoded map[string]any
			if err := json.Unmarshal([]byte(out), &decoded); err != nil {
				t.Fatal(err)
			}
			if v, ok := decoded["migration_sql"]; ok {
				t.Errorf("expected no migration_sql, got %v", v)
			}
		})
	}
}

// The handler must feed the annotated schema (planner sizing) into
// check_migration: a known table carries table_size, and a small one scales
// the verdict down to caution without ever going safe.
func TestCheckMigration_SurfacesSizingAndScalesSmallTable(t *testing.T) {
	snap := loadDemoSchema(t)

	firstCheck := func(out string) map[string]any {
		t.Helper()
		var decoded map[string]any
		if err := json.Unmarshal([]byte(out), &decoded); err != nil {
			t.Fatalf("expected JSON output: %v\n%s", err, out)
		}
		checks, _ := decoded["checks"].([]any)
		if len(checks) != 1 {
			t.Fatalf("expected one check, got %v", decoded["checks"])
		}
		check, _ := checks[0].(map[string]any)
		return check
	}

	ddl := "CREATE INDEX idx_tasks_status ON tasks (status)"

	large := serveOffline(t, NewOfflineServerAnnotated(annotate(snap, 2_000_000), lint.DefaultConfig()))
	check := firstCheck(callTool(t, large, "check_migration", map[string]any{"ddl": ddl}))
	if size, _ := check["table_size"].(string); size == "" {
		t.Errorf("expected table_size from planner sizing, got %v", check["table_size"])
	}
	if rows, _ := check["row_estimate"].(float64); rows != 2_000_000 {
		t.Errorf("expected row_estimate 2000000, got %v", check["row_estimate"])
	}
	if check["safety"] != "dangerous" {
		t.Errorf("large table should stay dangerous, got %v", check["safety"])
	}

	small := serveOffline(t, NewOfflineServerAnnotated(annotate(snap, 2_000), lint.DefaultConfig()))
	check = firstCheck(callTool(t, small, "check_migration", map[string]any{"ddl": ddl}))
	if check["safety"] != "caution" {
		t.Errorf("small table should be caution, got %v", check["safety"])
	}
	rationale, _ := check["rationale"].(map[string]any)
	if note, _ := rationale["note"].(string); !strings.Contains(note, "Table is small") {
		t.Errorf("expected small-table note, got %q", note)
	}
}

// A table created in the same file is recognized as empty: CREATE TABLE itself
// is safe, and follow-up index/constraint statements on it are safe without
// being flagged dangerous or needing rewrites.
func TestCheckMigration_TableCreatedInSameFile(t *testing.T) {
	c := setupOfflineTest(t)
	ddl := `
CREATE TABLE accounts_profile (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id bigint NOT NULL,
    bio text
);
CREATE INDEX idx_accounts_profile_user_id ON accounts_profile (user_id);
ALTER TABLE accounts_profile ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id);
`
	out := callTool(t, c, "check_migration", map[string]any{"ddl": ddl})
	var decoded map[string]any
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("expected JSON: %v\n%s", err, out)
	}
	checks, ok := decoded["checks"].([]any)
	if !ok || len(checks) != 3 {
		t.Fatalf("expected 3 checks, got %v", decoded["checks"])
	}
	for i, raw := range checks {
		check := raw.(map[string]any)
		if check["safety"] != "safe" {
			t.Errorf("check[%d] (%s on %v): expected safe, got %v",
				i, check["operation"], check["table"], check["safety"])
		}
		if _, hasSafer := check["safer_sql"]; hasSafer {
			t.Errorf("check[%d]: empty table operations must not offer safer_sql rewrites", i)
		}
	}
}

func decodeCheckMigration(t *testing.T, out string) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("expected JSON output: %v\n%s", err, out)
	}
	return decoded
}

// A pasted migration file is split by its framework markers; the default up
// direction must not sweep in the down statements, and direction=down must not
// re-analyze the up half.
func TestCheckMigration_GooseFileDirection(t *testing.T) {
	c := setupOfflineTest(t)
	ddl := "-- +goose Up\nCREATE INDEX idx_tasks_status ON tasks (status);\n-- +goose Down\nDROP TABLE tasks;\n"

	up := decodeCheckMigration(t, callTool(t, c, "check_migration", map[string]any{"ddl": ddl}))
	if up["framework"] != "goose" || up["direction"] != "up" {
		t.Errorf("expected framework=goose direction=up, got %v/%v", up["framework"], up["direction"])
	}
	upChecks, _ := up["checks"].([]any)
	if len(upChecks) != 1 {
		t.Fatalf("up should yield one check, got %v", up["checks"])
	}
	if op, _ := upChecks[0].(map[string]any)["operation"].(string); !strings.Contains(op, "CREATE INDEX") {
		t.Errorf("up check should be the index, got %q", op)
	}

	down := decodeCheckMigration(t, callTool(t, c, "check_migration", map[string]any{"ddl": ddl, "direction": "down"}))
	downChecks, _ := down["checks"].([]any)
	if len(downChecks) != 1 {
		t.Fatalf("down should yield one check, got %v", down["checks"])
	}
	if op, _ := downChecks[0].(map[string]any)["operation"].(string); op != "DROP TABLE" {
		t.Errorf("down check should be the DROP, got %q", op)
	}
}

func TestCheckMigration_DownOnPlainErrors(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "check_migration", map[string]any{
		"ddl":       "DROP TABLE tasks;",
		"direction": "down",
	})
	if !strings.Contains(out, "no down section") {
		t.Errorf("expected a no-down-section error, got %q", out)
	}
}

// direction is an enum at the protocol layer, so a bad value never reaches the
// handler.
func TestCheckMigration_InvalidDirectionRejectedBySchema(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "check_migration", map[string]any{
		"ddl":       "ALTER TABLE tasks ADD COLUMN x int;",
		"direction": "sideways",
	})
	if !strings.Contains(out, "direction") {
		t.Errorf("expected the direction enum to reject the value, got %q", out)
	}
}

// A goose rewrite that introduces CONCURRENTLY must come back as a goose file
// that can actually run it, with the down half preserved.
func TestCheckMigration_GooseMigrationSQL(t *testing.T) {
	c := setupOfflineTest(t)
	ddl := "-- +goose Up\nCREATE INDEX idx_tasks_status ON tasks (status);\n-- +goose Down\nDROP INDEX idx_tasks_status;\n"
	decoded := decodeCheckMigration(t, callTool(t, c, "check_migration", map[string]any{"ddl": ddl}))

	migrationSQL, _ := decoded["migration_sql"].(string)
	for _, want := range []string{"-- +goose NO TRANSACTION", "-- +goose Up", "CONCURRENTLY", "-- +goose Down", "DROP INDEX idx_tasks_status;"} {
		if !strings.Contains(migrationSQL, want) {
			t.Errorf("migration_sql missing %q:\n%s", want, migrationSQL)
		}
	}
	if _, err := pg_query.Parse(migrationSQL); err != nil {
		t.Errorf("migration_sql does not parse: %v\n%s", err, migrationSQL)
	}

	meta, _ := decoded["_meta"].(map[string]any)
	hint, _ := meta["hint"].(string)
	if !strings.Contains(hint, "down section") {
		t.Errorf("hint should point at the down section, got %q", hint)
	}
}

// A goose file already declaring NO TRANSACTION needs no transaction lecture.
func TestCheckMigration_GooseNoTransactionHint(t *testing.T) {
	c := setupOfflineTest(t)
	ddl := "-- +goose NO TRANSACTION\n-- +goose Up\nCREATE INDEX idx_tasks_status ON tasks (status);\n"
	decoded := decodeCheckMigration(t, callTool(t, c, "check_migration", map[string]any{"ddl": ddl}))

	meta, _ := decoded["_meta"].(map[string]any)
	hint, _ := meta["hint"].(string)
	if !strings.Contains(hint, "already configured") {
		t.Errorf("hint should acknowledge the configured no-transaction mode, got %q", hint)
	}
	if strings.Contains(hint, "cannot run inside a transaction") {
		t.Errorf("hint should not warn about transactions the file opted out of, got %q", hint)
	}
}

func TestCheckMigration_DbmateMigrationSQL(t *testing.T) {
	c := setupOfflineTest(t)
	ddl := "-- migrate:up\nCREATE INDEX idx_tasks_status ON tasks (status);\n-- migrate:down\nDROP INDEX idx_tasks_status;\n"
	decoded := decodeCheckMigration(t, callTool(t, c, "check_migration", map[string]any{"ddl": ddl}))

	if decoded["framework"] != "dbmate" {
		t.Errorf("expected framework=dbmate, got %v", decoded["framework"])
	}
	migrationSQL, _ := decoded["migration_sql"].(string)
	for _, want := range []string{"-- migrate:up transaction:false", "CONCURRENTLY", "-- migrate:down", "DROP INDEX idx_tasks_status;"} {
		if !strings.Contains(migrationSQL, want) {
			t.Errorf("migration_sql missing %q:\n%s", want, migrationSQL)
		}
	}
}

// tern wraps every migration in a transaction with no opt-out, so a CONCURRENTLY
// rewrite must not be shipped as a runnable tern file.
func TestCheckMigration_TernSuppressesMigrationSQL(t *testing.T) {
	c := setupOfflineTest(t)
	ddl := "CREATE INDEX idx_tasks_status ON tasks (status);\n---- create above / drop below ----\nDROP INDEX idx_tasks_status;\n"
	decoded := decodeCheckMigration(t, callTool(t, c, "check_migration", map[string]any{"ddl": ddl}))

	if decoded["framework"] != "tern" {
		t.Errorf("expected framework=tern, got %v", decoded["framework"])
	}
	if _, ok := decoded["migration_sql"]; ok {
		t.Errorf("tern + CONCURRENTLY should not ship migration_sql, got %v", decoded["migration_sql"])
	}
	meta, _ := decoded["_meta"].(map[string]any)
	hint, _ := meta["hint"].(string)
	if !strings.Contains(hint, "tern") {
		t.Errorf("hint should name the tern limitation, got %q", hint)
	}
}

// When a statement already uses CONCURRENTLY directly (e.g. DROP INDEX CONCURRENTLY
// which is Safe standalone), a goose file without NO TRANSACTION wraps it in the
// transaction it cannot run in: the check is dangerous and migration_sql adds the
// marker.
func TestCheckMigration_DirectConcurrentStatementHint(t *testing.T) {
	c := setupOfflineTest(t)
	ddl := "-- +goose Up\nDROP INDEX CONCURRENTLY idx_tasks_status;\n"
	decoded := decodeCheckMigration(t, callTool(t, c, "check_migration", map[string]any{"ddl": ddl}))

	checks, _ := decoded["checks"].([]any)
	if len(checks) != 1 {
		t.Fatalf("expected one check, got %v", decoded["checks"])
	}
	check := checks[0].(map[string]any)
	if check["safety"] != "dangerous" {
		t.Errorf("CONCURRENTLY in a transactional goose file must be dangerous, got %v", check["safety"])
	}

	migrationSQL, _ := decoded["migration_sql"].(string)
	for _, want := range []string{"-- +goose NO TRANSACTION", "DROP INDEX CONCURRENTLY idx_tasks_status;"} {
		if !strings.Contains(migrationSQL, want) {
			t.Errorf("migration_sql missing %q:\n%s", want, migrationSQL)
		}
	}
	if _, err := pg_query.Parse(migrationSQL); err != nil {
		t.Errorf("emitted goose file does not parse: %v\n%s", err, migrationSQL)
	}

	meta, _ := decoded["_meta"].(map[string]any)
	hint, _ := meta["hint"].(string)
	if !strings.Contains(hint, "-- +goose NO TRANSACTION") {
		t.Errorf("hint should mention the marker, got %q", hint)
	}
	if !strings.Contains(hint, "migration_sql adds") {
		t.Errorf("hint should say migration_sql already adds the marker, got %q", hint)
	}
}

// A goose file already carrying NO TRANSACTION runs the statement fine: the
// check stays safe and there is nothing for migration_sql to emit.
func TestCheckMigration_DirectConcurrentNoTransactionStaysSafe(t *testing.T) {
	c := setupOfflineTest(t)
	ddl := "-- +goose NO TRANSACTION\n-- +goose Up\nDROP INDEX CONCURRENTLY idx_tasks_status;\n"
	decoded := decodeCheckMigration(t, callTool(t, c, "check_migration", map[string]any{"ddl": ddl}))

	checks, _ := decoded["checks"].([]any)
	if len(checks) != 1 {
		t.Fatalf("expected one check, got %v", decoded["checks"])
	}
	if check := checks[0].(map[string]any); check["safety"] != "safe" {
		t.Errorf("NO TRANSACTION file should keep the direct CONCURRENTLY check safe, got %v", check["safety"])
	}
	if _, ok := decoded["migration_sql"]; ok {
		t.Errorf("nothing to rewrite, so migration_sql must be absent, got %v", decoded["migration_sql"])
	}
}

// dbmate without transaction:false is transactional too: a direct CONCURRENTLY
// statement is dangerous and the emitted file carries the marker.
func TestCheckMigration_DbmateDirectConcurrentStatement(t *testing.T) {
	c := setupOfflineTest(t)
	ddl := "-- migrate:up\nDROP INDEX CONCURRENTLY idx_tasks_status;\n"
	decoded := decodeCheckMigration(t, callTool(t, c, "check_migration", map[string]any{"ddl": ddl}))

	checks, _ := decoded["checks"].([]any)
	if len(checks) != 1 {
		t.Fatalf("expected one check, got %v", decoded["checks"])
	}
	if check := checks[0].(map[string]any); check["safety"] != "dangerous" {
		t.Errorf("CONCURRENTLY in a transactional dbmate file must be dangerous, got %v", check["safety"])
	}

	migrationSQL, _ := decoded["migration_sql"].(string)
	if !strings.Contains(migrationSQL, "-- migrate:up transaction:false") {
		t.Errorf("migration_sql should add transaction:false, got:\n%s", migrationSQL)
	}
}

// tern has no opt-out: a direct CONCURRENTLY statement is dangerous and no
// migration_sql can be shipped for it.
func TestCheckMigration_TernDirectConcurrentStatement(t *testing.T) {
	c := setupOfflineTest(t)
	ddl := "DROP INDEX CONCURRENTLY idx_tasks_status;\n---- create above / drop below ----\nCREATE INDEX idx_tasks_status ON tasks (status);\n"
	decoded := decodeCheckMigration(t, callTool(t, c, "check_migration", map[string]any{"ddl": ddl}))

	if decoded["framework"] != "tern" {
		t.Errorf("expected framework=tern, got %v", decoded["framework"])
	}
	checks, _ := decoded["checks"].([]any)
	if len(checks) != 1 {
		t.Fatalf("expected one check, got %v", decoded["checks"])
	}
	check := checks[0].(map[string]any)
	if check["safety"] != "dangerous" {
		t.Errorf("CONCURRENTLY in tern must be dangerous, got %v", check["safety"])
	}
	if _, hasSafer := check["safer_sql"]; hasSafer {
		t.Errorf("tern has no in-file rewrite, so safer_sql must be absent, got %v", check["safer_sql"])
	}
	if _, ok := decoded["migration_sql"]; ok {
		t.Errorf("tern + CONCURRENTLY should not ship migration_sql, got %v", decoded["migration_sql"])
	}
	meta, _ := decoded["_meta"].(map[string]any)
	hint, _ := meta["hint"].(string)
	if !strings.Contains(hint, "tern") || !strings.Contains(hint, "out-of-band") {
		t.Errorf("hint should name the tern limitation and the out-of-band remedy, got %q", hint)
	}
}

// When direction=down on dbmate, the concurrency hint must name -- migrate:down,
// not -- migrate:up.
func TestCheckMigration_DbmateDownDirectionHint(t *testing.T) {
	c := setupOfflineTest(t)
	ddl := "-- migrate:up\nCREATE TABLE foo (id int);\n-- migrate:down\nDROP INDEX CONCURRENTLY idx_tasks_status;\n"
	decoded := decodeCheckMigration(t, callTool(t, c, "check_migration", map[string]any{
		"ddl":       ddl,
		"direction": "down",
	}))

	meta, _ := decoded["_meta"].(map[string]any)
	hint, _ := meta["hint"].(string)
	if !strings.Contains(hint, "-- migrate:down") {
		t.Errorf("hint should name -- migrate:down for direction=down, got %q", hint)
	}
	if strings.Contains(hint, "-- migrate:up") {
		t.Errorf("hint should NOT name -- migrate:up for direction=down, got %q", hint)
	}
}
