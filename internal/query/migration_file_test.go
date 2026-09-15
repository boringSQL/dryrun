package query

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

const (
	gooseUp   = "CREATE INDEX users_email_idx ON users (email);"
	gooseDown = "DROP INDEX users_email_idx;"
)

func TestParseMigrationEnvelopePlain(t *testing.T) {
	content := "ALTER TABLE users ADD COLUMN age integer;\nALTER TABLE users DROP COLUMN legacy;"
	env := ParseMigrationEnvelope(content)

	if env.Framework != FrameworkPlain {
		t.Errorf("framework = %q, want plain", env.Framework)
	}
	if env.Up.DDL != content {
		t.Errorf("plain up DDL should be the whole input, got %q", env.Up.DDL)
	}
	if env.Down != nil {
		t.Error("plain SQL has no down section")
	}
	if env.Up.NoTransaction {
		t.Error("plain SQL is not a transaction-mode file")
	}
}

func TestParseMigrationEnvelopeGoose(t *testing.T) {
	content := strings.Join([]string{
		"-- +goose Up",
		"CREATE INDEX users_email_idx ON users (email);",
		"-- +goose Down",
		"DROP INDEX users_email_idx;",
		"",
	}, "\n")
	env := ParseMigrationEnvelope(content)

	if env.Framework != FrameworkGoose {
		t.Fatalf("framework = %q, want goose", env.Framework)
	}
	if got := strings.TrimSpace(env.Up.DDL); got != gooseUp {
		t.Errorf("up DDL = %q, want %q", got, gooseUp)
	}
	if env.Down == nil {
		t.Fatal("expected a down section")
	}
	if got := strings.TrimSpace(env.Down.DDL); got != gooseDown {
		t.Errorf("down DDL = %q, want %q", got, gooseDown)
	}
}

// NO TRANSACTION at the top of the file is the documented place and applies to
// both halves; a section-level directive only to its own half.
func TestParseMigrationEnvelopeGooseNoTransaction(t *testing.T) {
	global := ParseMigrationEnvelope("-- +goose NO TRANSACTION\n-- +goose Up\nSELECT 1;\n-- +goose Down\nSELECT 2;\n")
	if !global.Up.NoTransaction || global.Down == nil || !global.Down.NoTransaction {
		t.Errorf("file-level NO TRANSACTION should cover both halves: up=%v down=%v",
			global.Up.NoTransaction, global.Down != nil && global.Down.NoTransaction)
	}

	section := ParseMigrationEnvelope("-- +goose Up\n-- +goose NO TRANSACTION\nSELECT 1;\n-- +goose Down\nSELECT 2;\n")
	if !section.Up.NoTransaction || section.Down.NoTransaction {
		t.Errorf("section-level NO TRANSACTION should cover up only: up=%v down=%v",
			section.Up.NoTransaction, section.Down.NoTransaction)
	}
}

// StatementBegin/End are goose's splitter markers, not SQL; pg_query does its
// own splitting, so the markers are stripped and the body kept.
func TestParseMigrationEnvelopeGooseStatementMarkers(t *testing.T) {
	content := strings.Join([]string{
		"-- +goose Up",
		"-- +goose StatementBegin",
		"CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql;",
		"-- +goose StatementEnd",
		"-- +goose Down",
		"-- +goose StatementBegin",
		"DROP FUNCTION f();",
		"-- +goose StatementEnd",
		"",
	}, "\n")
	env := ParseMigrationEnvelope(content)

	if strings.Contains(env.Up.DDL, "StatementBegin") || strings.Contains(env.Up.DDL, "goose") {
		t.Errorf("up DDL still carries goose markers: %q", env.Up.DDL)
	}
	if !strings.Contains(env.Up.DDL, "SELECT 1; $$ LANGUAGE sql") {
		t.Errorf("up DDL lost the function body: %q", env.Up.DDL)
	}
	if strings.Contains(env.Down.DDL, "StatementEnd") {
		t.Errorf("down DDL still carries goose markers: %q", env.Down.DDL)
	}
}

// An empty up section must not fall back to plain and sweep in the down
// statements -- that would analyze a DROP as the forward migration.
func TestParseMigrationEnvelopeGooseEmptyUpDoesNotFallBack(t *testing.T) {
	content := "-- +goose Up\n-- +goose Down\nDROP TABLE users CASCADE;\n"
	env := ParseMigrationEnvelope(content)

	if env.Framework != FrameworkGoose {
		t.Fatalf("framework = %q, want goose", env.Framework)
	}
	if strings.TrimSpace(env.Up.DDL) != "" {
		t.Errorf("up should be empty, got %q", env.Up.DDL)
	}
	if env.Down == nil || strings.TrimSpace(env.Down.DDL) != "DROP TABLE users CASCADE;" {
		t.Fatalf("down = %+v, want the DROP", env.Down)
	}
}

func TestParseMigrationEnvelopeDbmate(t *testing.T) {
	content := strings.Join([]string{
		"-- migrate:up transaction:false",
		"CREATE INDEX users_email_idx ON users (email);",
		"-- migrate:down",
		"DROP INDEX users_email_idx;",
		"",
	}, "\n")
	env := ParseMigrationEnvelope(content)

	if env.Framework != FrameworkDbmate {
		t.Fatalf("framework = %q, want dbmate", env.Framework)
	}
	if got := strings.TrimSpace(env.Up.DDL); got != gooseUp {
		t.Errorf("up DDL = %q, want %q", got, gooseUp)
	}
	if !env.Up.NoTransaction {
		t.Error("transaction:false on the up marker should set NoTransaction")
	}
	if env.Down == nil || strings.TrimSpace(env.Down.DDL) != gooseDown {
		t.Fatalf("down = %+v, want the DROP INDEX", env.Down)
	}
	if env.Down.NoTransaction {
		t.Error("transaction:false on up must not leak into down")
	}
}

func TestParseMigrationEnvelopeTern(t *testing.T) {
	content := strings.Join([]string{
		"CREATE INDEX users_email_idx ON users (email);",
		"---- create above / drop below ----",
		"DROP INDEX users_email_idx;",
		"",
	}, "\n")
	env := ParseMigrationEnvelope(content)

	if env.Framework != FrameworkTern {
		t.Fatalf("framework = %q, want tern", env.Framework)
	}
	if got := strings.TrimSpace(env.Up.DDL); got != gooseUp {
		t.Errorf("up DDL = %q, want %q", got, gooseUp)
	}
	if env.Down == nil || strings.TrimSpace(env.Down.DDL) != gooseDown {
		t.Fatalf("down = %+v, want the DROP INDEX", env.Down)
	}
	if env.Up.NoTransaction || env.Down.NoTransaction {
		t.Error("tern has no transaction opt-out, so NoTransaction must stay false")
	}
}

func TestMigrationEnvelopeSection(t *testing.T) {
	plain := ParseMigrationEnvelope("SELECT 1;")
	if _, err := plain.Section(""); err != nil {
		t.Errorf("plain up should resolve: %v", err)
	}
	if _, err := plain.Section("down"); err == nil {
		t.Error("plain down should error")
	}
	if _, err := plain.Section("sideways"); err == nil {
		t.Error("invalid direction should error")
	}

	goose := ParseMigrationEnvelope("-- +goose Up\nSELECT 1;\n-- +goose Down\nSELECT 2;\n")
	up, err := goose.Section("UP")
	if err != nil || !strings.Contains(up.DDL, "SELECT 1") {
		t.Errorf("case-insensitive up lookup failed: %q, %v", up.DDL, err)
	}
	down, err := goose.Section("down")
	if err != nil || !strings.Contains(down.DDL, "SELECT 2") {
		t.Errorf("down lookup failed: %q, %v", down.DDL, err)
	}

	upOnly := ParseMigrationEnvelope("-- +goose Up\nSELECT 1;\n")
	if _, err := upOnly.Section("down"); err == nil {
		t.Error("goose file without a down marker should error on direction=down")
	}
}

func TestFormatMigrationFilePlain(t *testing.T) {
	env := ParseMigrationEnvelope("ALTER TABLE users ADD COLUMN age int;")
	got := FormatMigrationFile(env, "up", "-- header\nALTER TABLE users ADD COLUMN age bigint;")
	if got != "-- header\nALTER TABLE users ADD COLUMN age bigint;" {
		t.Errorf("plain rewrite should pass through, got %q", got)
	}
}

// A goose rewrite must stay a goose file: markers, the preserved down half, and
// NO TRANSACTION injected when the rewrite needs CONCURRENTLY.
func TestFormatMigrationFileGoose(t *testing.T) {
	env := ParseMigrationEnvelope("-- +goose Up\nCREATE INDEX users_email_idx ON users (email);\n-- +goose Down\nDROP INDEX users_email_idx;\n")
	got := FormatMigrationFile(env, "up", "-- header\nCREATE INDEX CONCURRENTLY users_email_idx ON users (email);\n")

	for _, want := range []string{"-- +goose NO TRANSACTION", "-- +goose Up", "CONCURRENTLY", "-- +goose Down", "DROP INDEX users_email_idx;"} {
		if !strings.Contains(got, want) {
			t.Errorf("goose rewrite missing %q:\n%s", want, got)
		}
	}
	if _, err := pg_query.Parse(got); err != nil {
		t.Errorf("goose rewrite does not parse: %v\n%s", err, got)
	}
}

func TestFormatMigrationFileDbmate(t *testing.T) {
	env := ParseMigrationEnvelope("-- migrate:up\nCREATE INDEX users_email_idx ON users (email);\n-- migrate:down\nDROP INDEX users_email_idx;\n")
	got := FormatMigrationFile(env, "up", "-- header\nCREATE INDEX CONCURRENTLY users_email_idx ON users (email);\n")

	for _, want := range []string{"-- migrate:up transaction:false", "CONCURRENTLY", "-- migrate:down", "DROP INDEX users_email_idx;"} {
		if !strings.Contains(got, want) {
			t.Errorf("dbmate rewrite missing %q:\n%s", want, got)
		}
	}
	if _, err := pg_query.Parse(got); err != nil {
		t.Errorf("dbmate rewrite does not parse: %v\n%s", err, got)
	}
}

// direction=down puts the rewrite under the down marker and leaves the up half
// untouched.
func TestFormatMigrationFileDownDirection(t *testing.T) {
	env := ParseMigrationEnvelope("-- +goose Up\nCREATE INDEX users_email_idx ON users (email);\n-- +goose Down\nDROP INDEX users_email_idx;\n")
	got := FormatMigrationFile(env, "down", "-- header\nDROP INDEX CONCURRENTLY users_email_idx;\n")

	upAt := strings.Index(got, "-- +goose Up")
	downAt := strings.Index(got, "-- +goose Down")
	concurrentAt := strings.Index(got, "CONCURRENTLY")
	if upAt < 0 || downAt < 0 || concurrentAt < 0 {
		t.Fatalf("rewrite lost a marker or the statement:\n%s", got)
	}
	if !(upAt < downAt && downAt < concurrentAt) {
		t.Errorf("down rewrite must sit after the down marker: up=%d down=%d stmt=%d\n%s", upAt, downAt, concurrentAt, got)
	}
	if strings.Contains(got[:downAt], "CONCURRENTLY") {
		t.Errorf("the up half should be untouched:\n%s", got)
	}
}

// tern wraps every migration in a transaction, so a CONCURRENTLY rewrite has no
// runnable form; the tool must not ship one.
func TestFormatMigrationFileTernSuppressesConcurrently(t *testing.T) {
	env := ParseMigrationEnvelope("CREATE INDEX users_email_idx ON users (email);\n---- create above / drop below ----\nDROP INDEX users_email_idx;\n")
	got := FormatMigrationFile(env, "up", "-- header\nCREATE INDEX CONCURRENTLY users_email_idx ON users (email);\n")
	if got != "" {
		t.Errorf("tern + CONCURRENTLY should suppress migration_sql, got:\n%s", got)
	}

	safe := FormatMigrationFile(env, "up", "-- header\nCREATE INDEX users_email_idx ON users (email);\n")
	if !strings.Contains(safe, ternSeparator) || !strings.Contains(safe, "DROP INDEX users_email_idx;") {
		t.Errorf("safe tern rewrite should keep the separator and down half:\n%s", safe)
	}
}

// A comment mentioning goose without a recognized directive must not hijack
// plain SQL.
func TestParseMigrationEnvelope_PlainWithGooseComment(t *testing.T) {
	content := "-- +goose was evaluated for this project but rejected\nALTER TABLE users ADD COLUMN age int;\n"
	env := ParseMigrationEnvelope(content)
	if env.Framework != FrameworkPlain {
		t.Errorf("expected plain framework for arbitrary +goose comment, got %q", env.Framework)
	}
}

// CONCURRENTLY matching must respect word boundaries: a table or column like
// concurrently_jobs must not trigger transaction warnings or tern suppression.
func TestFormatMigrationFile_ConcurrentlyWordBoundary(t *testing.T) {
	envTern := ParseMigrationEnvelope("ALTER TABLE concurrently_jobs ADD COLUMN age int;\n---- create above / drop below ----\nALTER TABLE concurrently_jobs DROP COLUMN age;\n")
	rewritten := "-- header\nALTER TABLE concurrently_jobs ADD COLUMN age bigint;\n"
	gotTern := FormatMigrationFile(envTern, "up", rewritten)
	if gotTern == "" {
		t.Error("tern should not suppress migration_sql for a table named concurrently_jobs")
	}

	envGoose := ParseMigrationEnvelope("-- +goose Up\nALTER TABLE concurrently_jobs ADD COLUMN age int;\n")
	gotGoose := FormatMigrationFile(envGoose, "up", rewritten)
	if strings.Contains(gotGoose, "NO TRANSACTION") {
		t.Error("goose should not inject NO TRANSACTION for a table named concurrently_jobs")
	}
}

// dbmate options separated by tabs or spaces must be recognized.
func TestParseMigrationEnvelopeDbmate_TabSeparated(t *testing.T) {
	content := "-- migrate:up\ttransaction:\tfalse\nCREATE INDEX idx ON users (email);\n"
	env := ParseMigrationEnvelope(content)
	if env.Framework != FrameworkDbmate {
		t.Fatalf("expected dbmate, got %q", env.Framework)
	}
	if !env.Up.NoTransaction {
		t.Error("tab-separated transaction:false should set NoTransaction")
	}
}

// Goose directives with optional colons (-- +goose: Up) must be recognized.
func TestParseMigrationEnvelopeGoose_ColonSyntax(t *testing.T) {
	content := "-- +goose: Up\nCREATE TABLE users (id int);\n-- +goose: Down\nDROP TABLE users;\n"
	env := ParseMigrationEnvelope(content)
	if env.Framework != FrameworkGoose {
		t.Fatalf("expected goose, got %q", env.Framework)
	}
	if env.Down == nil {
		t.Fatal("expected down section")
	}
}

// Tern separator case-insensitivity.
func TestParseMigrationEnvelopeTern_CaseInsensitive(t *testing.T) {
	content := "CREATE TABLE users (id int);\n---- CREATE ABOVE / DROP BELOW ----\nDROP TABLE users;\n"
	env := ParseMigrationEnvelope(content)
	if env.Framework != FrameworkTern {
		t.Fatalf("expected tern, got %q", env.Framework)
	}
	if env.Down == nil {
		t.Fatal("expected down section")
	}
}

func concurrentCheck(op, stmt string) MigrationCheck {
	return MigrationCheck{
		Operation: op, Safety: SafetySafe,
		Statement:      stmt,
		Rationale:      &Rationale{Reason: "safe"},
		Recommendation: "safe",
	}
}

// A CONCURRENTLY statement in a goose/dbmate section the runner wraps in a
// transaction fails at apply: the check must become dangerous, with the
// statement carried as SaferSQL so the marker gets injected downstream.
func TestMarkConcurrentInTransaction(t *testing.T) {
	const stmt = "DROP INDEX CONCURRENTLY idx_users;"

	tests := []struct {
		name        string
		content     string
		direction   string
		op          string
		wantSafe    bool
		wantRewrite bool
		wantSub     string
	}{
		{
			name:        "goose transactional",
			content:     "-- +goose Up\n" + stmt + "\n-- +goose Down\nSELECT 1;\n",
			direction:   "up",
			op:          "DROP INDEX CONCURRENTLY",
			wantRewrite: true,
			wantSub:     "goose",
		},
		{
			name:      "goose no transaction",
			content:   "-- +goose NO TRANSACTION\n-- +goose Up\n" + stmt + "\n",
			direction: "up",
			op:        "DROP INDEX CONCURRENTLY",
			wantSafe:  true,
		},
		{
			name:        "dbmate transactional",
			content:     "-- migrate:up\n" + stmt + "\n-- migrate:down\nSELECT 1;\n",
			direction:   "up",
			op:          "DROP INDEX CONCURRENTLY",
			wantRewrite: true,
			wantSub:     "dbmate",
		},
		{
			name:      "dbmate transaction false",
			content:   "-- migrate:up transaction:false\n" + stmt + "\n-- migrate:down\nSELECT 1;\n",
			direction: "up",
			op:        "DROP INDEX CONCURRENTLY",
			wantSafe:  true,
		},
		{
			name:      "tern no opt-out",
			content:   stmt + "\n---- create above / drop below ----\nSELECT 1;\n",
			direction: "up",
			op:        "DROP INDEX CONCURRENTLY",
			wantSub:   "no opt-out",
		},
		{
			name:      "plain runner unknown",
			content:   stmt + "\n",
			direction: "up",
			op:        "DROP INDEX CONCURRENTLY",
			wantSafe:  true,
		},
		{
			name:      "non-concurrent statement untouched",
			content:   "-- +goose Up\nCREATE INDEX idx ON users (email);\n",
			direction: "up",
			op:        "CREATE INDEX",
			wantSafe:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := ParseMigrationEnvelope(tc.content)
			sec, err := env.Section(tc.direction)
			if err != nil {
				t.Fatalf("section: %v", err)
			}
			checks := []MigrationCheck{concurrentCheck(tc.op, "DROP INDEX CONCURRENTLY idx_users;")}
			MarkConcurrentInTransaction(env, sec, checks)

			if tc.wantSafe {
				if checks[0].Safety != SafetySafe {
					t.Errorf("safety = %q, want safe", checks[0].Safety)
				}
				return
			}
			if checks[0].Safety != SafetyDangerous {
				t.Errorf("safety = %q, want dangerous", checks[0].Safety)
			}
			if tc.wantSub != "" && !strings.Contains(checks[0].Rationale.Reason+checks[0].Recommendation, tc.wantSub) {
				t.Errorf("reason/recommendation missing %q: %q / %q", tc.wantSub, checks[0].Rationale.Reason, checks[0].Recommendation)
			}
			switch {
			case tc.wantRewrite:
				if len(checks[0].SaferSQL) != 1 || checks[0].SaferSQL[0] != checks[0].Statement {
					t.Errorf("SaferSQL = %v, want the statement itself", checks[0].SaferSQL)
				}
			default:
				if len(checks[0].SaferSQL) != 0 {
					t.Errorf("SaferSQL = %v, want none (no in-file opt-out)", checks[0].SaferSQL)
				}
			}
		})
	}
}

// The other half of a goose file can carry the marker, so direction=down must
// read its own section rather than the up section's transaction mode.
func TestMarkConcurrentInTransaction_DownSection(t *testing.T) {
	const stmt = "DROP INDEX CONCURRENTLY idx_users;"
	content := "-- +goose NO TRANSACTION\n-- +goose Up\nSELECT 1;\n-- +goose Down\n" + stmt + "\n"
	env := ParseMigrationEnvelope(content)
	sec, err := env.Section("down")
	if err != nil {
		t.Fatalf("section: %v", err)
	}
	checks := []MigrationCheck{concurrentCheck("DROP INDEX CONCURRENTLY", stmt)}
	MarkConcurrentInTransaction(env, sec, checks)
	if checks[0].Safety != SafetySafe {
		t.Errorf("file-level NO TRANSACTION should cover the down half, got %q", checks[0].Safety)
	}
}
