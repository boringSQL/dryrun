package mcp

import (
	"encoding/json"
	"strings"
	"testing"
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
	if !strings.Contains(hint, "search_schema") {
		t.Fatalf("expected the hint to point at search_schema, got %q", hint)
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
