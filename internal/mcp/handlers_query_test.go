package mcp

import (
	"encoding/json"
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
