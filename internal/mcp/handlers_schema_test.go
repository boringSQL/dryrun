package mcp

import "testing"

// Smoke tests for the schema-family tools (list_tables, describe_table,
// search_schema, find_related). Each subtest exercises one tool against the
// offline demo snapshot and asserts the expected substrings appear in the
// rendered text/JSON output.
func TestSchemaHandlers_OfflineSmoke(t *testing.T) {
	c := setupOfflineTest(t)

	t.Run("list_tables", func(t *testing.T) {
		out := callTool(t, c, "list_tables", nil)
		assertContains(t, out, "PostgreSQL 18.3.0")
		assertContains(t, out, "users")
		assertContains(t, out, "tasks")
	})

	t.Run("describe_table", func(t *testing.T) {
		out := callTool(t, c, "describe_table", map[string]any{"table": "users"})
		assertContains(t, out, "pg_version")
		assertContains(t, out, "email")
		assertContains(t, out, "user_id")
	})

	t.Run("search_schema", func(t *testing.T) {
		out := callTool(t, c, "search_schema", map[string]any{"query": "email"})
		assertContains(t, out, "email")
	})

	t.Run("find_related", func(t *testing.T) {
		out := callTool(t, c, "find_related", map[string]any{"table": "users"})
		if out == "" {
			t.Fatal("empty result")
		}
	})
}
