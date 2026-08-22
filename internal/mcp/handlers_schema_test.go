package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
)

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
		assertContains(t, out, `"table": "public.organizations"`)
		assertContains(t, out, `"join": "JOIN public.organizations ON public.organizations.organization_id = public.users.organization_id"`)
	})
}

// fields whitelist drops all sections except the listed ones (plus identity
// keys schema/name and the always-injected _meta).
func TestDescribeTable_FieldsWhitelist(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "describe_table", map[string]any{
		"table":  "users",
		"fields": []any{"indexes"},
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}

	if _, ok := payload["indexes"]; !ok {
		t.Errorf("expected 'indexes' key, got: %v", keys(payload))
	}
	if _, ok := payload["_meta"]; !ok {
		t.Errorf("expected '_meta' key, got: %v", keys(payload))
	}
	for _, banned := range []string{"columns", "constraints", "stats", "policies"} {
		if _, ok := payload[banned]; ok {
			t.Errorf("expected %q to be filtered out, got: %v", banned, keys(payload))
		}
	}
}

func TestDescribeTable_UnknownFieldErrors(t *testing.T) {
	c := setupOfflineTest(t)
	var req mcp.CallToolRequest
	req.Params.Name = "describe_table"
	req.Params.Arguments = map[string]any{
		"table":  "users",
		"fields": []any{"bogus"},
	}
	result, err := c.CallTool(context.Background(), req)
	if err != nil {
		t.Fatalf("transport error: %v", err)
	}
	if !result.IsError {
		t.Fatalf("expected IsError, got success: %+v", result)
	}
	text := result.Content[0].(mcp.TextContent).Text
	if !strings.Contains(text, "unknown field 'bogus'") {
		t.Errorf("expected error text to mention unknown field, got: %s", text)
	}
}

// Default (no fields) preserves the flattened shape — columns and indexes
// both present at the top level.
func TestDescribeTable_DefaultShape(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "describe_table", map[string]any{"table": "users"})

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	for _, want := range []string{"schema", "name", "columns", "indexes", "_meta"} {
		if _, ok := payload[want]; !ok {
			t.Errorf("expected top-level %q in default shape, got: %v", want, keys(payload))
		}
	}
}

// Tables with FKs surface a _meta.next pointing at find_related so chaining
// clients can follow up without parsing the prose hint. Tables without FKs
// must not surface _meta.next.
func TestDescribeTable_FKSuggestsFindRelated(t *testing.T) {
	c := setupOfflineTest(t)

	t.Run("with_fk", func(t *testing.T) {
		out := callTool(t, c, "describe_table", map[string]any{"table": "users"})
		var payload map[string]any
		if err := json.Unmarshal([]byte(out), &payload); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, out)
		}
		meta, _ := payload["_meta"].(map[string]any)
		next, ok := meta["next"].([]any)
		if !ok || len(next) == 0 {
			t.Fatalf("expected _meta.next with one entry, got: %v", meta)
		}
		first, _ := next[0].(map[string]any)
		if first["tool"] != "find_related" {
			t.Errorf("expected tool=find_related, got %v", first["tool"])
		}
		args, _ := first["args"].(map[string]any)
		if args["table"] != "users" || args["schema"] != "public" {
			t.Errorf("expected args{table:users, schema:public}, got %v", args)
		}
	})

	t.Run("without_fk", func(t *testing.T) {
		out := callTool(t, c, "describe_table", map[string]any{"table": "organizations"})
		var payload map[string]any
		if err := json.Unmarshal([]byte(out), &payload); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, out)
		}
		meta, _ := payload["_meta"].(map[string]any)
		if _, present := meta["next"]; present {
			t.Errorf("expected no _meta.next for table without FKs, got: %v", meta)
		}
	})
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
