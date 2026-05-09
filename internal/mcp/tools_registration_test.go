package mcp

import (
	"context"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
)

// Round-trips the registered tool list through the client and asserts each
// listed tool resolves to a handler. Guards against drift between tools.go
// (registration) and handlers_*.go (implementations) — if Register names a
// tool that no handler is bound to, CallTool surfaces "tool not found" and
// this test fails.
func TestToolsRegistration_EveryListedToolHasHandler(t *testing.T) {
	c := setupOfflineTest(t)

	list, err := c.ListTools(context.Background(), mcp.ListToolsRequest{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(list.Tools) == 0 {
		t.Fatal("expected at least one registered tool")
	}

	for _, tool := range list.Tools {
		t.Run(tool.Name, func(t *testing.T) {
			if tool.Description == "" {
				t.Errorf("tool %s has empty description", tool.Name)
			}

			var req mcp.CallToolRequest
			req.Params.Name = tool.Name
			// minimal valid args for tools that require them; everything else
			// is fine with nil since we only care about handler resolution.
			switch tool.Name {
			case "describe_table", "find_related", "compare_nodes":
				req.Params.Arguments = map[string]any{"table": "users"}
			case "search_schema":
				req.Params.Arguments = map[string]any{"query": "users"}
			case "validate_query":
				req.Params.Arguments = map[string]any{"sql": "SELECT 1"}
			case "check_migration":
				req.Params.Arguments = map[string]any{"ddl": "ALTER TABLE users ADD COLUMN x INT"}
			case "suggest_index":
				req.Params.Arguments = map[string]any{"sql": "SELECT * FROM users"}
			}

			result, err := c.CallTool(context.Background(), req)
			if err != nil {
				t.Fatalf("CallTool(%s): %v", tool.Name, err)
			}
			if result == nil || len(result.Content) == 0 {
				t.Fatalf("CallTool(%s): empty result", tool.Name)
			}
			text, ok := result.Content[0].(mcp.TextContent)
			if !ok {
				t.Fatalf("CallTool(%s): expected TextContent, got %T", tool.Name, result.Content[0])
			}
			if strings.Contains(strings.ToLower(text.Text), "tool not found") {
				t.Errorf("tool %s registered but has no handler", tool.Name)
			}
		})
	}
}

// Pins the offline-mode tool surface. If a tool is added or removed from
// Register, this list must be updated in lockstep — that's the point: it
// turns "I forgot to wire/unwire X" into a failing test.
func TestToolsRegistration_OfflineToolSurface(t *testing.T) {
	c := setupOfflineTest(t)

	list, err := c.ListTools(context.Background(), mcp.ListToolsRequest{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}

	expected := map[string]bool{
		"list_tables":     true,
		"describe_table":  true,
		"search_schema":   true,
		"find_related":    true,
		"validate_query":  true,
		"check_migration": true,
		"suggest_index":   true,
		"lint_schema":     true,
		"compare_nodes":   true,
		"detect":          true,
		"vacuum_health":   true,
		"reload_schema":   true,
	}
	got := map[string]bool{}
	for _, tool := range list.Tools {
		got[tool.Name] = true
	}
	for name := range expected {
		if !got[name] {
			t.Errorf("expected tool %q to be registered (offline)", name)
		}
	}
	// online-only tools must NOT be registered offline
	for _, online := range []string{"explain_query", "refresh_schema", "check_drift"} {
		if got[online] {
			t.Errorf("online-only tool %q should not be registered offline", online)
		}
	}
}
