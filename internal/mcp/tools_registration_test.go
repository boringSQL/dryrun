package mcp

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/pkg/lint"
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
			case "describe_table":
				req.Params.Arguments = map[string]any{"table": "users"}
			case "validate_query":
				req.Params.Arguments = map[string]any{"sql": "SELECT 1"}
			case "check_migration":
				req.Params.Arguments = map[string]any{"ddl": "ALTER TABLE users ADD COLUMN x INT"}
			case "advise":
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

// Every registered tool must advertise a non-empty inputSchema with
// type:"object". MCP clients use this to validate arguments and to render
// parameter UIs; the empty {"properties":{},"required":[],"type":""} shape
// the old tool() shorthand produced broke both. The expected-required map
// below pins the required-args contract for each tool that has any.
func TestToolsRegistration_InputSchemaShape(t *testing.T) {
	c := setupOfflineTest(t)

	list, err := c.ListTools(context.Background(), mcp.ListToolsRequest{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}

	expectedRequired := map[string][]string{
		"describe_table":  {"table"},
		"validate_query":  {"sql"},
		"check_migration": {"ddl"},
		"advise":          {"sql"},
	}

	for _, tool := range list.Tools {
		t.Run(tool.Name, func(t *testing.T) {
			if tool.InputSchema.Type != "object" {
				t.Errorf("tool %s: inputSchema.type = %q, want \"object\"", tool.Name, tool.InputSchema.Type)
			}
			want, ok := expectedRequired[tool.Name]
			if !ok {
				return
			}
			got := map[string]bool{}
			for _, r := range tool.InputSchema.Required {
				got[r] = true
			}
			for _, r := range want {
				if !got[r] {
					t.Errorf("tool %s: required %q missing (have %v)", tool.Name, r, tool.InputSchema.Required)
				}
			}
		})
	}
}

// The exploration and finding tools declare output schemas so 2025-06-18+
// clients can consume structuredContent instead of scraping JSON out of the
// text blob (PLAN-MCP-upgrade.md steps 4 / B+C). This pins which tools carry
// one — if a schema is dropped (or a new high-value tool forgets to declare
// one and gets added to this list), the test fails. Tools outside this list
// are free to stay text-only.
func TestToolsRegistration_OutputSchemas(t *testing.T) {
	c := setupOfflineTest(t)

	list, err := c.ListTools(context.Background(), mcp.ListToolsRequest{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}

	wantOutputSchema := map[string]bool{
		"find_objects":    true,
		"describe_table":  true,
		"check_migration": true,
		"detect":          true,
		"lint_schema":     true,
		"snapshot_diff":   true,
	}

	seen := map[string]bool{}
	for _, tool := range list.Tools {
		seen[tool.Name] = true
		if !wantOutputSchema[tool.Name] {
			continue
		}
		if tool.OutputSchema.Type != "object" {
			t.Errorf("tool %s: outputSchema.type = %q, want \"object\"", tool.Name, tool.OutputSchema.Type)
		}
	}
	for name := range wantOutputSchema {
		if !seen[name] {
			t.Errorf("expected tool %q to be registered", name)
		}
	}
}

// A declared output schema is only half the contract — the result must
// actually carry structuredContent, or schema-aware clients get nothing and
// output validation silently skips (mcp-go validates only non-nil
// StructuredContent). This drives the happy path of every schema-carrying tool
// that works against the offline demo snapshot and asserts the structured
// payload is present, closing the loophole where a handler quietly regresses
// to text-only and every other test still passes. snapshot_diff is exercised
// separately (snapshot_diff_test.go) because it needs a seeded history store.
func TestToolsRegistration_StructuredContentPresent(t *testing.T) {
	c := setupOfflineTest(t)

	calls := []struct {
		name string
		tool string
		args map[string]any
	}{
		{"find_objects", "find_objects", nil},
		{"find_objects_query", "find_objects", map[string]any{"query": "users"}},
		{"describe_table", "describe_table", map[string]any{"table": "users"}},
		{"check_migration", "check_migration", map[string]any{"ddl": "ALTER TABLE users ADD COLUMN phone TEXT"}},
		{"detect", "detect", nil},
		{"detect_vacuum_health", "detect", map[string]any{"kind": "vacuum_health"}},
		{"lint_schema", "lint_schema", nil},
	}

	for _, tc := range calls {
		name, args := tc.name, tc.args
		t.Run(name, func(t *testing.T) {
			var req mcp.CallToolRequest
			req.Params.Name = tc.tool
			req.Params.Arguments = args

			result, err := c.CallTool(context.Background(), req)
			if err != nil {
				t.Fatalf("CallTool(%s): %v", name, err)
			}
			if result.IsError {
				t.Fatalf("CallTool(%s): unexpected error result: %v", name, result.Content)
			}
			if result.StructuredContent == nil {
				t.Errorf("tool %s declares an output schema but returned no structuredContent", name)
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
		"find_objects":    true,
		"describe_table":  true,
		"validate_query":  true,
		"check_migration": true,
		"lint_schema":     true,
		"detect":          true,
		"advise":          true,
		"snapshot_diff":   true,
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
	// online-only tools must NOT be registered offline, and analyze_plan is
	// gone entirely -- advise reads a supplied plan now
	for _, online := range []string{"explain_query", "check_drift", "analyze_plan"} {
		if got[online] {
			t.Errorf("online-only tool %q should not be registered offline", online)
		}
	}
	// schema_diff was dropped in favor of snapshot_diff
	if got["schema_diff"] {
		t.Error("schema_diff should no longer be registered")
	}
}

func boolPtrValue(t *testing.T, p *bool, field string) bool {
	t.Helper()
	if p == nil {
		t.Fatalf("%s is unset; mcp-go defaults it to the opposite of what our tools do", field)
	}
	return *p
}

// Nothing registered without a connection can reach a database, so every tool
// on the offline surface has to say so. Clients gate on these hints and
// inspectors grade on them, and the offline endpoint's whole claim is that it
// touches nothing. Both registration paths: RegisterOffline is what a hosted
// transport serves, and it is the one the claim is made about.
func TestOfflineToolsAnnotateThemselvesReadOnly(t *testing.T) {
	for name, c := range map[string]*client.Client{
		"Register with no pool": setupOfflineTest(t),
		"RegisterOffline":       setupOfflineRegisterTest(t),
	} {
		t.Run(name, func(t *testing.T) {
			list, err := c.ListTools(context.Background(), mcp.ListToolsRequest{})
			if err != nil {
				t.Fatalf("ListTools: %v", err)
			}
			if len(list.Tools) == 0 {
				t.Fatal("no tools registered")
			}
			for _, tool := range list.Tools {
				a := tool.Annotations
				if !boolPtrValue(t, a.ReadOnlyHint, tool.Name+".readOnlyHint") {
					t.Errorf("%s: readOnlyHint false with no connection", tool.Name)
				}
				if boolPtrValue(t, a.DestructiveHint, tool.Name+".destructiveHint") {
					t.Errorf("%s: destructiveHint true with no connection", tool.Name)
				}
			}
		})
	}
}

// One tool definition per mode: the parameter, the description and the
// annotation all have to agree about whether advise can execute.
func TestAdviseOffersAnalyzeOnlyWithAConnection(t *testing.T) {
	adviseTool := func(t *testing.T, srv *Server) mcp.Tool {
		t.Helper()
		list, err := serveOffline(t, srv).ListTools(context.Background(), mcp.ListToolsRequest{})
		if err != nil {
			t.Fatalf("ListTools: %v", err)
		}
		for _, tool := range list.Tools {
			if tool.Name == "advise" {
				return tool
			}
		}
		t.Fatal("advise is not registered")
		return mcp.Tool{}
	}

	t.Run("offline", func(t *testing.T) {
		tool := adviseTool(t, NewOfflineServer(loadDemoSchema(t), lint.DefaultConfig()))
		if _, ok := tool.InputSchema.Properties["analyze"]; ok {
			t.Error("analyze is declared on a server that cannot execute anything")
		}
		if strings.Contains(tool.Description, "EXPLAIN ANALYZE") {
			t.Errorf("description promises execution: %q", tool.Description)
		}
		// "needs no database connection" is on every schema-only description;
		// what has to survive is the clause naming what a connection adds
		if !strings.Contains(tool.Description, "plan-shape") {
			t.Errorf("description does not say what is missing: %q", tool.Description)
		}
		if !boolPtrValue(t, tool.Annotations.ReadOnlyHint, "readOnlyHint") {
			t.Error("readOnlyHint false on a server that cannot execute anything")
		}
		if boolPtrValue(t, tool.Annotations.DestructiveHint, "destructiveHint") {
			t.Error("destructiveHint true on a server that cannot execute anything")
		}
	})

	// registration only reads whether the pool is set, so an unusable one is
	// enough to pin the live branch
	t.Run("with a connection", func(t *testing.T) {
		srv := NewOfflineServer(loadDemoSchema(t), lint.DefaultConfig())
		srv.pool = &pgxpool.Pool{}

		tool := adviseTool(t, srv)
		if _, ok := tool.InputSchema.Properties["analyze"]; !ok {
			t.Error("analyze is missing where it can run")
		}
		if !strings.Contains(tool.Description, "EXECUTES") {
			t.Errorf("description hides that analyze executes: %q", tool.Description)
		}
		if boolPtrValue(t, tool.Annotations.ReadOnlyHint, "readOnlyHint") {
			t.Error("readOnlyHint true where analyze=true can execute the caller's SQL")
		}
		if !boolPtrValue(t, tool.Annotations.DestructiveHint, "destructiveHint") {
			t.Error("destructiveHint false where analyze=true can execute the caller's SQL")
		}
	})

	// the shared clause: both modes return corrected_sql, and a description
	// that claims advise never rewrites anything sends the agent elsewhere
	t.Run("both modes name corrected_sql", func(t *testing.T) {
		live := NewOfflineServer(loadDemoSchema(t), lint.DefaultConfig())
		live.pool = &pgxpool.Pool{}
		for mode, srv := range map[string]*Server{
			"offline": NewOfflineServer(loadDemoSchema(t), lint.DefaultConfig()),
			"live":    live,
		} {
			if d := adviseTool(t, srv).Description; !strings.Contains(d, "corrected_sql") {
				t.Errorf("%s: %q", mode, d)
			}
		}
	})
}

// Passing analyze without a connection has to be told, not dropped: the
// caller asked for the one thing this mode cannot do.
func TestAdviseSaysWhenAnalyzeWasIgnored(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "advise", map[string]any{
		"sql": "SELECT * FROM users WHERE email = 'x'", "analyze": true,
	})
	if !strings.Contains(out, "analyze needs a database connection") {
		t.Errorf("no signal that analyze was dropped:\n%.400s", out)
	}
}

// A validation error is the bigger news, but it must not swallow the fact that
// the caller asked for the one thing this mode cannot do.
func TestAdviseSaysAnalyzeWasIgnoredEvenOnInvalidSQL(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "advise", map[string]any{
		"sql": "SELECT nosuchcolumn FROM nosuchtable", "analyze": true,
	})
	if !strings.Contains(out, "analyze needs a database connection") {
		t.Errorf("no signal that analyze was dropped:\n%.400s", out)
	}
}
