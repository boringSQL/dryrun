package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/boringsql/dryrun/internal/lint"
	"github.com/boringsql/dryrun/internal/schema"
)

func setupOfflineTest(t *testing.T) *client.Client {
	t.Helper()

	snap, err := schema.LoadSchemaFile("../../examples/demo/.dryrun/schema.json")
	if err != nil {
		t.Fatal(err)
	}

	srv := NewOfflineServer(snap, lint.DefaultConfig())
	mcpSrv := mcpserver.NewMCPServer("dryrun-test", "0.1.0")
	srv.Register(mcpSrv)

	// Wire pipes exactly like mcptest does
	serverReader, clientWriter := io.Pipe()
	clientReader, serverWriter := io.Pipe()

	ctx, cancel := context.WithCancel(context.Background())

	stdioSrv := mcpserver.NewStdioServer(mcpSrv)
	stdioSrv.SetErrorLogger(log.New(io.Discard, "", 0))
	go stdioSrv.Listen(ctx, serverReader, serverWriter)

	var logBuf bytes.Buffer
	tr := transport.NewIO(clientReader, clientWriter, io.NopCloser(&logBuf))
	if err := tr.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}

	c := client.NewClient(tr)
	var initReq mcp.InitializeRequest
	initReq.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	if _, err := c.Initialize(ctx, initReq); err != nil {
		cancel()
		t.Fatal(err)
	}

	t.Cleanup(func() {
		tr.Close()
		cancel()
		serverWriter.Close()
		serverReader.Close()
	})

	return c
}

func callTool(t *testing.T, c *client.Client, name string, args map[string]any) string {
	t.Helper()

	var req mcp.CallToolRequest
	req.Params.Name = name
	req.Params.Arguments = args

	result, err := c.CallTool(context.Background(), req)
	if err != nil {
		t.Fatalf("CallTool(%s): %v", name, err)
	}
	if result == nil || len(result.Content) == 0 {
		t.Fatalf("CallTool(%s): empty result", name)
	}

	text, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("CallTool(%s): expected TextContent, got %T", name, result.Content[0])
	}
	return text.Text
}

func assertContains(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("expected output to contain %q, got:\n%.500s", needle, haystack)
	}
}

func TestOfflineMCPTools(t *testing.T) {
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

	t.Run("suggest_index", func(t *testing.T) {
		out := callTool(t, c, "suggest_index", map[string]any{
			"sql": "SELECT * FROM tasks WHERE status = 'open'",
		})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("lint_schema_default_all", func(t *testing.T) {
		out := callTool(t, c, "lint_schema", nil)
		assertContains(t, out, "findings")
		// default scope=all should include both convention and audit rules
		assertContains(t, out, "config_source")
	})

	t.Run("compare_nodes", func(t *testing.T) {
		out := callTool(t, c, "compare_nodes", map[string]any{"table": "users"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_default_all", func(t *testing.T) {
		out := callTool(t, c, "detect", nil)
		assertContains(t, out, "stale_stats")
		assertContains(t, out, "unused_indexes")
		assertContains(t, out, "anomalies")
		assertContains(t, out, "bloated_indexes")
	})

	t.Run("detect_stale_stats", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "stale_stats"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_unused_indexes", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "unused_indexes"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_anomalies", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "anomalies"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_bloated_indexes", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "bloated_indexes"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_bloated_with_threshold", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "bloated_indexes", "threshold": 2.0})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_invalid_kind", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "bogus"})
		assertContains(t, out, "unknown detect kind")
	})

	t.Run("lint_schema_scope_conventions", func(t *testing.T) {
		out := callTool(t, c, "lint_schema", map[string]any{"scope": "conventions"})
		assertContains(t, out, "findings")
		assertContains(t, out, "conventions")
	})

	t.Run("lint_schema_scope_audit", func(t *testing.T) {
		out := callTool(t, c, "lint_schema", map[string]any{"scope": "audit"})
		assertContains(t, out, "findings")
		assertContains(t, out, "audit")
	})

	t.Run("lint_schema_scope_all", func(t *testing.T) {
		out := callTool(t, c, "lint_schema", map[string]any{"scope": "all"})
		assertContains(t, out, "findings")
		assertContains(t, out, "all")
	})

	t.Run("lint_schema_with_schema_filter", func(t *testing.T) {
		out := callTool(t, c, "lint_schema", map[string]any{"schema": "public"})
		assertContains(t, out, "findings")
	})

	t.Run("vacuum_health", func(t *testing.T) {
		out := callTool(t, c, "vacuum_health", nil)
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("vacuum_health_with_filter", func(t *testing.T) {
		out := callTool(t, c, "vacuum_health", map[string]any{"table": "users"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("vacuum_health_nonexistent_table", func(t *testing.T) {
		out := callTool(t, c, "vacuum_health", map[string]any{"table": "nonexistent_xyz"})
		assertContains(t, out, "No vacuum health concerns")
	})

}

// auditRulePrefixes are rule prefixes that only appear from audit scope.
var auditRulePrefixes = []string{"indexes/", "fk/circular", "fk/orphan", "fk/type_mismatch", "docs/", "vacuum/", "naming/bool_prefix", "naming/reserved", "naming/id_mismatch", "pk/non_sequential"}

// conventionRulePrefixes are rule prefixes that only appear from conventions scope.
var conventionRulePrefixes = []string{"types/", "timestamps/", "constraints/", "partition/"}

func TestLintSchemaScopeIsolation(t *testing.T) {
	c := setupOfflineTest(t)

	parseFindings := func(t *testing.T, out string) []lint.Finding {
		t.Helper()
		var report lint.Report
		if err := json.Unmarshal([]byte(out), &report); err != nil {
			t.Fatalf("failed to parse report: %v", err)
		}
		return report.Findings
	}

	hasRulePrefix := func(findings []lint.Finding, prefix string) bool {
		for _, f := range findings {
			if strings.HasPrefix(f.Rule, prefix) || f.Rule == prefix {
				return true
			}
		}
		return false
	}

	t.Run("conventions_excludes_audit_rules", func(t *testing.T) {
		out := callTool(t, c, "lint_schema", map[string]any{"scope": "conventions"})
		findings := parseFindings(t, out)
		for _, prefix := range auditRulePrefixes {
			if hasRulePrefix(findings, prefix) {
				t.Errorf("conventions scope should not contain audit rule %q", prefix)
			}
		}
	})

	t.Run("audit_excludes_convention_rules", func(t *testing.T) {
		out := callTool(t, c, "lint_schema", map[string]any{"scope": "audit"})
		findings := parseFindings(t, out)
		for _, prefix := range conventionRulePrefixes {
			if hasRulePrefix(findings, prefix) {
				t.Errorf("audit scope should not contain convention rule %q", prefix)
			}
		}
	})

	t.Run("all_is_superset", func(t *testing.T) {
		allOut := callTool(t, c, "lint_schema", map[string]any{"scope": "all"})
		convOut := callTool(t, c, "lint_schema", map[string]any{"scope": "conventions"})
		auditOut := callTool(t, c, "lint_schema", map[string]any{"scope": "audit"})

		allFindings := parseFindings(t, allOut)
		convFindings := parseFindings(t, convOut)
		auditFindings := parseFindings(t, auditOut)

		if len(allFindings) < len(convFindings) {
			t.Errorf("all scope (%d findings) should have >= conventions (%d)", len(allFindings), len(convFindings))
		}
		if len(allFindings) < len(auditFindings) {
			t.Errorf("all scope (%d findings) should have >= audit (%d)", len(allFindings), len(auditFindings))
		}
		if len(allFindings) != len(convFindings)+len(auditFindings) {
			t.Errorf("all (%d) should equal conventions (%d) + audit (%d)", len(allFindings), len(convFindings), len(auditFindings))
		}
	})

	t.Run("schema_filter_reduces_findings", func(t *testing.T) {
		allOut := callTool(t, c, "lint_schema", nil)
		filteredOut := callTool(t, c, "lint_schema", map[string]any{"schema": "nonexistent_schema"})

		allFindings := parseFindings(t, allOut)
		filteredFindings := parseFindings(t, filteredOut)

		if len(filteredFindings) >= len(allFindings) && len(allFindings) > 0 {
			t.Errorf("filtering by nonexistent schema should reduce findings, got %d vs %d", len(filteredFindings), len(allFindings))
		}
	})
}
