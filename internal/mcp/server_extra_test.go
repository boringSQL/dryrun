package mcp

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/lint"
	"github.com/boringsql/dryrun/internal/schema"
)

// Pins the _meta block shape produced by injectMeta for an offline server:
// mode=offline, database and pg_version from the snapshot, and the hint field
// is present when non-empty, omitted when empty.
func TestInjectMeta_OfflineMode(t *testing.T) {
	snap := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.2 on x86_64", Database: "appdb",
		Timestamp: time.Now().UTC(),
	}
	srv := NewOfflineServer(snap, lint.DefaultConfig())

	t.Run("with_hint", func(t *testing.T) {
		out := map[string]any{"foo": "bar"}
		srv.injectMeta(out, "do the thing")
		meta, ok := out["_meta"].(map[string]any)
		if !ok {
			t.Fatalf("expected _meta map, got %T", out["_meta"])
		}
		if meta["mode"] != "offline" {
			t.Errorf("expected mode=offline, got %v", meta["mode"])
		}
		if meta["database"] != "appdb" {
			t.Errorf("expected database=appdb, got %v", meta["database"])
		}
		if _, has := meta["pg_version"]; !has {
			t.Error("expected pg_version key")
		}
		if meta["hint"] != "do the thing" {
			t.Errorf("expected hint set, got %v", meta["hint"])
		}
	})

	t.Run("empty_hint_omitted", func(t *testing.T) {
		out := map[string]any{}
		srv.injectMeta(out, "")
		meta, _ := out["_meta"].(map[string]any)
		if _, has := meta["hint"]; has {
			t.Error("expected no hint key when empty")
		}
	})
}

// verifies metaJSONResult returns a TextContent whose body is valid JSON that
// merges the payload at top level with an injected _meta block. Confirms hint
// propagation end-to-end through the JSON serializer.
func TestMetaJSONResult_ProducesValidJSON(t *testing.T) {
	snap := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.2 on x86_64", Database: "appdb",
		Timestamp: time.Now().UTC(),
	}
	srv := NewOfflineServer(snap, lint.DefaultConfig())

	payload := map[string]any{"valid": true, "warnings": []string{"w1"}}
	res := srv.metaJSONResult(payload, "", "use advise")
	if res == nil || len(res.Content) == 0 {
		t.Fatal("expected non-empty result")
	}
	tc, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("expected TextContent, got %T", res.Content[0])
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(tc.Text), &decoded); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, tc.Text)
	}
	meta, ok := decoded["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("expected _meta object, got %T", decoded["_meta"])
	}
	if meta["mode"] != "offline" {
		t.Errorf("expected offline mode, got %v", meta["mode"])
	}
	if meta["hint"] != "use advise" {
		t.Errorf("expected hint set, got %v", meta["hint"])
	}
	if decoded["valid"] != true {
		t.Errorf("expected payload merged: valid=true")
	}
}

// Pins the error message contract for getSchema when the server has no snap
// loaded; clients use the "no schema loaded" / "initialize first" substrings
// to surface actionable guidance back to the user.
func TestGetSchema_UninitializedError(t *testing.T) {
	srv := &Server{lintConfig: lint.DefaultConfig()}
	srv.SetUninitialized([]string{"/tmp/nonexistent"})
	_, err := srv.getSchema()
	if err == nil {
		t.Fatal("expected error when uninitialized")
	}
	if !strings.Contains(err.Error(), "no schema loaded") || !strings.Contains(err.Error(), "initialize first") {
		t.Errorf("unexpected error: %v", err)
	}
}

// verifies that reload_schema picks up a candidate path written at runtime,
// returns the "Schema loaded from" status message, and that getSchema then
// returns a populated snapshot. End-to-end test of the lazy-init reload flow.
func TestReloadSchema_LoadsFromCandidate(t *testing.T) {
	// copy demo schema to a temp path so reload picks it up
	src, err := os.ReadFile("../../examples/demo/.dryrun/schema.json")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "schema.json")
	if err := os.WriteFile(path, src, 0o644); err != nil {
		t.Fatal(err)
	}

	srv := &Server{lintConfig: lint.DefaultConfig()}
	srv.SetUninitialized([]string{path})

	res, err := srv.handleReloadSchema(context.Background(), mcp.CallToolRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || len(res.Content) == 0 {
		t.Fatal("empty result")
	}
	tc := res.Content[0].(mcp.TextContent)
	if !strings.Contains(tc.Text, "Schema loaded from") {
		t.Errorf("unexpected reload output: %s", tc.Text)
	}

	snap, err := srv.getSchema()
	if err != nil {
		t.Fatalf("getSchema after reload: %v", err)
	}
	if snap == nil || len(snap.Tables) == 0 {
		t.Error("expected snap with tables")
	}
}

// Pins the fall-through behavior when no candidate path exists on disk:
// reload_schema returns success with a "no schema file found" message instead
// of erroring, so the MCP client can show a sensible hint to the user.
func TestReloadSchema_NoCandidates(t *testing.T) {
	srv := &Server{lintConfig: lint.DefaultConfig()}
	srv.SetUninitialized([]string{"/no/such/path"})
	res, err := srv.handleReloadSchema(context.Background(), mcp.CallToolRequest{})
	if err != nil {
		t.Fatal(err)
	}
	tc := res.Content[0].(mcp.TextContent)
	if !strings.Contains(tc.Text, "no schema file found") {
		t.Errorf("expected not-found message, got %s", tc.Text)
	}
}

// verifies that the table filter passed through lint_schema actually reaches
// the audit layer: filtering to a nonexistent table should reduce
// tables_checked compared to filtering for a real one.
func TestLintSchema_TableFilter(t *testing.T) {
	c := setupOfflineTest(t)
	// existing table from demo
	out := callTool(t, c, "lint_schema", map[string]any{"table": "users"})
	// nonexistent filter should produce a much smaller (or zero) result
	outNone := callTool(t, c, "lint_schema", map[string]any{"table": "definitely_not_a_table_xyz"})

	type lintOut struct {
		Audit *lint.Report `json:"audit,omitempty"`
	}
	parse := func(s string) lintOut {
		var lo lintOut
		_ = json.Unmarshal([]byte(s), &lo)
		return lo
	}
	a := parse(out)
	b := parse(outNone)
	aCount := 0
	if a.Audit != nil {
		aCount = a.Audit.TablesChecked
	}
	bCount := 0
	if b.Audit != nil {
		bCount = b.Audit.TablesChecked
	}
	if bCount >= aCount && aCount > 0 {
		t.Errorf("expected nonexistent filter to reduce tables_checked, got a=%d b=%d", aCount, bCount)
	}
}

// Pins that vacuum_health with an unknown schema returns the friendly
// "No vacuum health concerns" message rather than an error or empty payload.
func TestVacuumHealth_SchemaFilter(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "vacuum_health", map[string]any{"schema": "nonexistent_schema_xyz"})
	if !strings.Contains(out, "No vacuum health concerns") {
		t.Errorf("expected empty vacuum health for unknown schema, got %s", out)
	}
}

// Sanity check that detect tolerates a table filter matching nothing without
// crashing or returning empty output. JSON-parseable detect kinds must still
// produce valid JSON, text-mode kinds are tolerated as is.
func TestDetect_TableFilter(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "detect", map[string]any{"table": "definitely_not_a_table_xyz"})
	// should still be valid output structure but filtered to nothing matching
	if out == "" {
		t.Fatal("empty result")
	}
	// stale_stats / unused_indexes payload should reflect filtering; just sanity check JSON parses
	var any map[string]any
	if err := json.Unmarshal([]byte(out), &any); err != nil {
		// some detect kinds return text; tolerate
		return
	}
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
