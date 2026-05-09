package mcp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/lint"
)

// verifies that reload_schema picks up a candidate path written at runtime,
// returns the "Schema loaded from" status message, and that getSchema then
// returns a populated snapshot. End-to-end test of the lazy-init reload flow.
func TestReloadSchema_LoadsFromCandidate(t *testing.T) {
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
