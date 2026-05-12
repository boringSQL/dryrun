package mcp

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/lint"
	"github.com/boringsql/dryrun/internal/schema"
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

// Locks down the v0.6 lookup order: when both history.db and a schema.json
// candidate are present, reload_schema must prefer history.db so the
// planner/activity streams come along for the ride. If this test ever
// regresses, stats apply and the activity-aware tools silently lose data.
func TestReloadSchema_HistoryBeatsSchemaFile(t *testing.T) {
	dir := t.TempDir()
	store, err := history.Open(filepath.Join(dir, "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	histSnap := &schema.SchemaSnapshot{
		Database:    "from_history",
		ContentHash: "hist-1",
		Tables:      []schema.Table{{Schema: "public", Name: "t_from_history"}},
	}
	if _, err := store.PutSchema(context.Background(), key, histSnap); err != nil {
		t.Fatal(err)
	}

	// candidate file carries a different table name; if reload picks it, getSchema
	// will see t_from_file instead of t_from_history.
	fileSnap := &schema.SchemaSnapshot{
		Database:    "from_file",
		ContentHash: "file-1",
		Tables:      []schema.Table{{Schema: "public", Name: "t_from_file"}},
	}
	path := filepath.Join(dir, "schema.json")
	data, err := json.Marshal(fileSnap)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	srv := &Server{lintConfig: lint.DefaultConfig(), history: store, snapshotKey: key}
	srv.SetUninitialized([]string{path})

	res, err := srv.handleReloadSchema(context.Background(), mcp.CallToolRequest{})
	if err != nil {
		t.Fatal(err)
	}
	tc := res.Content[0].(mcp.TextContent)
	if !strings.Contains(tc.Text, "history.db") {
		t.Errorf("expected history.db source in reload output, got: %s", tc.Text)
	}

	snap, err := srv.getSchema()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Tables) == 0 || snap.Tables[0].Name != "t_from_history" {
		t.Errorf("expected table from history.db, got %+v", snap.Tables)
	}
}
