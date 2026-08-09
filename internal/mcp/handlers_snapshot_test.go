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
	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

// Pins the fall-through behavior when history.db holds no schema for the key:
// reload_schema reports the key it looked under, so an agent can tell an empty
// history from a project/database mismatch.
func TestReloadSchema_NoSchemaInHistory(t *testing.T) {
	srv := &Server{lintConfig: lint.DefaultConfig(), snapshotKey: history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}}
	srv.SetUninitialized()
	res, err := srv.handleReloadSchema(context.Background(), mcp.CallToolRequest{})
	if err != nil {
		t.Fatal(err)
	}
	tc := res.Content[0].(mcp.TextContent)
	if !strings.Contains(tc.Text, "no schema snapshot in history.db") {
		t.Errorf("expected not-found message, got %s", tc.Text)
	}
	if !strings.Contains(tc.Text, "project=p") || !strings.Contains(tc.Text, "database=d") {
		t.Errorf("expected the resolved key in the message, got %s", tc.Text)
	}
}

// history.db is the only schema source: a schema.json sitting next to it must
// never be read, and the planner/activity streams must come along for the ride.
func TestReloadSchema_HistoryOnlySchemaFileIgnored(t *testing.T) {
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

	// decoy file carries a different table name; if reload picks it, getSchema
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
	srv.SetUninitialized()

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
