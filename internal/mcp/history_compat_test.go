package mcp

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

// legacyHistoryStore writes a Rust-shaped history.db — a snapshots table with
// no db_url_hash column — and opens it, yielding a *history.Store that reports
// CompatLegacy. This is exactly what an MCP user who upgraded from the Rust
// dryrun would be sitting on: the file is there, but this build cannot read it.
func legacyHistoryStore(t *testing.T) *history.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rust-history.db")

	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("create raw db: %v", err)
	}
	_, err = raw.Exec(`CREATE TABLE snapshots (
		id            INTEGER PRIMARY KEY,
		kind          TEXT NOT NULL,
		content_hash  TEXT NOT NULL,
		snapshot_json TEXT NOT NULL,
		project_id    TEXT,
		database_id   TEXT
	)`)
	if err != nil {
		t.Fatalf("seed rust schema: %v", err)
	}
	raw.Close()

	hist, err := history.Open(path)
	if err != nil {
		t.Fatalf("history.Open: %v", err)
	}
	if hist.Compat() != history.CompatLegacy {
		t.Fatalf("fixture is not legacy: got Compat %v, want legacy", hist.Compat())
	}
	t.Cleanup(func() { hist.Close() })
	return hist
}

func minimalSnapshot() *schema.SchemaSnapshot {
	return &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.2 on x86_64",
		Database:  "appdb",
		Timestamp: time.Now().UTC(),
	}
}

// TestInstructionsSurfacesLegacyHistory: an MCP-only user never sees CLI
// stderr, so a legacy history.db has to reach them through the protocol. The
// server instructions are delivered to every client on connect, so the
// compatibility warning must ride along in there. Without this, the user just
// gets silently degraded results with no clue why.
func TestInstructionsSurfacesLegacyHistory(t *testing.T) {
	srv := NewServer(nil, "", minimalSnapshot(), legacyHistoryStore(t), lint.DefaultConfig(), "")

	got := srv.Instructions()
	if !strings.Contains(got, "older dryrun") {
		t.Errorf("Instructions() must warn about the legacy history.db, got:\n%s", got)
	}
}

// TestInstructionsCleanWhenNoHistory: the warning is conditional. A server
// with no history store at all (offline mode) must NOT carry a history
// warning — a false alarm would train users to ignore the real one.
func TestInstructionsCleanWhenNoHistory(t *testing.T) {
	srv := NewOfflineServer(minimalSnapshot(), lint.DefaultConfig())

	if got := srv.Instructions(); strings.Contains(got, "older dryrun") {
		t.Errorf("offline server must not warn about history.db, got:\n%s", got)
	}
}

// TestSchemaDiffReportsLegacyHistory: snapshot_diff resolves snapshots through
// history.db. When that store is legacy, the failure must explain itself in-band
// — the tool response is the only channel an MCP user reads. A bare "no such
// column" would leave them stranded.
func TestSchemaDiffReportsLegacyHistory(t *testing.T) {
	srv := NewServer(nil, "", minimalSnapshot(), legacyHistoryStore(t), lint.DefaultConfig(), "")
	srv.SetSnapshotKey(history.SnapshotKey{ProjectID: "p", DatabaseID: "d"})

	var req mcp.CallToolRequest
	req.Params.Name = "snapshot_diff"
	req.Params.Arguments = map[string]any{"from": "abc123"}
	res, err := srv.handleSnapshotDiff(context.Background(), req)
	if err != nil {
		t.Fatalf("handler returned a transport error instead of an in-band result: %v", err)
	}
	text, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("expected TextContent, got %T", res.Content[0])
	}
	if !strings.Contains(text.Text, "older dryrun") {
		t.Errorf("result should explain the legacy history.db, got: %s", text.Text)
	}
}

// TestHistoryNoteEmptyWhenStoreOK: a healthy, current store produces no note,
// so neither Instructions nor any tool response nags about it.
func TestHistoryNoteEmptyWhenStoreOK(t *testing.T) {
	okStore, err := history.Open(filepath.Join(t.TempDir(), "fresh.db"))
	if err != nil {
		t.Fatalf("history.Open: %v", err)
	}
	t.Cleanup(func() { okStore.Close() })

	srv := NewServer(nil, "", minimalSnapshot(), okStore, lint.DefaultConfig(), "")
	if note := srv.historyNote(); note != nil {
		t.Errorf("a healthy store must yield no note, got %q", *note)
	}
}
