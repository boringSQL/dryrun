package mcpharness

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
)

// history.db is the only schema source: a leftover .dryrun/schema.json must be
// ignored rather than preferred, so a project with the file and no history
// starts uninitialized instead of silently serving stale schema.
func TestOffline_LeftoverSchemaFileIsIgnored(t *testing.T) {
	projectDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(projectDir, ".dryrun"), 0o755); err != nil {
		t.Fatal(err)
	}
	stale := &schema.SchemaSnapshot{
		PgVersion: "16.13", Database: "stale_file", ContentHash: refHash,
		Tables: []schema.Table{{OID: 99, Schema: "auth", Name: "t_from_stale_file"}},
	}
	b, _ := json.MarshalIndent(stale, "", "  ")
	if err := os.WriteFile(filepath.Join(projectDir, ".dryrun", "schema.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}

	cli := startMCP(t, projectDir)
	text, isErr := callMaybeError(t, cli, "lint_schema", map[string]any{"scope": "all"})
	if !isErr {
		t.Fatalf("expected uninitialized error, got payload: %s", text)
	}
	if !strings.Contains(text, "no schema loaded") {
		t.Errorf("expected 'no schema loaded' guidance, got: %s", text)
	}
	if strings.Contains(text, "t_from_stale_file") {
		t.Errorf("stale schema.json was served: %s", text)
	}
}

// lint_schema is the schema-only counterpart to vacuum_health: it must work
// when history.db carries a schema snapshot but no planner/activity stats.
// Locks in that the no-stats offline path still produces a well-formed JSON
// report rather than degrading silently.
func TestLintSchema_OfflineWithoutStats(t *testing.T) {
	fx := buildFixture(t, false) // schema only, no planner/activity
	cli := startMCP(t, fx.ProjectDir)

	var payload struct {
		Conventions map[string]any `json:"conventions"`
		Audit       map[string]any `json:"audit"`
	}
	callJSON(t, cli, "lint_schema", map[string]any{"scope": "all"}, &payload)

	if payload.Conventions == nil {
		t.Error("conventions section missing — lint pipeline did not run")
	}
	if payload.Audit == nil {
		t.Error("audit section missing — audit pipeline did not run")
	}
}

// Scope filter must be honored: with scope=conventions, the audit section is omitted.
func TestLintSchema_ScopeFiltersAuditSection(t *testing.T) {
	fx := buildFixture(t, false)
	cli := startMCP(t, fx.ProjectDir)

	var payload map[string]any
	callJSON(t, cli, "lint_schema", map[string]any{"scope": "conventions"}, &payload)

	if _, ok := payload["conventions"]; !ok {
		t.Error("conventions section missing under scope=conventions")
	}
	if _, ok := payload["audit"]; ok {
		t.Error("audit section present despite scope=conventions")
	}
}
