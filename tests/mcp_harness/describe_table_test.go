package mcpharness

import (
	"testing"
	"time"
)

// describe_table is the highest-traffic offline tool; in detail=stats it merges
// schema + planner sizing + per-node activity. Same wiring as vacuum_health but
// reached via a different handler — guards against partial regressions.
func TestDescribeTable_OfflineMergesSizingAndActivity(t *testing.T) {
	fx := buildFixture(t, true)
	cli := startMCP(t, fx.ProjectDir)

	var payload struct {
		Schema string `json:"schema"`
		Name   string `json:"name"`
		Stats  *struct {
			Reltuples float64 `json:"reltuples"`
			TableSize int64   `json:"table_size"`
		} `json:"stats"`
		Activity *struct {
			NDeadTup       int64      `json:"n_dead_tup"`
			LastAutovacuum *time.Time `json:"last_autovacuum"`
		} `json:"activity"`
	}
	callJSON(t, cli, "describe_table", map[string]any{
		"schema": "auth",
		"table":  "oauth_token",
		"detail": "stats",
	}, &payload)

	if payload.Schema != "auth" || payload.Name != "oauth_token" {
		t.Fatalf("wrong table echoed: %s.%s", payload.Schema, payload.Name)
	}
	if payload.Stats == nil {
		t.Fatal("stats missing — planner sizing not joined into describe_table")
	}
	if payload.Stats.Reltuples < 1_000_000 {
		t.Errorf("reltuples not surfaced: got %v", payload.Stats.Reltuples)
	}
	if payload.Stats.TableSize == 0 {
		t.Errorf("table_size not surfaced from planner sizing")
	}
	if payload.Activity == nil {
		t.Fatal("activity missing — pg_stat_user_tables counters not joined")
	}
	if payload.Activity.NDeadTup == 0 {
		t.Errorf("n_dead_tup not surfaced from activity stats")
	}
	if payload.Activity.LastAutovacuum == nil {
		t.Fatal("last_autovacuum not surfaced from activity stats")
	}
	if !payload.Activity.LastAutovacuum.Equal(fx.LastAutovacuum) {
		t.Errorf("last_autovacuum mismatch: got %v want %v", payload.Activity.LastAutovacuum, fx.LastAutovacuum)
	}
}

// Missing-table path: should surface a non-empty error result, not panic or
// return success.
func TestDescribeTable_UnknownTableErrors(t *testing.T) {
	fx := buildFixture(t, true)
	cli := startMCP(t, fx.ProjectDir)

	text, isErr := callMaybeError(t, cli, "describe_table", map[string]any{
		"schema": "auth",
		"table":  "does_not_exist",
	})
	if !isErr {
		t.Fatalf("expected error result for unknown table, got success: %q", text)
	}
	if text == "" {
		t.Fatal("error result had empty text")
	}
}
