package mcpharness

import (
	"testing"
	"time"
)

// Regression guard for the offline-history wiring bug: NewOfflineServer used to
// skip history.db, so Planner/Merged stayed nil and vacuum_health returned the
// literal "No vacuum health concerns found." instead of a JSON wrapper.
func TestVacuumHealth_OfflineSurfacesAutovacuumTimestamps(t *testing.T) {
	fx := buildFixture(t, true)
	cli := startMCP(t, fx.ProjectDir)

	var payload struct {
		VacuumHealth []struct {
			Schema         string     `json:"schema"`
			Table          string     `json:"table"`
			Reltuples      float64    `json:"reltuples"`
			DeadTuples     int64      `json:"dead_tuples"`
			LastAutovacuum *time.Time `json:"last_autovacuum"`
		} `json:"vacuum_health"`
		Count int `json:"count"`
	}
	// vacuum_health defaults to schema=public; the fixture table lives in auth
	callJSON(t, cli, "vacuum_health", map[string]any{"schema": "auth"}, &payload)

	if payload.Count == 0 {
		t.Fatal("count=0: offline mode did not load planner stats from history.db (regression)")
	}
	var entry *struct {
		Schema         string     `json:"schema"`
		Table          string     `json:"table"`
		Reltuples      float64    `json:"reltuples"`
		DeadTuples     int64      `json:"dead_tuples"`
		LastAutovacuum *time.Time `json:"last_autovacuum"`
	}
	for i := range payload.VacuumHealth {
		if payload.VacuumHealth[i].Schema == "auth" && payload.VacuumHealth[i].Table == "oauth_token" {
			entry = &payload.VacuumHealth[i]
			break
		}
	}
	if entry == nil {
		t.Fatal("auth.oauth_token missing from vacuum_health output")
	}
	if entry.Reltuples < 1_000_000 {
		t.Errorf("reltuples not surfaced from planner stats: got %v", entry.Reltuples)
	}
	if entry.DeadTuples == 0 {
		t.Errorf("dead_tuples not surfaced from activity stats")
	}
	if entry.LastAutovacuum == nil {
		t.Fatal("last_autovacuum not surfaced from activity stats (regression)")
	}
	if !entry.LastAutovacuum.Equal(fx.LastAutovacuum) {
		t.Errorf("last_autovacuum mismatch: got %v want %v", entry.LastAutovacuum, fx.LastAutovacuum)
	}
}
