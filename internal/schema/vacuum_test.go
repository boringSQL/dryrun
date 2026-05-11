package schema

import (
	"testing"
	"time"
)

func TestParseAutovacuumDefaults_NoGUCs(t *testing.T) {
	d := ParseAutovacuumDefaults(nil)
	if !d.Enabled {
		t.Error("expected enabled by default")
	}
	if d.VacuumThreshold != 50 {
		t.Errorf("expected threshold 50, got %d", d.VacuumThreshold)
	}
	if d.VacuumScaleFactor != 0.2 {
		t.Errorf("expected scale factor 0.2, got %f", d.VacuumScaleFactor)
	}
	if d.AnalyzeThreshold != 50 {
		t.Errorf("expected analyze threshold 50, got %d", d.AnalyzeThreshold)
	}
	if d.AnalyzeScaleFactor != 0.1 {
		t.Errorf("expected analyze scale factor 0.1, got %f", d.AnalyzeScaleFactor)
	}
	if d.FreezeMaxAge != 200_000_000 {
		t.Errorf("expected freeze max age 200M, got %d", d.FreezeMaxAge)
	}
}

func TestParseAutovacuumDefaults_CustomGUCs(t *testing.T) {
	gucs := []GucSetting{
		{Name: "autovacuum", Setting: "off"},
		{Name: "autovacuum_vacuum_threshold", Setting: "100"},
		{Name: "autovacuum_vacuum_scale_factor", Setting: "0.05"},
		{Name: "autovacuum_analyze_threshold", Setting: "200"},
		{Name: "autovacuum_analyze_scale_factor", Setting: "0.02"},
		{Name: "autovacuum_vacuum_cost_delay", Setting: "10"},
		{Name: "autovacuum_vacuum_cost_limit", Setting: "500"},
		{Name: "autovacuum_freeze_max_age", Setting: "300000000"},
		{Name: "autovacuum_multixact_freeze_max_age", Setting: "500000000"},
	}
	d := ParseAutovacuumDefaults(gucs)
	if d.Enabled {
		t.Error("expected disabled")
	}
	if d.VacuumThreshold != 100 || d.VacuumScaleFactor != 0.05 ||
		d.AnalyzeThreshold != 200 || d.AnalyzeScaleFactor != 0.02 ||
		d.VacuumCostDelay != 10 || d.VacuumCostLimit != 500 ||
		d.FreezeMaxAge != 300_000_000 || d.MultixactFreezeMaxAge != 500_000_000 {
		t.Errorf("custom GUC parsing failed: %+v", d)
	}
}

func TestParseAutovacuumDefaults_InvalidValues(t *testing.T) {
	d := ParseAutovacuumDefaults([]GucSetting{
		{Name: "autovacuum_vacuum_threshold", Setting: "not_a_number"},
		{Name: "autovacuum_vacuum_scale_factor", Setting: "bad"},
	})
	if d.VacuumThreshold != 50 || d.VacuumScaleFactor != 0.2 {
		t.Errorf("expected fallback to defaults on parse error, got %+v", d)
	}
}

// Builds an AnnotatedSchema with one table whose DDL + sizing + (optional) activity
// are wired up. Reloptions live on the Table; sizing on Planner; dead tuples on Activity.
func vacuumFixture(name string, reltuples float64, deadTup int64, reloptions []string) *AnnotatedSchema {
	t := Table{Schema: "public", Name: name, Reloptions: reloptions}
	return &AnnotatedSchema{
		Schema: &SchemaSnapshot{
			PgVersion: "PostgreSQL 17.0", Database: "test",
			Timestamp: time.Now().UTC(), ContentHash: "test",
			Tables: []Table{t},
		},
		Planner: &PlannerStatsSnapshot{Tables: []TableSizingEntry{
			{Table: t.Qual(), Sizing: TableSizing{Reltuples: reltuples}},
		}},
		Merged: &MergedActivity{Nodes: []NodeActivity{{
			Node: NodeIdentity{Source: "primary"},
			Tables: []TableActivityEntry{
				{Table: t.Qual(), Activity: TableActivity{NDeadTup: deadTup}},
			},
		}}},
	}
}

// Tables under 10k rows aren't worth tuning — autovacuum's defaults are fine,
// so AnalyzeVacuumHealth skips them entirely.
func TestAnalyzeVacuumHealth_SmallTableSkipped(t *testing.T) {
	a := vacuumFixture("small", 5_000, 100, nil)
	if got := AnalyzeVacuumHealth(a); len(got) != 0 {
		t.Errorf("expected 0 results for small table, got %d", len(got))
	}
}

// Without sizing, we have no Reltuples and can't decide whether the table
// warrants tuning. The implementation must return zero rather than guess.
func TestAnalyzeVacuumHealth_NoSizingSkipped(t *testing.T) {
	a := &AnnotatedSchema{
		Schema: &SchemaSnapshot{Tables: []Table{{Schema: "public", Name: "no_sizing"}}},
	}
	if got := AnalyzeVacuumHealth(a); len(got) != 0 {
		t.Errorf("expected 0 results when planner sizing is absent, got %d", len(got))
	}
}

// At default settings, vacuum trigger = threshold + scale_factor * reltuples.
// For 1M rows that's 50 + 0.2 * 1M = 200050; the implementation must match.
func TestAnalyzeVacuumHealth_DefaultSettings(t *testing.T) {
	a := vacuumFixture("big_table", 1_000_000, 5000, nil)
	results := AnalyzeVacuumHealth(a)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	vh := results[0]
	expected := 50.0 + 0.2*1_000_000
	if vh.VacuumTriggerAt != expected {
		t.Errorf("expected trigger at %f, got %f", expected, vh.VacuumTriggerAt)
	}
	if vh.HasOverrides || !vh.AutovacuumEnabled || vh.EffectiveThreshold != 50 || vh.EffectiveScale != 0.2 {
		t.Errorf("unexpected vh: %+v", vh)
	}
}

// Per-table reloptions override the cluster defaults; the calculated trigger
// must reflect them and HasOverrides must be true.
func TestAnalyzeVacuumHealth_TableOverrides(t *testing.T) {
	a := vacuumFixture("custom_table", 500_000, 1000,
		[]string{"autovacuum_vacuum_scale_factor=0.01", "autovacuum_vacuum_threshold=100"})
	results := AnalyzeVacuumHealth(a)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	vh := results[0]
	if !vh.HasOverrides {
		t.Error("expected has_overrides=true")
	}
	expected := 100.0 + 0.01*500_000
	if vh.VacuumTriggerAt != expected {
		t.Errorf("expected trigger %f, got %f", expected, vh.VacuumTriggerAt)
	}
	if vh.EffectiveThreshold != 100 || vh.EffectiveScale != 0.01 {
		t.Errorf("override settings not applied: %+v", vh)
	}
}

// `autovacuum_enabled=false` on the table emits a strongly-worded recommendation —
// the only situation where the analyzer screams at the operator.
func TestAnalyzeVacuumHealth_DisabledAutovacuum(t *testing.T) {
	a := vacuumFixture("disabled_av", 50_000, 10000, []string{"autovacuum_enabled=false"})
	results := AnalyzeVacuumHealth(a)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	vh := results[0]
	if vh.AutovacuumEnabled {
		t.Error("expected autovacuum disabled")
	}
	found := false
	for _, r := range vh.Recommendations {
		if r == "autovacuum is disabled for this table! This won't end good; you've been warned" {
			found = true
		}
	}
	if !found {
		t.Error("expected disabled-autovacuum recommendation")
	}
}

// Tables ≥ 1M rows using cluster defaults get a tuning recommendation — the
// recommendation message starts with "large table".
func TestAnalyzeVacuumHealth_LargeTableRecommendation(t *testing.T) {
	a := vacuumFixture("huge_table", 5_000_000, 100, nil)
	results := AnalyzeVacuumHealth(a)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	hasRec := false
	for _, r := range results[0].Recommendations {
		if len(r) > 0 && r[0] == 'l' {
			hasRec = true
		}
	}
	if !hasRec {
		t.Errorf("expected large-table recommendation, got %v", results[0].Recommendations)
	}
}

// Dead-tuple ratio > 10% triggers a separate recommendation — the message starts
// with "high dead tuple".
func TestAnalyzeVacuumHealth_HighDeadTupleRatio(t *testing.T) {
	a := vacuumFixture("bloated", 100_000, 15000, nil)
	results := AnalyzeVacuumHealth(a)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	hasRec := false
	for _, r := range results[0].Recommendations {
		if len(r) > 0 && r[0] == 'h' {
			hasRec = true
		}
	}
	if !hasRec {
		t.Errorf("expected high-dead-tuple recommendation, got %v", results[0].Recommendations)
	}
}

// Trigger threshold > 10M dead tuples means vacuum will rarely fire. Results
// should include a recommendation beginning with "vacuum".
func TestAnalyzeVacuumHealth_HighTriggerThreshold(t *testing.T) {
	a := vacuumFixture("massive", 100_000_000, 0, nil)
	results := AnalyzeVacuumHealth(a)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	hasRec := false
	for _, r := range results[0].Recommendations {
		if len(r) > 0 && r[0] == 'v' {
			hasRec = true
		}
	}
	if !hasRec {
		t.Errorf("expected high-trigger-threshold recommendation, got %v", results[0].Recommendations)
	}
}
