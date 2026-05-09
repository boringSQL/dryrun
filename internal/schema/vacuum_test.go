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
	if d.VacuumThreshold != 100 {
		t.Errorf("expected threshold 100, got %d", d.VacuumThreshold)
	}
	if d.VacuumScaleFactor != 0.05 {
		t.Errorf("expected scale factor 0.05, got %f", d.VacuumScaleFactor)
	}
	if d.AnalyzeThreshold != 200 {
		t.Errorf("expected analyze threshold 200, got %d", d.AnalyzeThreshold)
	}
	if d.AnalyzeScaleFactor != 0.02 {
		t.Errorf("expected analyze scale factor 0.02, got %f", d.AnalyzeScaleFactor)
	}
	if d.VacuumCostDelay != 10 {
		t.Errorf("expected cost delay 10, got %d", d.VacuumCostDelay)
	}
	if d.VacuumCostLimit != 500 {
		t.Errorf("expected cost limit 500, got %d", d.VacuumCostLimit)
	}
	if d.FreezeMaxAge != 300_000_000 {
		t.Errorf("expected freeze max age 300M, got %d", d.FreezeMaxAge)
	}
	if d.MultixactFreezeMaxAge != 500_000_000 {
		t.Errorf("expected multixact freeze max age 500M, got %d", d.MultixactFreezeMaxAge)
	}
}

func TestParseAutovacuumDefaults_InvalidValues(t *testing.T) {
	gucs := []GucSetting{
		{Name: "autovacuum_vacuum_threshold", Setting: "not_a_number"},
		{Name: "autovacuum_vacuum_scale_factor", Setting: "bad"},
	}
	d := ParseAutovacuumDefaults(gucs)
	// Should fall back to defaults
	if d.VacuumThreshold != 50 {
		t.Errorf("expected default threshold 50 on parse error, got %d", d.VacuumThreshold)
	}
	if d.VacuumScaleFactor != 0.2 {
		t.Errorf("expected default scale factor 0.2 on parse error, got %f", d.VacuumScaleFactor)
	}
}

func vacuumTestSnap() *SchemaSnapshot {
	return &SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0", Database: "test",
		Timestamp: time.Now().UTC(), ContentHash: "test",
	}
}

func TestAnalyzeVacuumHealth_SmallTableSkipped(t *testing.T) {
	snap := vacuumTestSnap()
	snap.Tables = []Table{{
		Schema: "public", Name: "small",
		Stats: &TableStats{Reltuples: 5000, DeadTuples: 100},
	}}
	results := AnalyzeVacuumHealth(snap)
	if len(results) != 0 {
		t.Errorf("expected 0 results for small table, got %d", len(results))
	}
}

func TestAnalyzeVacuumHealth_NoStatsSkipped(t *testing.T) {
	snap := vacuumTestSnap()
	snap.Tables = []Table{{
		Schema: "public", Name: "no_stats",
	}}
	results := AnalyzeVacuumHealth(snap)
	if len(results) != 0 {
		t.Errorf("expected 0 results for table without stats, got %d", len(results))
	}
}

func TestAnalyzeVacuumHealth_DefaultSettings(t *testing.T) {
	snap := vacuumTestSnap()
	snap.Tables = []Table{{
		Schema: "public", Name: "big_table",
		Stats: &TableStats{Reltuples: 1_000_000, DeadTuples: 5000},
	}}
	results := AnalyzeVacuumHealth(snap)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	vh := results[0]
	if vh.Table != "big_table" {
		t.Errorf("expected table big_table, got %s", vh.Table)
	}
	// trigger = 50 + 0.2 * 1M = 200050
	expectedTrigger := 50.0 + 0.2*1_000_000
	if vh.VacuumTriggerAt != expectedTrigger {
		t.Errorf("expected trigger at %f, got %f", expectedTrigger, vh.VacuumTriggerAt)
	}
	if vh.HasOverrides {
		t.Error("expected no overrides")
	}
	if !vh.AutovacuumEnabled {
		t.Error("expected autovacuum enabled")
	}
	if vh.EffectiveThreshold != 50 {
		t.Errorf("expected effective threshold 50, got %d", vh.EffectiveThreshold)
	}
	if vh.EffectiveScale != 0.2 {
		t.Errorf("expected effective scale 0.2, got %f", vh.EffectiveScale)
	}
}

func TestAnalyzeVacuumHealth_TableOverrides(t *testing.T) {
	snap := vacuumTestSnap()
	snap.Tables = []Table{{
		Schema: "public", Name: "custom_table",
		Stats:      &TableStats{Reltuples: 500_000, DeadTuples: 1000},
		Reloptions: []string{"autovacuum_vacuum_scale_factor=0.01", "autovacuum_vacuum_threshold=100"},
	}}
	results := AnalyzeVacuumHealth(snap)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	vh := results[0]
	if !vh.HasOverrides {
		t.Error("expected has_overrides=true")
	}
	// trigger = 100 + 0.01 * 500k = 5100
	expectedTrigger := 100.0 + 0.01*500_000
	if vh.VacuumTriggerAt != expectedTrigger {
		t.Errorf("expected trigger at %f, got %f", expectedTrigger, vh.VacuumTriggerAt)
	}
	if vh.EffectiveThreshold != 100 {
		t.Errorf("expected effective threshold 100, got %d", vh.EffectiveThreshold)
	}
	if vh.EffectiveScale != 0.01 {
		t.Errorf("expected effective scale 0.01, got %f", vh.EffectiveScale)
	}
}

func TestAnalyzeVacuumHealth_DisabledAutovacuum(t *testing.T) {
	snap := vacuumTestSnap()
	snap.Tables = []Table{{
		Schema: "public", Name: "disabled_av",
		Stats:      &TableStats{Reltuples: 50_000, DeadTuples: 10000},
		Reloptions: []string{"autovacuum_enabled=false"},
	}}
	results := AnalyzeVacuumHealth(snap)
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
		t.Error("expected disabled autovacuum recommendation")
	}
}

func TestAnalyzeVacuumHealth_LargeTableRecommendation(t *testing.T) {
	snap := vacuumTestSnap()
	snap.Tables = []Table{{
		Schema: "public", Name: "huge_table",
		Stats: &TableStats{Reltuples: 5_000_000, DeadTuples: 100},
	}}
	results := AnalyzeVacuumHealth(snap)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	vh := results[0]
	hasLargeTableRec := false
	for _, r := range vh.Recommendations {
		if len(r) > 0 && r[0] == 'l' { // starts with "large table"
			hasLargeTableRec = true
		}
	}
	if !hasLargeTableRec {
		t.Errorf("expected large table recommendation, got %v", vh.Recommendations)
	}
}

func TestAnalyzeVacuumHealth_HighDeadTupleRatio(t *testing.T) {
	snap := vacuumTestSnap()
	snap.Tables = []Table{{
		Schema: "public", Name: "bloated",
		Stats: &TableStats{Reltuples: 100_000, DeadTuples: 15000},
	}}
	results := AnalyzeVacuumHealth(snap)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	hasDeadTupleRec := false
	for _, r := range results[0].Recommendations {
		if len(r) > 0 && r[0] == 'h' { // starts with "high dead tuple"
			hasDeadTupleRec = true
		}
	}
	if !hasDeadTupleRec {
		t.Errorf("expected high dead tuple recommendation, got %v", results[0].Recommendations)
	}
}

func TestAnalyzeVacuumHealth_HighTriggerThreshold(t *testing.T) {
	snap := vacuumTestSnap()
	snap.Tables = []Table{{
		Schema: "public", Name: "massive",
		Stats: &TableStats{Reltuples: 100_000_000, DeadTuples: 0},
	}}
	results := AnalyzeVacuumHealth(snap)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	// trigger = 50 + 0.2 * 100M = 20_000_050, well above 10M
	hasHighThresholdRec := false
	for _, r := range results[0].Recommendations {
		if len(r) > 0 && r[0] == 'v' { // starts with "vacuum won't trigger"
			hasHighThresholdRec = true
		}
	}
	if !hasHighThresholdRec {
		t.Errorf("expected high trigger threshold recommendation, got %v", results[0].Recommendations)
	}
}

func TestAnalyzeVacuumHealth_SortedByProgress(t *testing.T) {
	snap := vacuumTestSnap()
	snap.Tables = []Table{
		{
			Schema: "public", Name: "low_progress",
			Stats: &TableStats{Reltuples: 100_000, DeadTuples: 100},
		},
		{
			Schema: "public", Name: "high_progress",
			Stats: &TableStats{Reltuples: 100_000, DeadTuples: 15000},
		},
	}
	results := AnalyzeVacuumHealth(snap)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Table != "high_progress" {
		t.Errorf("expected high_progress first (higher progress), got %s", results[0].Table)
	}
}

func TestAnalyzeVacuumHealth_GlobalGUCOverrides(t *testing.T) {
	snap := vacuumTestSnap()
	snap.GUCs = []GucSetting{
		{Name: "autovacuum_vacuum_threshold", Setting: "200"},
		{Name: "autovacuum_vacuum_scale_factor", Setting: "0.05"},
	}
	snap.Tables = []Table{{
		Schema: "public", Name: "guc_test",
		Stats: &TableStats{Reltuples: 200_000, DeadTuples: 500},
	}}
	results := AnalyzeVacuumHealth(snap)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	vh := results[0]
	// trigger = 200 + 0.05 * 200k = 10200
	expectedTrigger := 200.0 + 0.05*200_000
	if vh.VacuumTriggerAt != expectedTrigger {
		t.Errorf("expected trigger at %f, got %f", expectedTrigger, vh.VacuumTriggerAt)
	}
}

func TestParseReloptions(t *testing.T) {
	opts := parseReloptions([]string{
		"autovacuum_vacuum_scale_factor=0.01",
		"fillfactor=90",
		"autovacuum_enabled=off",
	})
	if len(opts) != 3 {
		t.Fatalf("expected 3 opts, got %d", len(opts))
	}
	if opts["autovacuum_vacuum_scale_factor"] != "0.01" {
		t.Errorf("unexpected scale factor: %s", opts["autovacuum_vacuum_scale_factor"])
	}
	if opts["fillfactor"] != "90" {
		t.Errorf("unexpected fillfactor: %s", opts["fillfactor"])
	}
}

// Pins the monotonicity contract of SuggestedVacuumKnobs: as table size grows,
// the vacuum scale factor must strictly decrease. Also checks invariants at each
// size: thresholds positive, analyze scale factor positive and not above vacSF.
func TestSuggestedVacuumKnobs_Monotonic(t *testing.T) {
	sizes := []float64{1_000, 100_000, 1_000_000, 100_000_000}
	var prevSF float64 = 1e9
	for _, n := range sizes {
		vacSF, vacThresh, azSF, azThresh := SuggestedVacuumKnobs(n)
		if vacSF <= 0 {
			t.Errorf("rows=%v: vacSF must be positive, got %v", n, vacSF)
		}
		if vacSF >= prevSF {
			t.Errorf("rows=%v: expected vacSF to decrease (prev=%v, got=%v)", n, prevSF, vacSF)
		}
		prevSF = vacSF
		if azSF <= 0 || azSF > vacSF+1e-9 {
			t.Errorf("rows=%v: analyze sf should be > 0 and <= vacSF, got az=%v vac=%v", n, azSF, vacSF)
		}
		if vacThresh <= 0 {
			t.Errorf("rows=%v: vacThresh must be positive, got %d", n, vacThresh)
		}
		if azThresh <= 0 {
			t.Errorf("rows=%v: azThresh must be positive, got %d", n, azThresh)
		}
	}
}

// pins the explicit clamps in SuggestedVacuumKnobs: vacSF >= 0.001,
// vacThresh in [500, 5000], azThresh >= 250. Exercises both extreme ends,
// a trillion-row table for the upper caps and a tiny one for the floors.
func TestSuggestedVacuumKnobs_BoundsClamp(t *testing.T) {
	// extremely large table: vacSF clamped at 0.001 floor
	vacSF, vacThresh, _, azThresh := SuggestedVacuumKnobs(1_000_000_000_000)
	if vacSF < 0.001 {
		t.Errorf("expected vacSF floored at 0.001, got %v", vacSF)
	}
	if vacThresh > 5000 {
		t.Errorf("expected vacThresh capped at 5000, got %d", vacThresh)
	}
	if azThresh < 250 {
		t.Errorf("expected azThresh floored at 250, got %d", azThresh)
	}

	// tiny table: vacThresh floored at 500
	_, vacThresh2, _, azThresh2 := SuggestedVacuumKnobs(1_000)
	if vacThresh2 < 500 {
		t.Errorf("expected vacThresh floored at 500, got %d", vacThresh2)
	}
	if azThresh2 < 250 {
		t.Errorf("expected azThresh floored at 250, got %d", azThresh2)
	}
}

// Verifies that AggregateTableStats sources vacuum/analyze timestamps only from
// the primary, never from standbys, even when standby timestamps are newer.
// Covers three shapes: primary plus standbys, standbys only (timestamps must be
// nil), and single node without an explicit standby flag still aggregating.
func TestAggregateTableStats_PrimaryOnlyVacuumTimestamps(t *testing.T) {
	old := time.Now().UTC().Add(-48 * time.Hour)
	recent := time.Now().UTC().Add(-1 * time.Hour)
	newer := time.Now().UTC()

	t.Run("primary_timestamps_win_over_newer_standby", func(t *testing.T) {
		nodes := []NodeStats{
			{Source: "primary", IsStandby: false, TableStats: []NodeTableStats{{
				Schema: "public", Table: "t",
				Stats: TableStats{
					Reltuples:       100,
					LastVacuum:      &old,
					LastAutovacuum:  &recent,
					LastAnalyze:     &old,
					LastAutoanalyze: &recent,
				},
			}}},
			{Source: "standby1", IsStandby: true, TableStats: []NodeTableStats{{
				Schema: "public", Table: "t",
				// newer standby timestamps must be ignored
				Stats: TableStats{
					LastVacuum:      &newer,
					LastAutovacuum:  &newer,
					LastAnalyze:     &newer,
					LastAutoanalyze: &newer,
				},
			}}},
			{Source: "standby2", IsStandby: true, TableStats: []NodeTableStats{{
				Schema: "public", Table: "t",
				Stats: TableStats{LastVacuum: &newer},
			}}},
		}
		got := AggregateTableStats(nodes, "public", "t")
		if got == nil {
			t.Fatal("expected aggregated stats")
		}
		if got.LastVacuum == nil || !got.LastVacuum.Equal(old) {
			t.Errorf("expected LastVacuum from primary (%v), got %v", old, got.LastVacuum)
		}
		if got.LastAutovacuum == nil || !got.LastAutovacuum.Equal(recent) {
			t.Errorf("expected LastAutovacuum from primary (%v), got %v", recent, got.LastAutovacuum)
		}
		if got.LastAnalyze == nil || !got.LastAnalyze.Equal(old) {
			t.Errorf("expected LastAnalyze from primary (%v), got %v", old, got.LastAnalyze)
		}
		if got.LastAutoanalyze == nil || !got.LastAutoanalyze.Equal(recent) {
			t.Errorf("expected LastAutoanalyze from primary (%v), got %v", recent, got.LastAutoanalyze)
		}
	})

	t.Run("all_standbys_timestamps_nil", func(t *testing.T) {
		nodes := []NodeStats{
			{Source: "s1", IsStandby: true, TableStats: []NodeTableStats{{
				Schema: "public", Table: "t",
				Stats: TableStats{Reltuples: 50, LastVacuum: &newer, LastAutovacuum: &newer, LastAnalyze: &newer, LastAutoanalyze: &newer},
			}}},
			{Source: "s2", IsStandby: true, TableStats: []NodeTableStats{{
				Schema: "public", Table: "t",
				Stats: TableStats{Reltuples: 100, LastVacuum: &recent},
			}}},
		}
		got := AggregateTableStats(nodes, "public", "t")
		if got == nil {
			t.Fatal("expected aggregated stats")
		}
		if got.LastVacuum != nil || got.LastAutovacuum != nil || got.LastAnalyze != nil || got.LastAutoanalyze != nil {
			t.Errorf("expected all timestamps nil from standby-only, got vac=%v av=%v an=%v aan=%v",
				got.LastVacuum, got.LastAutovacuum, got.LastAnalyze, got.LastAutoanalyze)
		}
		// non-timestamp aggregates still work
		if got.Reltuples != 100 {
			t.Errorf("expected Reltuples=100, got %v", got.Reltuples)
		}
	})

	t.Run("single_node_no_standby_flag_still_aggregates", func(t *testing.T) {
		nodes := []NodeStats{
			{Source: "only", TableStats: []NodeTableStats{{
				Schema: "public", Table: "t",
				Stats: TableStats{Reltuples: 10, LastVacuum: &recent, LastAutoanalyze: &recent},
			}}},
		}
		got := AggregateTableStats(nodes, "public", "t")
		if got == nil {
			t.Fatal("expected aggregated stats")
		}
		if got.LastVacuum == nil || !got.LastVacuum.Equal(recent) {
			t.Errorf("expected LastVacuum from single node, got %v", got.LastVacuum)
		}
		if got.LastAutoanalyze == nil || !got.LastAutoanalyze.Equal(recent) {
			t.Errorf("expected LastAutoanalyze from single node, got %v", got.LastAutoanalyze)
		}
	})
}

func TestParseReloptions_Empty(t *testing.T) {
	opts := parseReloptions(nil)
	if len(opts) != 0 {
		t.Errorf("expected 0 opts, got %d", len(opts))
	}
}
