package vacuum

import (
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/pkg/snapshot"
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
	gucs := []snapshot.GucSetting{
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
	d := ParseAutovacuumDefaults([]snapshot.GucSetting{
		{Name: "autovacuum_vacuum_threshold", Setting: "not_a_number"},
		{Name: "autovacuum_vacuum_scale_factor", Setting: "bad"},
	})
	if d.VacuumThreshold != 50 || d.VacuumScaleFactor != 0.2 {
		t.Errorf("expected fallback to defaults on parse error, got %+v", d)
	}
}

// Builds an AnnotatedSchema with one table whose DDL + sizing + (optional) activity
// are wired up. Reloptions live on the Table; sizing on Planner; dead tuples on Activity.
func vacuumFixture(name string, reltuples float64, deadTup int64, reloptions []string) *snapshot.AnnotatedSchema {
	t := snapshot.Table{Schema: "public", Name: name, Reloptions: reloptions}
	return &snapshot.AnnotatedSchema{
		Schema: &snapshot.SchemaSnapshot{
			PgVersion: "PostgreSQL 17.0", Database: "test",
			Timestamp: time.Now().UTC(), ContentHash: "test",
			Tables: []snapshot.Table{t},
		},
		Planner: &snapshot.PlannerStatsSnapshot{Tables: []snapshot.TableSizingEntry{
			{Table: t.Qual(), Sizing: snapshot.TableSizing{Reltuples: reltuples}},
		}},
		Merged: &snapshot.MergedActivity{Nodes: []snapshot.NodeActivity{{
			Node: snapshot.NodeIdentity{Source: "primary"},
			Tables: []snapshot.TableActivityEntry{
				{Table: t.Qual(), Activity: snapshot.TableActivity{NDeadTup: deadTup}},
			},
		}}},
	}
}

// findingFor pulls the first finding with the given code out of a result set.
func findingFor(results []VacuumHealth, code VacuumCode) *VacuumFinding {
	for i := range results {
		for j := range results[i].Findings {
			if results[i].Findings[j].Code == code {
				return &results[i].Findings[j]
			}
		}
	}
	return nil
}

// On stock PostgreSQL a large table on default autovacuum settings earns the
// fixed-knob tuning suggestion at medium severity.
func TestAnalyzeVacuumHealth_KnobAdvice_Postgres(t *testing.T) {
	a := vacuumFixture("big", 2_000_000, 5000, nil)
	f := findingFor(AnalyzeVacuumHealth(a), CodeDefaultKnobsLargeTable)
	if f == nil {
		t.Fatal("expected a default-knobs finding on vanilla postgres")
	}
	if f.Severity != SeverityMedium {
		t.Errorf("severity = %q, want medium", f.Severity)
	}
	// The knobs live in Action as a copy-pasteable statement; Message keeps the why.
	if !strings.HasPrefix(f.Action, "ALTER TABLE public.big SET (autovacuum_vacuum_scale_factor = ") ||
		!strings.HasSuffix(f.Action, ";") {
		t.Errorf("postgres advice should carry the ALTER TABLE statement, got: %s", f.Action)
	}
	if !strings.Contains(f.Message, "default autovacuum settings") {
		t.Errorf("message should keep the prose why, got: %s", f.Message)
	}
}

// On AlloyDB Omni the fixed-knob runbook is the wrong advice: adaptive autovacuum
// schedules by load. Reworded and downgraded to info, but the knobs are still the
// operator's to set (Omni runs on your own box), so no "managed" caveat.
func TestAnalyzeVacuumHealth_KnobAdvice_Omni(t *testing.T) {
	a := vacuumFixture("big", 2_000_000, 5000, nil)
	a.Schema.Flavor = snapshot.FlavorAlloyDBOmni
	f := findingFor(AnalyzeVacuumHealth(a), CodeDefaultKnobsLargeTable)
	if f == nil {
		t.Fatal("expected a default-knobs finding on Omni")
	}
	if f.Severity != SeverityInfo {
		t.Errorf("severity = %q, want info on adaptive autovacuum", f.Severity)
	}
	if !strings.Contains(f.Message, "adaptive autovacuum") {
		t.Errorf("Omni advice should mention adaptive autovacuum, got: %s", f.Message)
	}
	if strings.Contains(f.Message, "autovacuum_vacuum_scale_factor=") {
		t.Errorf("Omni advice must not push the fixed knobs, got: %s", f.Message)
	}
	if strings.Contains(f.Message, "not yours to set") {
		t.Errorf("Omni knobs are tunable, should not claim otherwise, got: %s", f.Message)
	}
}

// Managed AlloyDB adds that the knobs aren't the operator's to set (no ALTER SYSTEM).
func TestAnalyzeVacuumHealth_KnobAdvice_Managed(t *testing.T) {
	a := vacuumFixture("big", 2_000_000, 5000, nil)
	a.Schema.Flavor = snapshot.FlavorAlloyDBManaged
	f := findingFor(AnalyzeVacuumHealth(a), CodeDefaultKnobsLargeTable)
	if f == nil {
		t.Fatal("expected a default-knobs finding on managed AlloyDB")
	}
	if f.Severity != SeverityInfo {
		t.Errorf("severity = %q, want info", f.Severity)
	}
	if !strings.Contains(f.Message, "not yours to set") {
		t.Errorf("managed advice should note the knobs are unmanageable, got: %s", f.Message)
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
	a := &snapshot.AnnotatedSchema{
		Schema: &snapshot.SchemaSnapshot{Tables: []snapshot.Table{{Schema: "public", Name: "no_sizing"}}},
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

// A partitioned parent has no heap of its own — autovacuum runs on the leaf
// partitions, and the parent's reltuples is the aggregate across them. Even when that
// aggregate is large and on default knobs, the parent must be skipped (the leaves are
// analyzed as their own tables); flagging it is misleading noise.
func TestAnalyzeVacuumHealth_PartitionedParentSkipped(t *testing.T) {
	a := vacuumFixture("part_parent", 70_000_000, 0, nil)
	a.Schema.Tables[0].PartitionInfo = &snapshot.PartitionInfo{
		Strategy: snapshot.PartitionRange,
		Key:      "id",
		Children: []snapshot.PartitionChild{{}},
	}
	if got := AnalyzeVacuumHealth(a); len(got) != 0 {
		t.Errorf("expected 0 results for a partitioned parent, got %d: %+v", len(got), got)
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

// FrozenXidAge mirrors postgres' age(relfrozenxid): the forward distance in
// transactions between the table's frozen xid and the snapshot's next-xid
// reference. We exercise the simple case, the wraparound (modular) case, and
// the two "not applicable" sentinels so callers can trust the bool.
func TestFrozenXidAge(t *testing.T) {
	cases := []struct {
		name        string
		frozen      int64
		databaseXid int64
		wantAge     int64
		wantOK      bool
	}{
		{name: "simple forward distance", frozen: 1_000, databaseXid: 201_000_000, wantAge: 200_999_000, wantOK: true},
		{name: "freshly frozen, tiny age", frozen: 500_000, databaseXid: 500_050, wantAge: 50, wantOK: true},
		// databaseXid wrapped past 2^32 (epoch bumped): current32 = 100, frozen at
		// 4_000_000_000, so age = (100 - 4000000000) mod 2^32 = 294_967_396.
		{name: "wraparound: current already past 2^32", frozen: 4_000_000_000, databaseXid: (int64(1) << 32) + 100, wantAge: 294_967_396, wantOK: true},
		{name: "no frozen xid (partitioned parent)", frozen: 0, databaseXid: 12_345, wantAge: 0, wantOK: false},
		{name: "no reference point", frozen: 999, databaseXid: 0, wantAge: 0, wantOK: false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := snapshot.TableSizing{RelfrozenXid: c.frozen}
			age, ok := s.FrozenXidAge(c.databaseXid)
			if ok != c.wantOK {
				t.Fatalf("ok=%v want %v", ok, c.wantOK)
			}
			if ok && age != c.wantAge {
				t.Errorf("age=%d want %d", age, c.wantAge)
			}
		})
	}
}

// A relfrozenxid age past vacuum_failsafe_age (default 1.6B) means anti-wraparound
// VACUUM is in last-resort mode — a high finding. Reaching freeze_max_age (the
// routine trigger) alone must NOT fire; that's covered by WraparoundQuiet.
func TestAnalyzeVacuumHealth_WraparoundImminent(t *testing.T) {
	a := vacuumFixture("aging", 1_000_000, 0, nil)
	// default failsafe is 1.6B; put the frozen xid 1.7B behind the ref.
	a.Planner.DatabaseXid = 1_710_000_000
	a.Planner.Tables[0].Sizing.RelfrozenXid = 10_000_000 // age = 1.7B, past failsafe

	results := AnalyzeVacuumHealth(a)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	vh := results[0]
	if vh.XidAge != 1_700_000_000 {
		t.Errorf("XidAge=%d want 1700000000", vh.XidAge)
	}
	if vh.FreezeMaxAge != 200_000_000 {
		t.Errorf("FreezeMaxAge=%d want 200000000", vh.FreezeMaxAge)
	}
	if vh.FailsafeAge != 1_600_000_000 {
		t.Errorf("FailsafeAge=%d want 1600000000", vh.FailsafeAge)
	}
	if !hasFreezeFinding(vh, CodeFreezeAgeHigh, SeverityHigh, "last-resort") {
		t.Errorf("expected high relfrozenxid last-resort finding, got %+v", vh.Findings)
	}
}

// Multixact wraparound mirrors the xid path: relminmxid age past
// vacuum_multixact_failsafe_age (1.6B) surfaces a high finding naming relminmxid.
func TestAnalyzeVacuumHealth_MultixactWraparoundImminent(t *testing.T) {
	a := vacuumFixture("aging_mxid", 1_000_000, 0, nil)
	a.Planner.DatabaseMxid = 1_700_000_000
	a.Planner.Tables[0].Sizing.RelminMxid = 20_000_000 // age = 1.68B, past failsafe

	vh := AnalyzeVacuumHealth(a)[0]
	if vh.MxidAge != 1_680_000_000 {
		t.Errorf("MxidAge=%d want 1680000000", vh.MxidAge)
	}
	if vh.MultixactFreezeMaxAge != 400_000_000 {
		t.Errorf("MultixactFreezeMaxAge=%d want 400000000", vh.MultixactFreezeMaxAge)
	}
	if vh.MultixactFailsafeAge != 1_600_000_000 {
		t.Errorf("MultixactFailsafeAge=%d want 1600000000", vh.MultixactFailsafeAge)
	}
	if !hasFreezeFinding(vh, CodeMxidAgeHigh, SeverityHigh, "relminmxid") {
		t.Errorf("expected high relminmxid last-resort finding, got %+v", vh.Findings)
	}
}

func hasFreezeFinding(vh VacuumHealth, code VacuumCode, sev Severity, substr string) bool {
	for _, f := range vh.Findings {
		if f.Code == code && f.Severity == sev && strings.Contains(f.Message, substr) {
			return true
		}
	}
	return false
}

// A relfrozenxid age between the routine trigger and 2x it is normal sawtooth and
// must NOT fire; at/over 2x freeze_max_age (falling behind) it's a medium finding.
func TestAnalyzeVacuumHealth_FreezeBandMedium(t *testing.T) {
	// 250M = 1.25x freeze_max_age: routine, no finding.
	a := vacuumFixture("riding", 1_000_000, 0, nil)
	a.Planner.DatabaseXid = 260_000_000
	a.Planner.Tables[0].Sizing.RelfrozenXid = 10_000_000 // age = 250M
	if hasFreezeFinding(AnalyzeVacuumHealth(a)[0], CodeFreezeAgeHigh, SeverityMedium, "") {
		t.Errorf("did not expect a freeze finding at 1.25x freeze_max_age")
	}

	// 450M = 2.25x freeze_max_age: overshot the trigger, falling behind → medium.
	b := vacuumFixture("behind", 1_000_000, 0, nil)
	b.Planner.DatabaseXid = 460_000_000
	b.Planner.Tables[0].Sizing.RelfrozenXid = 10_000_000 // age = 450M
	if !hasFreezeFinding(AnalyzeVacuumHealth(b)[0], CodeFreezeAgeHigh, SeverityMedium, "isn't keeping up") {
		t.Errorf("expected a medium freeze finding at 2.25x freeze_max_age")
	}
}

// Healthy frozen-xid age (within the routine sawtooth, under 2x freeze_max_age) and
// the partitioned-parent case (relfrozenxid 0) must NOT raise a freeze finding, and
// the partitioned case must leave the freeze fields zeroed.
func TestAnalyzeVacuumHealth_WraparoundQuiet(t *testing.T) {
	a := vacuumFixture("young", 1_000_000, 0, nil)
	a.Planner.DatabaseXid = 200_000_000
	a.Planner.DatabaseMxid = 400_000_000
	a.Planner.Tables[0].Sizing.RelfrozenXid = 190_000_000 // xid age = 10M = 5%
	a.Planner.Tables[0].Sizing.RelminMxid = 380_000_000   // mxid age = 20M = 5%

	vh := AnalyzeVacuumHealth(a)[0]
	for _, r := range vh.Recommendations {
		if strings.Contains(r, "anti-wraparound") {
			t.Errorf("did not expect anti-wraparound rec at 5%%, got %v", vh.Recommendations)
		}
	}

	// partitioned parent: relfrozenxid/relminmxid 0 means nothing to age.
	p := vacuumFixture("parent", 1_000_000, 0, nil)
	p.Planner.DatabaseXid = 200_000_000
	p.Planner.DatabaseMxid = 400_000_000
	p.Planner.Tables[0].Sizing.RelfrozenXid = 0
	p.Planner.Tables[0].Sizing.RelminMxid = 0

	pvh := AnalyzeVacuumHealth(p)[0]
	if pvh.XidAge != 0 || pvh.FreezeMaxAge != 0 || pvh.FreezeProgress != 0 {
		t.Errorf("expected zeroed freeze fields for partitioned parent, got %+v", pvh)
	}
	if pvh.MxidAge != 0 || pvh.MultixactFreezeMaxAge != 0 || pvh.MultixactFreezeProgress != 0 {
		t.Errorf("expected zeroed multixact freeze fields for partitioned parent, got %+v", pvh)
	}
}

// The planner doc's settings win: it re-hashes every capture, while the schema digest
// only moves on DDL, so schema GUCs are frozen at the last schema change. A cluster that
// retunes autovacuum in postgresql.conf must not be graded against last month's values.
func TestFreshGUCs_PrefersPlanner(t *testing.T) {
	a := &snapshot.AnnotatedSchema{
		Schema:  &snapshot.SchemaSnapshot{GUCs: []snapshot.GucSetting{{Name: "autovacuum_vacuum_scale_factor", Setting: "0.2"}}},
		Planner: &snapshot.PlannerStatsSnapshot{GUCs: []snapshot.GucSetting{{Name: "autovacuum_vacuum_scale_factor", Setting: "0.05"}}},
	}
	if got := ParseAutovacuumDefaults(freshGUCs(a)).VacuumScaleFactor; got != 0.05 {
		t.Errorf("scale factor = %v, want the planner's 0.05", got)
	}
}

// Planner docs captured before GUCs moved there carry none; fall back to the schema's.
func TestFreshGUCs_FallsBackToSchema(t *testing.T) {
	schemaGUCs := []snapshot.GucSetting{{Name: "autovacuum_vacuum_scale_factor", Setting: "0.2"}}
	for _, planner := range []*snapshot.PlannerStatsSnapshot{nil, {}, {GUCs: []snapshot.GucSetting{}}} {
		a := &snapshot.AnnotatedSchema{Schema: &snapshot.SchemaSnapshot{GUCs: schemaGUCs}, Planner: planner}
		if got := ParseAutovacuumDefaults(freshGUCs(a)).VacuumScaleFactor; got != 0.2 {
			t.Errorf("planner %+v: scale factor = %v, want the schema's 0.2", planner, got)
		}
	}
}
