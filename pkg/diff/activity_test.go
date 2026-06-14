package diff

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

// emptyActivity builds a minimal ActivityStatsSnapshot carrying just the content
// hash the SnapshotDiff envelope renders from. Each test layers in only the
// table/index activity rows it needs, keeping every case focused on one behavior
// of the counter differ (signed delta, reset-awareness, gauges, ...).
func emptyActivity(hash string) *snapshot.ActivityStatsSnapshot {
	return &snapshot.ActivityStatsSnapshot{ContentHash: hash}
}

// tblAct / idxAct are tiny constructors for activity entries. The differ keys
// tables on their qualified name and indexes on table+index, so we always pass
// those explicitly; the activity struct carries the cumulative counters and
// point-in-time gauges that become individual CounterDelta rows once diffed.
func tblAct(schema, name string, a snapshot.TableActivity) snapshot.TableActivityEntry {
	return snapshot.TableActivityEntry{
		Table:    snapshot.QualifiedName{Schema: schema, Name: name},
		Activity: a,
	}
}

func idxAct(schema, table, index string, a snapshot.IndexActivity) snapshot.IndexActivityEntry {
	return snapshot.IndexActivityEntry{
		Table:    snapshot.QualifiedName{Schema: schema, Name: table},
		Index:    index,
		Activity: a,
	}
}

// findCounter scans an activity delta for the CounterDelta on a given object name
// + metric, failing the test if it is absent. Tests use this rather than indexing
// into the slice because DiffActivity sorts rows deterministically and we do not
// want assertions coupled to the positional index of any one metric.
func findCounter(t *testing.T, rows []CounterDelta, name, metric string) CounterDelta {
	t.Helper()
	for _, r := range rows {
		if r.Identity.Name == name && r.Metric == metric {
			return r
		}
	}
	t.Fatalf("expected a %s/%s counter row, got %+v", name, metric, rows)
	return CounterDelta{}
}

// --- counter deltas: signed values + percent ---
//
// A cumulative counter that grew between two activity snapshots yields a
// CounterDelta with values carried verbatim, a signed Delta (B-A) via a non-nil
// pointer, and a Pct relative to A. This is the same foldable shape as sizing —
// the engine integrates these across a series into scan-rate trends.
func TestDiffActivity_SignedDeltaAndPct(t *testing.T) {
	from := emptyActivity("a")
	from.Tables = []snapshot.TableActivityEntry{
		tblAct("public", "time_entry", snapshot.TableActivity{SeqScan: 100, IdxScan: 1_000}),
	}
	to := emptyActivity("b")
	to.Tables = []snapshot.TableActivityEntry{
		tblAct("public", "time_entry", snapshot.TableActivity{SeqScan: 150, IdxScan: 1_500}),
	}

	delta, err := DiffActivity(from, to)
	if err != nil {
		t.Fatalf("DiffActivity returned an error: %v", err)
	}

	row := findCounter(t, delta.Counters, "time_entry", MetricIdxScan)
	if row.ValueA != 1_000 || row.ValueB != 1_500 {
		t.Errorf("idx_scan values not carried verbatim: %+v", row)
	}
	if row.Delta == nil || *row.Delta != 500 {
		t.Errorf("expected signed delta 500, got %v", row.Delta)
	}
	if row.Pct == nil || math.Abs(*row.Pct-50) > 1e-9 {
		t.Errorf("expected pct 50, got %v", row.Pct)
	}
	if row.ResetBetween {
		t.Errorf("a growing counter must not be flagged as reset: %+v", row)
	}
}

// The load-bearing honesty fix: pg_stat_* counters are cumulative and only ever
// climb, so a *decrease* means pg_stat_reset / failover happened between the two
// captures (the real toggl case: time_entry_constraints idx_scan 581,434 -> 18).
// The delta across a reset is meaningless, so the differ must set ResetBetween
// and omit Delta/Pct rather than print a nonsensical -581,416.
func TestDiffActivity_ResetOmitsDelta(t *testing.T) {
	from := emptyActivity("a")
	from.Tables = []snapshot.TableActivityEntry{
		tblAct("public", "time_entry_constraints", snapshot.TableActivity{IdxScan: 581_434, SeqScan: 9_000}),
	}
	to := emptyActivity("b")
	to.Tables = []snapshot.TableActivityEntry{
		tblAct("public", "time_entry_constraints", snapshot.TableActivity{IdxScan: 18, SeqScan: 12}),
	}

	delta, _ := DiffActivity(from, to)
	row := findCounter(t, delta.Counters, "time_entry_constraints", MetricIdxScan)
	if !row.ResetBetween {
		t.Errorf("expected reset_between for a rolled-back counter, got %+v", row)
	}
	if row.Delta != nil {
		t.Errorf("expected delta omitted across a reset, got %v", *row.Delta)
	}
	if row.Pct != nil {
		t.Errorf("expected pct omitted across a reset, got %v", *row.Pct)
	}
	// The values themselves are still carried so the reader sees the rollback.
	if row.ValueA != 581_434 || row.ValueB != 18 {
		t.Errorf("expected the raw before/after values to be preserved, got %+v", row)
	}
}

// Reset is detected per object from *any* decreased cumulative counter, and the
// whole object's cumulative rows are flagged — once stats were reset, none of
// that table's cumulative deltas are trustworthy, even the ones that happen to
// show an increase.
func TestDiffActivity_ResetFlagsAllCumulativeRows(t *testing.T) {
	from := emptyActivity("a")
	from.Tables = []snapshot.TableActivityEntry{
		tblAct("public", "t", snapshot.TableActivity{IdxScan: 1_000, SeqScan: 5, NTupIns: 200}),
	}
	to := emptyActivity("b")
	// idx_scan dropped (reset), but seq_scan and n_tup_ins read higher post-reset.
	to.Tables = []snapshot.TableActivityEntry{
		tblAct("public", "t", snapshot.TableActivity{IdxScan: 3, SeqScan: 9, NTupIns: 240}),
	}

	delta, _ := DiffActivity(from, to)
	for _, m := range []string{MetricIdxScan, MetricSeqScan, MetricNTupIns} {
		row := findCounter(t, delta.Counters, "t", m)
		if !row.ResetBetween || row.Delta != nil {
			t.Errorf("expected %s flagged reset with delta omitted, got %+v", m, row)
		}
	}
}

// n_live_tup / n_dead_tup are point-in-time gauges, not cumulative counters: they
// legitimately fall (a VACUUM clears dead tuples). A gauge decrease must NOT be
// read as a reset, and the gauge must keep its honest signed delta even when the
// table's cumulative counters did reset.
func TestDiffActivity_GaugesKeepDeltaAndDoNotTriggerReset(t *testing.T) {
	from := emptyActivity("a")
	from.Tables = []snapshot.TableActivityEntry{
		tblAct("public", "t", snapshot.TableActivity{IdxScan: 100, NDeadTup: 5_000, NLiveTup: 1_000}),
	}
	to := emptyActivity("b")
	to.Tables = []snapshot.TableActivityEntry{
		tblAct("public", "t", snapshot.TableActivity{IdxScan: 120, NDeadTup: 100, NLiveTup: 1_200}),
	}

	delta, _ := DiffActivity(from, to)

	// A falling dead-tuple gauge alone is not a reset, so idx_scan stays a real delta.
	idx := findCounter(t, delta.Counters, "t", MetricIdxScan)
	if idx.ResetBetween || idx.Delta == nil || *idx.Delta != 20 {
		t.Errorf("expected a clean idx_scan delta of 20, got %+v", idx)
	}
	dead := findCounter(t, delta.Counters, "t", MetricNDeadTup)
	if dead.ResetBetween {
		t.Errorf("a gauge must never be flagged reset: %+v", dead)
	}
	if dead.Delta == nil || *dead.Delta != -4_900 {
		t.Errorf("expected dead-tuple gauge delta -4900, got %v", dead.Delta)
	}
}

// Even when the table's cumulative counters reset, the gauges keep their deltas:
// the gauge value is a current-state estimate, comparable across the reset.
func TestDiffActivity_GaugeDeltaSurvivesReset(t *testing.T) {
	from := emptyActivity("a")
	from.Tables = []snapshot.TableActivityEntry{
		tblAct("public", "t", snapshot.TableActivity{IdxScan: 9_000, NDeadTup: 100}),
	}
	to := emptyActivity("b")
	to.Tables = []snapshot.TableActivityEntry{
		tblAct("public", "t", snapshot.TableActivity{IdxScan: 5, NDeadTup: 400}), // idx_scan reset, dead grew
	}

	delta, _ := DiffActivity(from, to)
	dead := findCounter(t, delta.Counters, "t", MetricNDeadTup)
	if dead.ResetBetween || dead.Delta == nil || *dead.Delta != 300 {
		t.Errorf("expected dead-tuple gauge to keep its delta of 300 across a reset, got %+v", dead)
	}
}

// Indexes carry their own cumulative counters and their own reset detection: a
// dropped idx_scan on an index is a reset just as on a table.
func TestDiffActivity_IndexReset(t *testing.T) {
	from := emptyActivity("a")
	from.Indexes = []snapshot.IndexActivityEntry{
		idxAct("public", "time_entry", "time_entry_pkey", snapshot.IndexActivity{IdxScan: 50_000}),
	}
	to := emptyActivity("b")
	to.Indexes = []snapshot.IndexActivityEntry{
		idxAct("public", "time_entry", "time_entry_pkey", snapshot.IndexActivity{IdxScan: 7}),
	}

	delta, _ := DiffActivity(from, to)
	row := findCounter(t, delta.Counters, "time_entry_pkey", MetricIdxScan)
	if row.Identity.Kind != "index" {
		t.Errorf("expected index kind, got %q", row.Identity.Kind)
	}
	if !row.ResetBetween || row.Delta != nil {
		t.Errorf("expected the index counter flagged reset with delta omitted, got %+v", row)
	}
}

// A table present only on the "to" side has ValueA == 0, so percent change is
// undefined (not zero) and Pct stays nil — and a climb from zero is not a reset.
func TestDiffActivity_NewTableHasNilPct(t *testing.T) {
	from := emptyActivity("a")
	to := emptyActivity("b")
	to.Tables = []snapshot.TableActivityEntry{
		tblAct("public", "audit_log", snapshot.TableActivity{SeqScan: 42}),
	}

	delta, _ := DiffActivity(from, to)
	row := findCounter(t, delta.Counters, "audit_log", MetricSeqScan)
	if row.ResetBetween {
		t.Errorf("a newly-present table is not a reset: %+v", row)
	}
	if row.Delta == nil || *row.Delta != 42 {
		t.Errorf("expected delta 42, got %v", row.Delta)
	}
	if row.Pct != nil {
		t.Errorf("expected nil pct for a counter growing from zero, got %v", *row.Pct)
	}
}

// The JSON form is the contract the cloud imports: reset_between must serialize,
// the omitted delta must stay omitted, and a normal signed delta must round-trip.
func TestDiffActivity_JSONPreservesResetAndDelta(t *testing.T) {
	from := emptyActivity("a")
	from.Tables = []snapshot.TableActivityEntry{
		tblAct("public", "grew", snapshot.TableActivity{IdxScan: 100}),
		tblAct("public", "wasreset", snapshot.TableActivity{IdxScan: 9_000}),
	}
	to := emptyActivity("b")
	to.Tables = []snapshot.TableActivityEntry{
		tblAct("public", "grew", snapshot.TableActivity{IdxScan: 250}),
		tblAct("public", "wasreset", snapshot.TableActivity{IdxScan: 3}),
	}

	delta, _ := DiffActivity(from, to)
	raw, err := json.Marshal(delta)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var out ActivityDelta
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	grew := findCounter(t, out.Counters, "grew", MetricIdxScan)
	if grew.Delta == nil || *grew.Delta != 150 || grew.ResetBetween {
		t.Errorf("round-trip lost the clean delta: %+v", grew)
	}
	reset := findCounter(t, out.Counters, "wasreset", MetricIdxScan)
	if !reset.ResetBetween || reset.Delta != nil {
		t.Errorf("round-trip lost reset_between / delta omission: %+v", reset)
	}
}

// --- console honesty ---

// A reset row is signal, not noise: the renderer surfaces it with an explicit
// "reset between" note rather than suppressing it or printing a phantom drop.
func TestRenderActivityConsole_ShowsResetNote(t *testing.T) {
	from := emptyActivity("aaaaaaaaaaaa")
	from.Tables = []snapshot.TableActivityEntry{
		tblAct("public", "constraints", snapshot.TableActivity{IdxScan: 581_434}),
	}
	to := emptyActivity("bbbbbbbbbbbb")
	to.Tables = []snapshot.TableActivityEntry{
		tblAct("public", "constraints", snapshot.TableActivity{IdxScan: 18}),
	}

	delta, _ := DiffActivity(from, to)
	env := &SnapshotDiff{Kind: "activity", FromHash: from.ContentHash, ToHash: to.ContentHash, Activity: delta}

	var buf bytes.Buffer
	RenderActivityConsole(&buf, env, DefaultMinPct)
	out := buf.String()
	if !strings.Contains(out, "reset between") {
		t.Errorf("expected a reset-between note, got:\n%s", out)
	}
	if !strings.Contains(out, "constraints") {
		t.Errorf("expected the reset object to be shown, got:\n%s", out)
	}
}

// Sub-threshold movers are suppressed in the console just like sizing, while a
// big mover survives and drives the top-movers headline.
func TestRenderActivityConsole_SuppressesSubThreshold(t *testing.T) {
	from := emptyActivity("aaaaaaaaaaaa")
	from.Tables = []snapshot.TableActivityEntry{
		tblAct("public", "hot", snapshot.TableActivity{IdxScan: 1_000}),
		tblAct("public", "quiet", snapshot.TableActivity{IdxScan: 100_000}),
	}
	to := emptyActivity("bbbbbbbbbbbb")
	to.Tables = []snapshot.TableActivityEntry{
		tblAct("public", "hot", snapshot.TableActivity{IdxScan: 5_000}),     // +400%
		tblAct("public", "quiet", snapshot.TableActivity{IdxScan: 100_050}), // +0.05%
	}

	delta, _ := DiffActivity(from, to)
	env := &SnapshotDiff{Kind: "activity", FromHash: from.ContentHash, ToHash: to.ContentHash, Activity: delta}

	var buf bytes.Buffer
	RenderActivityConsole(&buf, env, DefaultMinPct)
	out := buf.String()
	if !strings.Contains(out, "hot") {
		t.Errorf("expected the big mover to be shown, got:\n%s", out)
	}
	if strings.Contains(out, "quiet") {
		t.Errorf("expected the 0.05%% row to be suppressed, got:\n%s", out)
	}
}

// RenderConsole is the single dispatch entry point; an activity envelope must
// route to the activity renderer, not the schema fallback or the planner branch.
func TestRenderConsole_DispatchesActivity(t *testing.T) {
	from := emptyActivity("aaaaaaaaaaaa")
	from.Tables = []snapshot.TableActivityEntry{tblAct("public", "t", snapshot.TableActivity{IdxScan: 100})}
	to := emptyActivity("bbbbbbbbbbbb")
	to.Tables = []snapshot.TableActivityEntry{tblAct("public", "t", snapshot.TableActivity{IdxScan: 200})}

	delta, _ := DiffActivity(from, to)
	env := &SnapshotDiff{Kind: "activity", FromHash: from.ContentHash, ToHash: to.ContentHash, Activity: delta}

	var buf bytes.Buffer
	RenderConsole(&buf, env)
	if !strings.Contains(buf.String(), "activity diff") {
		t.Errorf("expected RenderConsole to route to the activity renderer, got:\n%s", buf.String())
	}
}
