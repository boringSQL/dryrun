package diff

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

// emptyPlanner builds a minimal-but-valid PlannerStatsSnapshot carrying just the
// bits DiffPlanner keys on: a content hash (so the SnapshotDiff envelope has real
// endpoints to render) and a timestamp. Each test layers in only the table/index
// sizing rows it cares about, which keeps every case focused on one behavior of
// the sizing differ (signed delta, pct, suppression, ordering, ...).
func emptyPlanner(hash string) *snapshot.PlannerStatsSnapshot {
	return &snapshot.PlannerStatsSnapshot{
		Database: "test", Timestamp: time.Now().UTC(), ContentHash: hash,
	}
}

// tbl is a tiny constructor for a table sizing entry. The differ keys tables on
// their qualified name, so we always pass schema+name explicitly; the sizing
// struct carries the row/page/byte counters that become individual SizingDelta
// rows once diffed.
func tbl(schema, name string, s snapshot.TableSizing) snapshot.TableSizingEntry {
	return snapshot.TableSizingEntry{
		Table:  snapshot.QualifiedName{Schema: schema, Name: name},
		Sizing: s,
	}
}

// findSizing scans a planner delta for the SizingDelta on a given object name +
// metric and returns it, failing the test if it is absent. Tests use this rather
// than indexing into the slice because DiffPlanner sorts rows deterministically
// and the positional index of any one metric is an implementation detail we do
// not want the assertions coupled to.
func findSizing(t *testing.T, rows []SizingDelta, name, metric string) SizingDelta {
	t.Helper()
	for _, r := range rows {
		if r.Identity.Name == name && r.Metric == metric {
			return r
		}
	}
	t.Fatalf("expected a %s/%s sizing row, got %+v", name, metric, rows)
	return SizingDelta{}
}

// --- sizing deltas: signed values + percent ---
//
// The core contract: a table whose row/byte counters moved between two planner
// snapshots yields one SizingDelta per metric, with ValueA/ValueB carried
// verbatim, a signed Delta (B-A), and a Pct that is the percent change relative
// to A. This is the raw, foldable shape the cloud engine consumes — no rounding,
// no judgment, just arithmetic.

func TestDiffPlanner_SignedDeltaAndPct(t *testing.T) {
	from := emptyPlanner("a")
	from.Tables = []snapshot.TableSizingEntry{
		tbl("public", "time_entry", snapshot.TableSizing{Reltuples: 1_000_000, TableSize: 100 << 20}),
	}
	to := emptyPlanner("b")
	to.Tables = []snapshot.TableSizingEntry{
		tbl("public", "time_entry", snapshot.TableSizing{Reltuples: 1_138_000, TableSize: 218 << 20}),
	}

	delta, err := DiffPlanner(from, to)
	if err != nil {
		t.Fatalf("DiffPlanner returned an error: %v", err)
	}

	rows := findSizing(t, delta.Sizing, "time_entry", MetricReltuples)
	if rows.ValueA != 1_000_000 || rows.ValueB != 1_138_000 {
		t.Errorf("reltuples values not carried verbatim: %+v", rows)
	}
	if rows.Delta != 138_000 {
		t.Errorf("expected signed delta 138000, got %v", rows.Delta)
	}
	if rows.Pct == nil || math.Abs(*rows.Pct-13.8) > 0.001 {
		t.Errorf("expected pct ~13.8, got %v", rows.Pct)
	}
}

// A shrinking counter must produce a negative delta and a negative percent — the
// sign is load-bearing because the engine distinguishes growth from contraction.
func TestDiffPlanner_NegativeDelta(t *testing.T) {
	from := emptyPlanner("a")
	from.Tables = []snapshot.TableSizingEntry{
		tbl("public", "sessions", snapshot.TableSizing{Reltuples: 2_000}),
	}
	to := emptyPlanner("b")
	to.Tables = []snapshot.TableSizingEntry{
		tbl("public", "sessions", snapshot.TableSizing{Reltuples: 1_500}),
	}

	delta, _ := DiffPlanner(from, to)
	row := findSizing(t, delta.Sizing, "sessions", MetricReltuples)
	if row.Delta != -500 {
		t.Errorf("expected delta -500, got %v", row.Delta)
	}
	if row.Pct == nil || math.Abs(*row.Pct-(-25)) > 0.001 {
		t.Errorf("expected pct -25, got %v", row.Pct)
	}
}

// A table absent from the "from" side has ValueA == 0, which makes percent change
// mathematically undefined (not zero). DiffPlanner must leave Pct nil in that case
// so the engine and the renderer can treat newly-present objects as a distinct
// class rather than as a 0%-or-infinite mover.
func TestDiffPlanner_NewTableHasNilPct(t *testing.T) {
	from := emptyPlanner("a")
	to := emptyPlanner("b")
	to.Tables = []snapshot.TableSizingEntry{
		tbl("public", "audit_log", snapshot.TableSizing{Reltuples: 5_000}),
	}

	delta, _ := DiffPlanner(from, to)
	row := findSizing(t, delta.Sizing, "audit_log", MetricReltuples)
	if row.ValueA != 0 || row.ValueB != 5_000 || row.Delta != 5_000 {
		t.Errorf("new table values wrong: %+v", row)
	}
	if row.Pct != nil {
		t.Errorf("expected nil pct for a table growing from zero, got %v", *row.Pct)
	}
}

// toast_bytes is noise for the overwhelming majority of tables (no TOAST), so the
// differ only emits that metric when at least one side is non-zero. The core
// metrics (reltuples, relpages, table/total/indexes bytes) are always present.
func TestDiffPlanner_ToastOnlyWhenPresent(t *testing.T) {
	from := emptyPlanner("a")
	from.Tables = []snapshot.TableSizingEntry{
		tbl("public", "plain", snapshot.TableSizing{Reltuples: 10}),
		tbl("public", "toasty", snapshot.TableSizing{Reltuples: 10, ToastSize: 4096}),
	}
	to := emptyPlanner("b")
	to.Tables = []snapshot.TableSizingEntry{
		tbl("public", "plain", snapshot.TableSizing{Reltuples: 20}),
		tbl("public", "toasty", snapshot.TableSizing{Reltuples: 20, ToastSize: 8192}),
	}

	delta, _ := DiffPlanner(from, to)
	for _, r := range delta.Sizing {
		if r.Identity.Name == "plain" && r.Metric == MetricToastBytes {
			t.Errorf("plain table should not carry a toast_bytes row: %+v", r)
		}
	}
	// toasty has TOAST on both sides, so the row must exist.
	row := findSizing(t, delta.Sizing, "toasty", MetricToastBytes)
	if row.Delta != 4096 {
		t.Errorf("expected toast delta 4096, got %v", row.Delta)
	}
}

// Indexes are diffed alongside tables, keyed on the index name, and carry their
// own metric set (reltuples, relpages, index_bytes). The Identity.Kind must say
// "index" so the engine and renderer can label the object correctly.
func TestDiffPlanner_IndexRows(t *testing.T) {
	from := emptyPlanner("a")
	from.Indexes = []snapshot.IndexSizingEntry{{
		Table:  snapshot.QualifiedName{Schema: "public", Name: "time_entry"},
		Index:  "time_entry_pkey",
		Sizing: snapshot.IndexSizing{Reltuples: 1_000_000, Relpages: 2740, Size: 22 << 20},
	}}
	to := emptyPlanner("b")
	to.Indexes = []snapshot.IndexSizingEntry{{
		Table:  snapshot.QualifiedName{Schema: "public", Name: "time_entry"},
		Index:  "time_entry_pkey",
		Sizing: snapshot.IndexSizing{Reltuples: 1_138_000, Relpages: 3100, Size: 25 << 20},
	}}

	delta, _ := DiffPlanner(from, to)
	row := findSizing(t, delta.Sizing, "time_entry_pkey", MetricIndexBytes)
	if row.Identity.Kind != "index" {
		t.Errorf("expected index kind, got %q", row.Identity.Kind)
	}
	if row.Delta != (25<<20)-(22<<20) {
		t.Errorf("expected index_bytes delta of 3 MiB, got %v", row.Delta)
	}
}

// DiffPlanner must order rows deterministically (kind, schema, name, then a fixed
// metric order) because the cloud dedups on the serialized delta — a non-stable
// order would make identical diffs hash differently. We assert the row sequence
// is non-decreasing under the same comparison the differ uses.
func TestDiffPlanner_DeterministicOrder(t *testing.T) {
	from := emptyPlanner("a")
	from.Tables = []snapshot.TableSizingEntry{
		tbl("public", "zebra", snapshot.TableSizing{Reltuples: 1}),
		tbl("public", "alpha", snapshot.TableSizing{Reltuples: 1}),
	}
	to := emptyPlanner("b")
	to.Tables = []snapshot.TableSizingEntry{
		tbl("public", "zebra", snapshot.TableSizing{Reltuples: 2}),
		tbl("public", "alpha", snapshot.TableSizing{Reltuples: 2}),
	}

	delta, _ := DiffPlanner(from, to)
	var lastName string
	for _, r := range delta.Sizing {
		if r.Identity.Name < lastName {
			t.Fatalf("rows not sorted by name: %q came after %q", r.Identity.Name, lastName)
		}
		if r.Identity.Name != lastName {
			lastName = r.Identity.Name
		}
	}
}

// The JSON form is the stable contract the cloud imports, so it must keep every
// row — including sub-threshold ones the console suppresses — and round-trip
// without losing the signed delta or the nullable pct.
func TestDiffPlanner_JSONKeepsAllRows(t *testing.T) {
	from := emptyPlanner("a")
	from.Tables = []snapshot.TableSizingEntry{
		tbl("public", "barely", snapshot.TableSizing{Reltuples: 100_000}),
	}
	to := emptyPlanner("b")
	to.Tables = []snapshot.TableSizingEntry{
		tbl("public", "barely", snapshot.TableSizing{Reltuples: 100_001}), // +0.001%, way under any threshold
	}

	delta, _ := DiffPlanner(from, to)
	raw, err := json.Marshal(delta)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var out PlannerDelta
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(out.Sizing) != len(delta.Sizing) {
		t.Errorf("JSON dropped rows: %d in, %d out", len(delta.Sizing), len(out.Sizing))
	}
	row := findSizing(t, out.Sizing, "barely", MetricReltuples)
	if row.Delta != 1 || row.Pct == nil {
		t.Errorf("round-trip lost delta/pct fidelity: %+v", row)
	}
}

// --- console honesty ---
//
// The console renderer is where the "honesty" lives: it must drop zero and
// sub-threshold rows, group surviving rows under their object, and lead with a
// "N objects changed, top movers:" headline. None of this touches the JSON
// contract — it is purely a human-facing presentation concern.

func TestRenderPlannerConsole_SuppressesSubThreshold(t *testing.T) {
	from := emptyPlanner("aaaaaaaaaaaa")
	from.Tables = []snapshot.TableSizingEntry{
		tbl("public", "big_mover", snapshot.TableSizing{Reltuples: 1_000_000}),
		tbl("public", "noise", snapshot.TableSizing{Reltuples: 100_000}),
	}
	to := emptyPlanner("bbbbbbbbbbbb")
	to.Tables = []snapshot.TableSizingEntry{
		tbl("public", "big_mover", snapshot.TableSizing{Reltuples: 1_500_000}), // +50%
		tbl("public", "noise", snapshot.TableSizing{Reltuples: 100_050}),       // +0.05%, below default 1%
	}

	delta, _ := DiffPlanner(from, to)
	env := &SnapshotDiff{Kind: "planner", FromHash: from.ContentHash, ToHash: to.ContentHash, Planner: delta}

	var buf bytes.Buffer
	RenderPlannerConsole(&buf, env, DefaultMinPct)
	out := buf.String()

	if !strings.Contains(out, "big_mover") {
		t.Errorf("expected the 50%% mover to be shown, got:\n%s", out)
	}
	if strings.Contains(out, "noise") {
		t.Errorf("expected the 0.05%% row to be suppressed, got:\n%s", out)
	}
	if !strings.Contains(out, "1 object changed, top movers:") {
		t.Errorf("expected the top-movers headline, got:\n%s", out)
	}
}

// Newly-present objects (Pct nil, ValueA 0) are the biggest possible movers, so
// they must survive suppression and sort ahead of any finite-percent row.
func TestRenderPlannerConsole_NewObjectSortsFirst(t *testing.T) {
	from := emptyPlanner("aaaaaaaaaaaa")
	from.Tables = []snapshot.TableSizingEntry{
		tbl("public", "existing", snapshot.TableSizing{Reltuples: 1_000_000}),
	}
	to := emptyPlanner("bbbbbbbbbbbb")
	to.Tables = []snapshot.TableSizingEntry{
		tbl("public", "existing", snapshot.TableSizing{Reltuples: 1_100_000}), // +10%
		tbl("public", "brand_new", snapshot.TableSizing{Reltuples: 5_000}),    // from zero, pct nil
	}

	delta, _ := DiffPlanner(from, to)
	env := &SnapshotDiff{Kind: "planner", FromHash: from.ContentHash, ToHash: to.ContentHash, Planner: delta}

	var buf bytes.Buffer
	RenderPlannerConsole(&buf, env, DefaultMinPct)
	out := buf.String()

	newIdx := strings.Index(out, "brand_new")
	existingIdx := strings.Index(out, "existing")
	if newIdx < 0 || existingIdx < 0 {
		t.Fatalf("expected both objects in output, got:\n%s", out)
	}
	if newIdx > existingIdx {
		t.Errorf("expected the newly-present table to sort first, got:\n%s", out)
	}
}

// When nothing clears the threshold the renderer says so explicitly rather than
// printing an empty movers list — the empty state still teaches the user that a
// diff happened, just below the noise floor.
func TestRenderPlannerConsole_NoMovers(t *testing.T) {
	from := emptyPlanner("aaaaaaaaaaaa")
	from.Tables = []snapshot.TableSizingEntry{
		tbl("public", "steady", snapshot.TableSizing{Reltuples: 1_000_000}),
	}
	to := emptyPlanner("bbbbbbbbbbbb")
	to.Tables = []snapshot.TableSizingEntry{
		tbl("public", "steady", snapshot.TableSizing{Reltuples: 1_000_100}), // +0.01%
	}

	delta, _ := DiffPlanner(from, to)
	env := &SnapshotDiff{Kind: "planner", FromHash: from.ContentHash, ToHash: to.ContentHash, Planner: delta}

	var buf bytes.Buffer
	RenderPlannerConsole(&buf, env, DefaultMinPct)
	if !strings.Contains(buf.String(), "no movers") {
		t.Errorf("expected a 'no movers' line, got:\n%s", buf.String())
	}
}

// RenderConsole is the single dispatch entry point; given a planner envelope it
// must route to the planner renderer (not the schema "no renderer yet" fallback).
func TestRenderConsole_DispatchesPlanner(t *testing.T) {
	from := emptyPlanner("aaaaaaaaaaaa")
	from.Tables = []snapshot.TableSizingEntry{tbl("public", "t", snapshot.TableSizing{Reltuples: 100})}
	to := emptyPlanner("bbbbbbbbbbbb")
	to.Tables = []snapshot.TableSizingEntry{tbl("public", "t", snapshot.TableSizing{Reltuples: 200})}

	delta, _ := DiffPlanner(from, to)
	env := &SnapshotDiff{Kind: "planner", FromHash: from.ContentHash, ToHash: to.ContentHash, Planner: delta}

	var buf bytes.Buffer
	RenderConsole(&buf, env)
	if !strings.Contains(buf.String(), "planner diff") {
		t.Errorf("expected RenderConsole to route to the planner renderer, got:\n%s", buf.String())
	}
}
