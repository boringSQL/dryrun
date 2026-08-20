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

// bloat is a derived estimate (kept out of the content hash) that the differ
// surfaces as a bloat_ratio row only when at least one endpoint carries an
// estimate — non-btree and pre-ANALYZE entries leave Bloat nil and must produce
// no bloat row at all. A table whose estimated ratio climbed between snapshots is
// exactly the signal an operator wants out of a diff: the heap is rotting even if
// the row count barely moved.
func TestDiffPlanner_TableBloatRow(t *testing.T) {
	from := emptyPlanner("a")
	from.Tables = []snapshot.TableSizingEntry{{
		Table:  snapshot.QualifiedName{Schema: "public", Name: "time_entry"},
		Sizing: snapshot.TableSizing{Reltuples: 1_000_000, Relpages: 20_000},
		Bloat:  &snapshot.BloatEstimate{BloatRatio: 1.4, ExpectedPages: 14_285, ActualPages: 20_000},
	}}
	to := emptyPlanner("b")
	to.Tables = []snapshot.TableSizingEntry{{
		Table:  snapshot.QualifiedName{Schema: "public", Name: "time_entry"},
		Sizing: snapshot.TableSizing{Reltuples: 1_000_000, Relpages: 42_000},
		Bloat:  &snapshot.BloatEstimate{BloatRatio: 2.94, ExpectedPages: 14_285, ActualPages: 42_000},
	}}

	delta, _ := DiffPlanner(from, to)
	row := findSizing(t, delta.Sizing, "time_entry", MetricBloatRatio)
	if row.Identity.Kind != "table" {
		t.Errorf("expected table kind, got %q", row.Identity.Kind)
	}
	if row.ValueA != 1.4 || row.ValueB != 2.94 {
		t.Errorf("bloat ratios not carried verbatim: %+v", row)
	}
	if math.Abs(row.Delta-1.54) > 1e-9 {
		t.Errorf("expected bloat delta 1.54, got %v", row.Delta)
	}
}

// No bloat estimate on either side means no bloat_ratio row — the differ must not
// invent a 0 → 0 row for the (common) entries Postgres can't estimate.
func TestDiffPlanner_NoBloatRowWhenAbsent(t *testing.T) {
	from := emptyPlanner("a")
	from.Tables = []snapshot.TableSizingEntry{tbl("public", "plain", snapshot.TableSizing{Reltuples: 10})}
	to := emptyPlanner("b")
	to.Tables = []snapshot.TableSizingEntry{tbl("public", "plain", snapshot.TableSizing{Reltuples: 20})}

	delta, _ := DiffPlanner(from, to)
	for _, r := range delta.Sizing {
		if r.Metric == MetricBloatRatio {
			t.Errorf("expected no bloat_ratio row when neither side estimates bloat, got %+v", r)
		}
	}
}

// An index that gains a bloat estimate (nil → value) surfaces a bloat_ratio row
// from a zero baseline, mirroring how newly-present sizing metrics behave, and the
// console renders the ratio with an "x" suffix rather than a humanized count.
func TestRenderPlannerConsole_IndexBloatRendersRatio(t *testing.T) {
	from := emptyPlanner("aaaaaaaaaaaa")
	from.Indexes = []snapshot.IndexSizingEntry{{
		Table:  snapshot.QualifiedName{Schema: "public", Name: "time_entry"},
		Index:  "time_entry_pkey",
		Sizing: snapshot.IndexSizing{Reltuples: 1_000_000, Relpages: 2_740},
		Bloat:  &snapshot.BloatEstimate{BloatRatio: 1.1},
	}}
	to := emptyPlanner("bbbbbbbbbbbb")
	to.Indexes = []snapshot.IndexSizingEntry{{
		Table:  snapshot.QualifiedName{Schema: "public", Name: "time_entry"},
		Index:  "time_entry_pkey",
		Sizing: snapshot.IndexSizing{Reltuples: 1_000_000, Relpages: 9_900},
		Bloat:  &snapshot.BloatEstimate{BloatRatio: 4.0},
	}}

	delta, _ := DiffPlanner(from, to)
	row := findSizing(t, delta.Sizing, "time_entry_pkey", MetricBloatRatio)
	if row.Identity.Kind != "index" {
		t.Errorf("expected index kind, got %q", row.Identity.Kind)
	}

	env := &SnapshotDiff{Kind: "planner", FromHash: from.ContentHash, ToHash: to.ContentHash, Planner: delta}
	var buf bytes.Buffer
	RenderPlannerConsole(&buf, env, DefaultMinPct)
	out := buf.String()
	if !strings.Contains(out, "bloat_ratio") {
		t.Errorf("expected a bloat_ratio mover, got:\n%s", out)
	}
	if !strings.Contains(out, "1.10x") || !strings.Contains(out, "4.00x") {
		t.Errorf("expected ratios rendered with an x suffix, got:\n%s", out)
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
	RenderConsoleMinPct(&buf, env, DefaultMinPct)
	if !strings.Contains(buf.String(), "planner diff") {
		t.Errorf("expected RenderConsole to route to the planner renderer, got:\n%s", buf.String())
	}
}

// fp returns a pointer to a float64 literal; pg_stats fields are all nullable
// (*float64), and the column-stats tests need to express "stat is present with
// value X" versus "stat is absent" (nil) on each side of the diff.
func fp(v float64) *float64 { return &v }

// col builds a single column-stats entry keyed on its qualified table + column.
// The differ joins from/to columns on this key and only emits a StatDelta when
// both sides carry the same stat, so tests pass the table+column explicitly.
func col(schema, table, column string, s snapshot.ColumnStats) snapshot.ColumnStatsEntry {
	return snapshot.ColumnStatsEntry{
		Table:  snapshot.QualifiedName{Schema: schema, Name: table},
		Column: column,
		Stats:  s,
	}
}

// --- column-stats deltas (D4) ---
//
// pg_stats.n_distinct is encoded two ways: an absolute count when >= 0, or a
// negative ratio-of-reltuples when < 0 (e.g. -0.5 means "half the rows are
// distinct"). The same column can flip representation between two snapshots as
// the row count crosses Postgres' heuristic boundary. DiffPlanner must resolve
// both sides to an absolute count (via the table's reltuples) before subtracting,
// otherwise a representation flip injects a phantom delta — the toggl case where
// tags.deleted_at went 2221 → -0.0276 and a naive diff reads it as ~-2221.
func TestDiffPlanner_NDistinctNormalizedAcrossEncodingFlip(t *testing.T) {
	from := emptyPlanner("a")
	from.Tables = []snapshot.TableSizingEntry{tbl("public", "tags", snapshot.TableSizing{Reltuples: 80_000})}
	from.Columns = []snapshot.ColumnStatsEntry{
		col("public", "tags", "deleted_at", snapshot.ColumnStats{NDistinct: fp(2221)}), // absolute count
	}
	to := emptyPlanner("b")
	to.Tables = []snapshot.TableSizingEntry{tbl("public", "tags", snapshot.TableSizing{Reltuples: 80_000})}
	to.Columns = []snapshot.ColumnStatsEntry{
		col("public", "tags", "deleted_at", snapshot.ColumnStats{NDistinct: fp(-0.0276)}), // ratio: 0.0276 * 80000 = 2208
	}

	delta, _ := DiffPlanner(from, to)
	row := findSizing(t, delta.Stats, "tags.deleted_at", MetricNDistinct)
	if math.Abs(row.ValueA-2221) > 0.001 {
		t.Errorf("expected ValueA resolved to 2221, got %v", row.ValueA)
	}
	if math.Abs(row.ValueB-2208) > 0.001 {
		t.Errorf("expected ValueB resolved to ~2208 (0.0276*80000), got %v", row.ValueB)
	}
	// The honest delta is a tiny -13, not the ~-2221 a naive subtraction would yield.
	if math.Abs(row.Delta-(-13)) > 0.5 {
		t.Errorf("expected a small normalized delta near -13, got %v", row.Delta)
	}
}

// A negative n_distinct on both sides is still a ratio: with reltuples doubling
// and the ratio held constant, the absolute distinct count must double too.
func TestDiffPlanner_NDistinctRatioScalesWithReltuples(t *testing.T) {
	from := emptyPlanner("a")
	from.Tables = []snapshot.TableSizingEntry{tbl("public", "events", snapshot.TableSizing{Reltuples: 1_000})}
	from.Columns = []snapshot.ColumnStatsEntry{
		col("public", "events", "kind", snapshot.ColumnStats{NDistinct: fp(-0.5)}), // 500 distinct
	}
	to := emptyPlanner("b")
	to.Tables = []snapshot.TableSizingEntry{tbl("public", "events", snapshot.TableSizing{Reltuples: 2_000})}
	to.Columns = []snapshot.ColumnStatsEntry{
		col("public", "events", "kind", snapshot.ColumnStats{NDistinct: fp(-0.5)}), // 1000 distinct
	}

	delta, _ := DiffPlanner(from, to)
	row := findSizing(t, delta.Stats, "events.kind", MetricNDistinct)
	if row.ValueA != 500 || row.ValueB != 1_000 || row.Delta != 500 {
		t.Errorf("expected 500 -> 1000 (delta 500), got %+v", row)
	}
}

// null_frac and correlation are plain bounded floats; their deltas are the raw
// signed difference, and the Identity must be a "column" kind so the engine can
// attribute distribution drift to the right object.
func TestDiffPlanner_NullFracAndCorrelation(t *testing.T) {
	from := emptyPlanner("a")
	from.Columns = []snapshot.ColumnStatsEntry{
		col("public", "users", "email", snapshot.ColumnStats{NullFrac: fp(0.0), Correlation: fp(0.95)}),
	}
	to := emptyPlanner("b")
	to.Columns = []snapshot.ColumnStatsEntry{
		col("public", "users", "email", snapshot.ColumnStats{NullFrac: fp(0.2), Correlation: fp(-0.10)}),
	}

	delta, _ := DiffPlanner(from, to)

	nf := findSizing(t, delta.Stats, "users.email", MetricNullFrac)
	if math.Abs(nf.Delta-0.2) > 1e-9 {
		t.Errorf("expected null_frac delta 0.2, got %v", nf.Delta)
	}
	if nf.Identity.Kind != "column" {
		t.Errorf("expected column kind, got %q", nf.Identity.Kind)
	}
	corr := findSizing(t, delta.Stats, "users.email", MetricCorrelation)
	if math.Abs(corr.Delta-(-1.05)) > 1e-9 {
		t.Errorf("expected correlation delta -1.05 (0.95 -> -0.10), got %v", corr.Delta)
	}
}

// A StatDelta needs the stat present on both sides; a column that gains a stat
// it never had (nil -> value) produces no row, since there is no honest baseline
// to subtract. One-sided physical presence is the sizing channel's concern.
func TestDiffPlanner_StatNeedsBothSides(t *testing.T) {
	from := emptyPlanner("a")
	from.Columns = []snapshot.ColumnStatsEntry{
		col("public", "t", "c", snapshot.ColumnStats{NullFrac: fp(0.1)}), // no correlation on the from side
	}
	to := emptyPlanner("b")
	to.Columns = []snapshot.ColumnStatsEntry{
		col("public", "t", "c", snapshot.ColumnStats{NullFrac: fp(0.1), Correlation: fp(0.5)}),
	}

	delta, _ := DiffPlanner(from, to)
	for _, r := range delta.Stats {
		if r.Metric == MetricCorrelation {
			t.Errorf("expected no correlation row when one side lacks it, got %+v", r)
		}
	}
}

// most_common_vals has no meaningful arithmetic delta, so the differ reports
// churn: Delta is the set turnover (members that entered or left) and Pct is the
// churn fraction. Here {a,b,c} -> {b,c,d} drops "a" and adds "d": union 4,
// intersection 2, turnover 2, churn 50%.
func TestDiffPlanner_MCVChurn(t *testing.T) {
	from := emptyPlanner("a")
	from.Columns = []snapshot.ColumnStatsEntry{
		col("public", "t", "status", snapshot.ColumnStats{MostCommonVals: strp("{a,b,c}")}),
	}
	to := emptyPlanner("b")
	to.Columns = []snapshot.ColumnStatsEntry{
		col("public", "t", "status", snapshot.ColumnStats{MostCommonVals: strp("{b,c,d}")}),
	}

	delta, _ := DiffPlanner(from, to)
	row := findSizing(t, delta.Stats, "t.status", MetricMCVChurn)
	if row.Delta != 2 {
		t.Errorf("expected turnover 2, got %v", row.Delta)
	}
	if row.Pct == nil || math.Abs(*row.Pct-50) > 1e-9 {
		t.Errorf("expected 50%% churn, got %v", row.Pct)
	}
}

// Identical most_common_vals is zero churn — and a quoted element containing a
// comma must be treated as a single member, not split on the inner comma.
func TestDiffPlanner_MCVChurnZeroWithQuotedElement(t *testing.T) {
	from := emptyPlanner("a")
	from.Columns = []snapshot.ColumnStatsEntry{
		col("public", "t", "label", snapshot.ColumnStats{MostCommonVals: strp(`{"a,b",c}`)}),
	}
	to := emptyPlanner("b")
	to.Columns = []snapshot.ColumnStatsEntry{
		col("public", "t", "label", snapshot.ColumnStats{MostCommonVals: strp(`{"a,b",c}`)}),
	}

	delta, _ := DiffPlanner(from, to)
	row := findSizing(t, delta.Stats, "t.label", MetricMCVChurn)
	if row.ValueA != 2 || row.ValueB != 2 {
		t.Errorf("expected 2-element sets (quoted comma not split), got %+v", row)
	}
	if row.Delta != 0 || row.Pct == nil || *row.Pct != 0 {
		t.Errorf("expected zero churn for identical MCV lists, got %+v", row)
	}
}

// Stats live on PlannerDelta.Stats and must survive the JSON contract the cloud
// imports, including the nullable pct.
func TestDiffPlanner_StatsSurviveJSON(t *testing.T) {
	from := emptyPlanner("a")
	from.Tables = []snapshot.TableSizingEntry{tbl("public", "t", snapshot.TableSizing{Reltuples: 1_000})}
	from.Columns = []snapshot.ColumnStatsEntry{
		col("public", "t", "c", snapshot.ColumnStats{NDistinct: fp(100), Correlation: fp(0.5)}),
	}
	to := emptyPlanner("b")
	to.Tables = []snapshot.TableSizingEntry{tbl("public", "t", snapshot.TableSizing{Reltuples: 1_000})}
	to.Columns = []snapshot.ColumnStatsEntry{
		col("public", "t", "c", snapshot.ColumnStats{NDistinct: fp(150), Correlation: fp(0.6)}),
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
	if len(out.Stats) != len(delta.Stats) {
		t.Errorf("JSON dropped stat rows: %d in, %d out", len(delta.Stats), len(out.Stats))
	}
	row := findSizing(t, out.Stats, "t.c", MetricNDistinct)
	if row.Delta != 50 || row.Pct == nil {
		t.Errorf("round-trip lost stat delta/pct fidelity: %+v", row)
	}
}

// The console grows a "stats drift:" section when column stats moved; a big
// distribution shift (correlation 0.95 -> -0.1) must surface there.
func TestRenderPlannerConsole_StatsDriftSection(t *testing.T) {
	from := emptyPlanner("aaaaaaaaaaaa")
	from.Columns = []snapshot.ColumnStatsEntry{
		col("public", "orders", "created_at", snapshot.ColumnStats{Correlation: fp(0.95)}),
	}
	to := emptyPlanner("bbbbbbbbbbbb")
	to.Columns = []snapshot.ColumnStatsEntry{
		col("public", "orders", "created_at", snapshot.ColumnStats{Correlation: fp(-0.10)}),
	}

	delta, _ := DiffPlanner(from, to)
	env := &SnapshotDiff{Kind: "planner", FromHash: from.ContentHash, ToHash: to.ContentHash, Planner: delta}

	var buf bytes.Buffer
	RenderPlannerConsole(&buf, env, DefaultMinPct)
	out := buf.String()
	if !strings.Contains(out, "stats drift:") {
		t.Errorf("expected a stats drift section, got:\n%s", out)
	}
	if !strings.Contains(out, "correlation") || !strings.Contains(out, "created_at") {
		t.Errorf("expected the correlation mover on created_at, got:\n%s", out)
	}
}
