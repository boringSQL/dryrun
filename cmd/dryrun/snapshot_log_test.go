package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/pkg/diff"
)

var logT0 = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

func schemaRow(hash string, at time.Duration) history.SnapshotSummary {
	return history.SnapshotSummary{Kind: history.SchemaKind(), ContentHash: hash, Timestamp: logT0.Add(at)}
}

func statsRow(kind history.SnapshotKind, schemaRef string, at time.Duration) history.SnapshotSummary {
	return history.SnapshotSummary{Kind: kind, SchemaRefHash: schemaRef, NodeLabel: kind.NodeLabel, Timestamp: logT0.Add(at)}
}

// Stats must land under the schema version that was live when they were taken.
// The hash alone cannot say that once a schema is reverted (A -> B -> A stores
// A twice), and a pruned twin must not pull its old stats under a later version.
func TestAssignVersions(t *testing.T) {
	// newest first, as ListSchema returns them
	schemas := []history.SnapshotSummary{
		schemaRow("A", 48*time.Hour), // A again after the revert
		schemaRow("B", 24*time.Hour),
		schemaRow("A", 0),
	}
	primary := history.ActivityKind("primary")

	cases := []struct {
		name string
		row  history.SnapshotSummary
		want int // index into schemas, -1 for orphan
	}{
		{"before the revert belongs to the first A", statsRow(primary, "A", time.Hour), 2},
		{"after the revert belongs to the second A", statsRow(primary, "A", 49*time.Hour), 0},
		{"B in between", statsRow(primary, "B", 25*time.Hour), 1},
		{"same second as its schema", statsRow(primary, "B", 24*time.Hour), 1},
		{"a few seconds before its schema is clock skew", statsRow(primary, "A", -30*time.Second), 2},
		{"unknown schema is an orphan", statsRow(primary, "gone", time.Hour), -1},
		{"empty schema_ref is an orphan", statsRow(primary, "", time.Hour), -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			per, orphans := assignVersions(schemas, []history.SnapshotSummary{tc.row})
			got := -1
			for i, rows := range per {
				if len(rows) > 0 {
					got = i
				}
			}
			if got != tc.want || (tc.want == -1) != (len(orphans) == 1) {
				t.Fatalf("assigned to %d (orphans %d), want %d", got, len(orphans), tc.want)
			}
		})
	}

	// prune deleted the first A because the second survives: its stats are
	// hours older than any A left, and must not show up under the later A
	t.Run("stats of a pruned twin are orphans", func(t *testing.T) {
		pruned := schemas[:2]
		per, orphans := assignVersions(pruned, []history.SnapshotSummary{statsRow(primary, "A", time.Hour)})
		if len(per[0]) != 0 || len(orphans) != 1 {
			t.Fatalf("pruned-twin stats landed under %v, orphans %d", per, len(orphans))
		}
	})
}

// capture --all walks the fleet node by node, so one run spreads over minutes;
// hourly captures of a stable schema must still stay apart.
func TestClusterCaptures(t *testing.T) {
	planner := history.PlannerKind()
	q := func(node string, at time.Duration) history.SnapshotSummary {
		return statsRow(history.QueryKind(node), "A", at)
	}
	rows := []history.SnapshotSummary{
		statsRow(planner, "A", 0),
		q("primary", 0),
		q("replica-1", 81*time.Second),
		q("replica-2", 160*time.Second), // chained to replica-1, not to the start
		statsRow(planner, "A", time.Hour),
		q("primary", time.Hour),
	}
	caps := clusterCaptures(rows)
	if len(caps) != 2 {
		t.Fatalf("got %d captures, want 2: %+v", len(caps), caps)
	}
	if got := strings.Join(caps[1].Nodes, ", "); got != "primary, replica-1, replica-2" {
		t.Errorf("fleet capture nodes = %q", got)
	}
	if !caps[0].At.Equal(logT0.Add(time.Hour)) {
		t.Errorf("captures not newest first: %v", caps[0].At)
	}
	var buf bytes.Buffer
	renderCaptures(&buf, clusterCaptures([]history.SnapshotSummary{statsRow(planner, "A", 0)}))
	if !strings.HasSuffix(buf.String(), "  planner\n") {
		t.Errorf("planner-only capture renders %q", buf.String())
	}
}

// Over budget, a busy table folds to a count, overloads merge, and the cut
// never leaves a table header without its changes. The "N more" count must
// add up and the hint must run as printed, naming this version and its parent.
func TestRenderVersionFolds(t *testing.T) {
	pub := "public"
	table := func(name string) diff.ObjectRef {
		return diff.ObjectRef{Kind: "table", Schema: &pub, Name: name}
	}
	col := func(tbl, name string) diff.Change {
		yes := true
		return diff.Change{Type: diff.ColumnAdded, Object: table(tbl), Column: &diff.ColumnChange{Name: name, Nullable: &yes}}
	}
	fn := func(args string) diff.Change {
		return diff.Change{Type: diff.FunctionAdded, Object: diff.ObjectRef{Kind: "function", Schema: &pub, Name: "f(" + args + ")"}}
	}

	var changes []diff.Change
	for i := range 6 {
		changes = append(changes, col("big", fmt.Sprintf("c%d", i)))
	}
	changes = append(changes, fn("int"), fn("text"))
	for i := range 3 {
		changes = append(changes, diff.Change{Type: diff.TableAdded, Object: table(fmt.Sprintf("t%d", i))})
	}
	// two-child table lands across the cut: its header must not dangle
	changes = append(changes, col("small", "a"), col("small", "b"))

	v := schemaVersion{Row: schemaRow("abcdef0123", 0), Ref: 4, Delta: &diff.SchemaDelta{Changes: changes}}
	var buf bytes.Buffer
	renderVersion(&buf, v, logView{})
	out := buf.String()

	for _, want := range []string{
		"schema abcdef0  (latest~4)",
		"13 changes",
		"6 changes\n",         // big folded
		"+ function f() ×2\n", // overloads merged
		"    … 2 more · dryrun snapshot diff latest~5 latest~4\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "~ table small") || strings.Contains(out, "public.") {
		t.Errorf("dangling header or spelled-out public schema:\n%s", out)
	}
}

// -p exists to be grepped: every change must come out whole, with overloads
// told apart by signature and nothing folded, cut or clipped.
func TestRenderVersionPatch(t *testing.T) {
	pub := "public"
	tbl := diff.ObjectRef{Kind: "table", Schema: &pub, Name: "big"}
	yes := true
	var changes []diff.Change
	for i := range 8 {
		changes = append(changes, diff.Change{Type: diff.ColumnAdded, Object: tbl, Column: &diff.ColumnChange{Name: fmt.Sprintf("c%d", i), Nullable: &yes}})
	}
	for _, args := range []string{"int", "text"} {
		changes = append(changes, diff.Change{Type: diff.FunctionAdded, Object: diff.ObjectRef{Kind: "function", Schema: &pub, Name: "f(" + args + ")"}})
	}
	longType := "TABLE(" + strings.Repeat("col bigint, ", 15) + "last text)"
	changes = append(changes, diff.Change{Type: diff.ColumnTypeChanged, Object: tbl,
		Column: &diff.ColumnChange{Name: "wide", FromType: "text", ToType: longType}})

	v := schemaVersion{Row: schemaRow("abcdef0123", 0), Ref: 1, Delta: &diff.SchemaDelta{Changes: changes}}
	var buf bytes.Buffer
	renderVersion(&buf, v, logView{Patch: true})
	out := buf.String()

	for _, want := range []string{"c0 added", "c7 added", "+ function f(int)\n", "+ function f(text)\n", longType} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "…") || strings.Contains(out, "×") {
		t.Errorf("-p cut, clipped or merged lines:\n%s", out)
	}
}

// The text log defaults to 20 versions, but -p is for grepping the whole history:
// a silent cap there hides older versions, since grep drops the "older" trailer.
func TestLogLimit(t *testing.T) {
	cases := []struct {
		limit      int
		set, patch bool
		want       int
	}{
		{0, false, false, defaultLogLimit},
		{0, false, true, 0},
		{2, true, true, 2},
	}
	for _, tc := range cases {
		if got := logLimit(tc.limit, tc.set, tc.patch); got != tc.want {
			t.Errorf("logLimit(%d, set=%t, patch=%t) = %d, want %d", tc.limit, tc.set, tc.patch, got, tc.want)
		}
	}
}
