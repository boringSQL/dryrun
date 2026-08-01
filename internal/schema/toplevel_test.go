package schema

import (
	"strings"
	"testing"
)

func strPtr(s string) *string { return &s }

func TestCollectIdentifiersSplitsOnNonWordBytes(t *testing.T) {
	got := map[string]struct{}{}
	collectIdentifiers("SELECT a.id FROM public.Task_Assignments a WHERE a.x=$1", got)

	for _, want := range []string{"select", "id", "public", "task_assignments", "a", "x"} {
		if _, ok := got[want]; !ok {
			t.Errorf("missing identifier %q in %v", want, got)
		}
	}
	// $ is a word byte so placeholders tokenize whole; harmless, since an
	// identifier may not start with $ and can never collide with one
	if _, ok := got["$1"]; !ok {
		t.Errorf("expected $1 to tokenize as one unit, got %v", got)
	}
	// separators must still split rather than run identifiers together
	if _, ok := got["a.id"]; ok {
		t.Errorf("dot failed to split an identifier: %v", got)
	}
}

// $ is legal after the first character of an unquoted identifier, so a table
// named my$table must still match the statement text that references it.
func TestCollectIdentifiersKeepsDollarInNames(t *testing.T) {
	got := map[string]struct{}{}
	collectIdentifiers("SELECT * FROM my$table WHERE id = $1", got)
	if _, ok := got["my$table"]; !ok {
		t.Errorf("dollar-containing identifier was split: %v", got)
	}
}

func annotatedWith(track *string, canonicals ...string) *AnnotatedSchema {
	entries := make([]QueryStatsEntry, len(canonicals))
	for i, c := range canonicals {
		entries[i] = QueryStatsEntry{Canonical: c}
	}
	return &AnnotatedSchema{
		QueryStats: []QueryStatsSnapshot{{
			PgssTrack: track,
			RawRows:   1,
			Queries:   entries,
		}},
	}
}

// An unknown track must never be treated as 'top'. Old history DBs, pulled
// payloads from an older producer, and an unreadable GUC all reach nil.
func TestUnknownTrackIsNotTreatedAsTop(t *testing.T) {
	ix := BuildQueryRefIndex(annotatedWith(nil, "SELECT 1 FROM t"))
	if ix == nil {
		t.Fatal("stats were captured; the index must exist to report why it stood down")
	}
	if ix.Unattributed("hidden_table", unattributedScanThreshold) {
		t.Error("an unrecorded track must not be treated as 'top'")
	}
	if r := ix.SkipReason(); !strings.Contains(r, "track") {
		t.Errorf("skip reason should name the track, got %q", r)
	}
	if ix := BuildQueryRefIndex(&AnnotatedSchema{}); ix != nil {
		t.Errorf("want nil index with no query stats at all, got %+v", ix)
	}
	if ix := BuildQueryRefIndex(nil); ix != nil {
		t.Errorf("want nil index for nil schema, got %+v", ix)
	}
}

func TestUnattributedRequiresTrackTop(t *testing.T) {
	ix := BuildQueryRefIndex(annotatedWith(strPtr("all"), "SELECT 1 FROM visible_table"))
	if ix == nil {
		t.Fatal("want an index when track was captured")
	}
	if ix.Unattributed("hidden_table", 100_000) {
		t.Error("an unfiltered track = 'all' capture records nested statements, so an unexplained table is not a pgss blind spot")
	}
}

// A toplevel-filtered capture excludes nested statements at fetch time, so
// track = 'all' no longer rescues the blind spot: the flag fires as under
// track = 'top'.
func TestUnattributedFiresWhenToplevelFiltered(t *testing.T) {
	a := annotatedWith(strPtr("all"), "SELECT 1 FROM visible_table")
	a.QueryStats[0].ToplevelOnly = true
	ix := BuildQueryRefIndex(a)
	if ix == nil {
		t.Fatal("want an index when track was captured")
	}
	if !ix.Unattributed("hidden_table", 100_000) {
		t.Error("a filtered capture holds top-level statements only; nested work is invisible regardless of track")
	}
	// nil track with the filter set is still top-level-only.
	a.QueryStats[0].PgssTrack = nil
	if !BuildQueryRefIndex(a).Unattributed("hidden_table", 100_000) {
		t.Error("the filter, not the track GUC, decides what the capture can see")
	}
}

// track = 'none' records nothing, so an unreferenced table proves no blind
// spot — even on a node new enough to carry the toplevel marker.
func TestUnattributedSuppressedWhenTrackNone(t *testing.T) {
	a := annotatedWith(strPtr("none"), "SELECT 1 FROM visible_table")
	a.QueryStats[0].ToplevelOnly = true
	if BuildQueryRefIndex(a).Unattributed("hidden_table", 100_000) {
		t.Error("a node that records nothing is not evidence of the function/trigger blind spot")
	}
}

func TestUnattributedFlagsOnlyUnreferencedBusyTables(t *testing.T) {
	ix := BuildQueryRefIndex(annotatedWith(strPtr("top"),
		"SELECT a.id FROM public.visible_table a WHERE a.x = $1"))
	if ix == nil {
		t.Fatal("want an index when track was captured")
	}

	if !ix.Unattributed("hidden_table", unattributedScanThreshold) {
		t.Error("a busy table no statement mentions should be flagged")
	}
	if ix.Unattributed("visible_table", 100_000) {
		t.Error("a table a captured statement references must never be flagged")
	}
	// case-insensitive: pgss text casing is whatever the client sent
	if ix.Unattributed("VISIBLE_TABLE", 100_000) {
		t.Error("reference matching must be case-insensitive")
	}
	if ix.Unattributed("hidden_table", unattributedScanThreshold-1) {
		t.Error("below the threshold the scans are too few to be evidence of anything")
	}
	var nilIx *QueryRefIndex
	if nilIx.Unattributed("hidden_table", unattributedScanThreshold) {
		t.Error("nil index must be inert, not flag everything")
	}
}

// A capped or evicted statement set is a subset of the workload, so "nothing
// mentions this table" stops being evidence and the flag must stand down.
func TestUnattributedSuppressedWhenStatementSetTruncated(t *testing.T) {
	capped := annotatedWith(strPtr("top"), "SELECT 1 FROM visible_table")
	capped.QueryStats[0].RawRows = QueryStatsRowCap
	if BuildQueryRefIndex(capped).Unattributed("hidden_table", unattributedScanThreshold) {
		t.Error("hitting the row cap means the statement set is incomplete")
	}

	evicted := annotatedWith(strPtr("top"), "SELECT 1 FROM visible_table")
	evicted.QueryStats[0].InfoAfter = &QueryStatsInfo{Dealloc: 1}
	if BuildQueryRefIndex(evicted).Unattributed("hidden_table", unattributedScanThreshold) {
		t.Error("a dealloc means pgss discarded shapes that might have explained the table")
	}
}

// Activity is summed across nodes, so a single node that can see nested
// statements is enough to disqualify the blind-spot explanation.
func TestUnattributedRequiresEveryNodeOnTrackTop(t *testing.T) {
	a := annotatedWith(strPtr("top"), "SELECT 1 FROM visible_table")
	a.QueryStats = append(a.QueryStats, QueryStatsSnapshot{
		PgssTrack: strPtr("all"),
		RawRows:   1,
		Queries:   []QueryStatsEntry{{Canonical: "SELECT 1 FROM visible_table"}},
	})
	if BuildQueryRefIndex(a).Unattributed("hidden_table", unattributedScanThreshold) {
		t.Error("a node on track = 'all' can record nested statements, so the scans are explainable")
	}
}

// Leaf partitions accrue activity under their own name while statements name
// the parent; flagging them would bury the real finding under one row per partition.
func TestUnattributedSkipsPartitionChildren(t *testing.T) {
	a := annotatedWith(strPtr("top"), "SELECT 1 FROM events WHERE at = $1")
	a.Schema = &SchemaSnapshot{Tables: []Table{{
		Schema: "public", Name: "events",
		PartitionInfo: &PartitionInfo{
			Strategy: PartitionRange,
			Key:      "at",
			Children: []PartitionChild{{Schema: "public", Name: "events_2026_01"}},
		},
	}}}
	ix := BuildQueryRefIndex(a)
	if ix.Unattributed("events_2026_01", unattributedScanThreshold) {
		t.Error("a partition child must not be flagged; its parent is what statements name")
	}
	if !ix.Unattributed("orphan_table", unattributedScanThreshold) {
		t.Error("a genuinely unreferenced non-partition table should still flag")
	}
}
