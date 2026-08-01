package schema

import (
	"testing"

	"github.com/boringsql/qshape"
)

func strPtr(s string) *string { return &s }

// One nested row per fingerprint is the common case; two rows of the same shape
// must roll up rather than overwrite, or the caller's subtraction under-counts.
func TestGroupNestedRollsUpByFingerprint(t *testing.T) {
	nested := []qshape.Query{
		{QueryID: 1, Calls: 10, Raw: "SELECT id FROM audit_log WHERE id = $1", TotalExecTimeMs: 100, Rows: 10},
		{QueryID: 2, Calls: 5, Raw: "SELECT id FROM audit_log WHERE id = $2", TotalExecTimeMs: 50, Rows: 5},
		{QueryID: 3, Calls: 1, Raw: "SELECT name FROM other WHERE id = $1", TotalExecTimeMs: 7, Rows: 1},
	}
	got, err := groupNested(nested)
	if err != nil {
		t.Fatalf("groupNested: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 fingerprints, got %d: %+v", len(got), got)
	}

	var audit nestedRollup
	found := false
	for _, r := range got {
		if r.calls == 15 {
			audit, found = r, true
		}
	}
	if !found {
		t.Fatalf("the two audit_log rows did not roll up into one entry: %+v", got)
	}
	if audit.execTimeMs != 150 {
		t.Errorf("exec time: want 150, got %v", audit.execTimeMs)
	}
}

func TestGroupNestedEmptyIsNil(t *testing.T) {
	got, err := groupNested(nil)
	if err != nil {
		t.Fatalf("groupNested: %v", err)
	}
	if got != nil {
		t.Errorf("want nil for no nested rows, got %+v", got)
	}
}

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

// The flag exists to explain scans that pg_stat_statements structurally cannot
// show. Without the setting there is no such explanation, so no flag.
func TestBuildQueryRefIndexNilWithoutTrack(t *testing.T) {
	if ix := BuildQueryRefIndex(annotatedWith(nil, "SELECT 1 FROM t")); ix != nil {
		t.Errorf("want nil index when pgss_track was not captured, got %+v", ix)
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
		t.Error("track = 'all' records nested statements, so an unexplained table is not a pgss blind spot")
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
