package schema

import (
	"testing"
	"time"
)

// DetectStaleStats must return entries worst-first so a downstream response cap
// keeps the tables that most urgently need ANALYZE rather than an arbitrary
// slice. "Worst" has a deliberate ordering: a table that has NEVER been analyzed
// (nil days-ago) outranks any table that was analyzed at some point, and among
// the ever-analyzed the one analyzed longest ago comes first. We seed three
// tables on a single primary node — one never analyzed, one 30 days stale, one
// 10 days stale — in an order that does NOT match the desired output, so a
// passing test proves the sort actually reordered them rather than coincidence.
func TestDetectStaleStats_WorstFirstOrdering(t *testing.T) {
	now := time.Now().UTC()
	d10 := now.Add(-10 * 24 * time.Hour)
	d30 := now.Add(-30 * 24 * time.Hour)

	// Insertion order: 10d, never, 30d — intentionally NOT the expected order.
	a := &AnnotatedSchema{
		Schema: &SchemaSnapshot{},
		Merged: &MergedActivity{Nodes: []NodeActivity{{
			Node: NodeIdentity{Source: "primary"},
			Tables: []TableActivityEntry{
				{Table: qual("public", "recent"), Activity: TableActivity{LastAnalyze: &d10}},
				{Table: qual("public", "never"), Activity: TableActivity{}},
				{Table: qual("public", "ancient"), Activity: TableActivity{LastAnalyze: &d30}},
			},
		}}},
	}

	entries := DetectStaleStats(a, 7)
	if len(entries) != 3 {
		t.Fatalf("expected all 3 tables stale past the 7-day threshold, got %d", len(entries))
	}

	// never-analyzed first, then 30-day, then 10-day.
	gotOrder := []string{entries[0].Table, entries[1].Table, entries[2].Table}
	wantOrder := []string{"never", "ancient", "recent"}
	for i := range wantOrder {
		if gotOrder[i] != wantOrder[i] {
			t.Errorf("position %d: got %q, want %q (full order: %v)", i, gotOrder[i], wantOrder[i], gotOrder)
		}
	}

	// The never-analyzed table specifically must carry a nil days-ago so callers
	// can render it as "never" rather than "0 days ago".
	if entries[0].LastAnalyzedDaysAgo != nil {
		t.Errorf("never-analyzed table should have nil LastAnalyzedDaysAgo, got %v", *entries[0].LastAnalyzedDaysAgo)
	}
}
