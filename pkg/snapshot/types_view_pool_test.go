package snapshot

import (
	"testing"
	"time"
)

// A rotating label lists one entry per server, newest first. Nodes() names
// labels, so it keeps that first entry and drops the repeats.
func TestNodesListsEachLabelOnce(t *testing.T) {
	t0 := time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)
	a := &AnnotatedSchema{Merged: &MergedActivity{Nodes: []NodeActivity{
		{Node: NodeIdentity{Source: "primary", Timestamp: t0}},
		{Node: NodeIdentity{Source: "pool", Timestamp: t0}},
		{Node: NodeIdentity{Source: "pool", Timestamp: t0.Add(-time.Hour)}},
	}}}
	got := a.Nodes()
	if len(got) != 2 || got[0].Source != "primary" || got[1].Source != "pool" || !got[1].Timestamp.Equal(t0) {
		t.Fatalf("want primary and pool's newest row, got %+v", got)
	}
	if !a.Merged.LabelRepeats("pool") || a.Merged.LabelRepeats("primary") {
		t.Error("LabelRepeats must see only the rotating label")
	}
}
