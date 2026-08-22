package history

import (
	"context"
	"testing"
	"time"
)

// The MCP layer reports schema, planner and activity capture times separately
// because the three are written by separate commands. GetAnnotated is where
// they are joined, and the planner payload passes through
// RollUpPartitionSizing on the way, so this pins that none of the three is
// lost or attributed to another.
func TestGetAnnotated_PreservesEachCaptureTime(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	snap := testSnapshot("sref-A", "appdb")
	snap.Timestamp = time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
	if _, err := store.PutSchema(ctx, k, snap); err != nil {
		t.Fatalf("put schema: %v", err)
	}

	// DDL has not moved in weeks; planner is re-captured every night
	planner := plannerFixture("sref-A", "ch-P", "appdb")
	planner.Timestamp = time.Date(2026, 7, 22, 2, 0, 0, 0, time.UTC)
	if _, err := store.PutPlanner(ctx, k, planner); err != nil {
		t.Fatalf("put planner: %v", err)
	}

	primary := activityFixture("sref-A", "ch-A1", "primary", false)
	primary.Node.Timestamp = time.Date(2026, 7, 22, 2, 15, 0, 0, time.UTC)
	if _, err := store.PutActivity(ctx, k, primary); err != nil {
		t.Fatalf("put primary activity: %v", err)
	}
	replica := activityFixture("sref-A", "ch-A2", "replica1", true)
	replica.Node.Timestamp = time.Date(2026, 7, 3, 2, 15, 0, 0, time.UTC)
	if _, err := store.PutActivity(ctx, k, replica); err != nil {
		t.Fatalf("put replica activity: %v", err)
	}

	a, err := store.GetAnnotated(ctx, k, NewRefLatest())
	if err != nil {
		t.Fatalf("GetAnnotated: %v", err)
	}

	if !a.Schema.Timestamp.Equal(snap.Timestamp) {
		t.Errorf("schema time: want %s, got %s", snap.Timestamp, a.Schema.Timestamp)
	}
	if a.Planner == nil {
		t.Fatal("no planner joined")
	}
	if !a.Planner.Timestamp.Equal(planner.Timestamp) {
		t.Errorf("planner time: want %s, got %s", planner.Timestamp, a.Planner.Timestamp)
	}
	if a.Merged == nil || len(a.Merged.Nodes) != 2 {
		t.Fatalf("want both nodes, got %+v", a.Merged)
	}
	for _, n := range a.Merged.Nodes {
		want := primary.Node.Timestamp
		if n.Node.Source == "replica1" {
			want = replica.Node.Timestamp
		}
		if !n.Node.Timestamp.Equal(want) {
			t.Errorf("%s time: want %s, got %s", n.Node.Source, want, n.Node.Timestamp)
		}
	}
}
