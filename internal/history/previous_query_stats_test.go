package history

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// PreviousQueryStats backs the comparability caveats: it must return the
// capture LatestQueryStats supersedes, per node, and nothing else.
func TestPreviousQueryStats_ReturnsTheSupersededCapture(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	base := time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		q := queryStatsFixture("sref-A", fmt.Sprintf("qch-primary-%d", i), "primary")
		q.Node.Timestamp = base.Add(time.Duration(i) * time.Hour)
		if _, err := store.PutQueryStats(ctx, k, q); err != nil {
			t.Fatalf("put #%d: %v", i, err)
		}
	}

	prev, err := store.PreviousQueryStats(ctx, k)
	if err != nil {
		t.Fatalf("previous: %v", err)
	}
	if len(prev) != 1 {
		t.Fatalf("rows = %d, want 1 per node", len(prev))
	}
	// second-newest, not the newest and not the oldest
	if want := base.Add(time.Hour); !prev[0].Node.Timestamp.Equal(want) {
		t.Errorf("timestamp = %s, want %s", prev[0].Node.Timestamp, want)
	}

	latest, err := store.LatestQueryStats(ctx, k)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if latest[0].Node.Timestamp.Equal(prev[0].Node.Timestamp) {
		t.Error("previous must not be the same capture as latest")
	}
}

// A node captured once has nothing to compare against. It must be absent
// rather than present-and-empty: the caveats treat a returned row as a real
// earlier capture, so a synthetic zero row would read as a settings change.
func TestPreviousQueryStats_AbsentForSingleCapture(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	q := queryStatsFixture("sref-A", "qch-only", "primary")
	q.Node.Timestamp = time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)
	if _, err := store.PutQueryStats(ctx, k, q); err != nil {
		t.Fatalf("put: %v", err)
	}

	prev, err := store.PreviousQueryStats(ctx, k)
	if err != nil {
		t.Fatalf("previous: %v", err)
	}
	if len(prev) != 0 {
		t.Errorf("rows = %d, want none for a node with one capture", len(prev))
	}
}

// Per node, like LatestQueryStats: a node with history must not borrow another
// node's older capture, and a single-capture node must not appear at all.
func TestPreviousQueryStats_IsPerNode(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	base := time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)

	for i := 0; i < 2; i++ {
		q := queryStatsFixture("sref-A", fmt.Sprintf("qch-primary-%d", i), "primary")
		q.Node.Timestamp = base.Add(time.Duration(i) * time.Hour)
		if _, err := store.PutQueryStats(ctx, k, q); err != nil {
			t.Fatalf("put primary #%d: %v", i, err)
		}
	}
	single := queryStatsFixture("sref-A", "qch-replica-0", "replica")
	single.Node.Timestamp = base
	if _, err := store.PutQueryStats(ctx, k, single); err != nil {
		t.Fatalf("put replica: %v", err)
	}

	prev, err := store.PreviousQueryStats(ctx, k)
	if err != nil {
		t.Fatalf("previous: %v", err)
	}
	if len(prev) != 1 {
		t.Fatalf("rows = %d, want only the node with two captures", len(prev))
	}
	if prev[0].Node.Source != "primary" {
		t.Errorf("node = %s, want primary", prev[0].Node.Source)
	}
	if !prev[0].Node.Timestamp.Equal(base) {
		t.Errorf("timestamp = %s, want primary's older capture at %s", prev[0].Node.Timestamp, base)
	}
}

// Captures pushed within the same RFC3339 second are ordered by id, matching
// LatestQueryStats — otherwise the two disagree about which row is which.
func TestPreviousQueryStats_BreaksTimestampTiesById(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	ts := time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)

	for i := 0; i < 2; i++ {
		q := queryStatsFixture("sref-A", fmt.Sprintf("qch-tie-%d", i), "primary")
		q.Node.Timestamp = ts
		if _, err := store.PutQueryStats(ctx, k, q); err != nil {
			t.Fatalf("put #%d: %v", i, err)
		}
	}

	latest, err := store.LatestQueryStats(ctx, k)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	prev, err := store.PreviousQueryStats(ctx, k)
	if err != nil {
		t.Fatalf("previous: %v", err)
	}
	if len(latest) != 1 || len(prev) != 1 {
		t.Fatalf("latest=%d previous=%d, want 1 each", len(latest), len(prev))
	}
	if latest[0].ContentHash == prev[0].ContentHash {
		t.Error("latest and previous resolved to the same row on a timestamp tie")
	}
}
