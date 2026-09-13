package history

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
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

// A rotating label's second-newest capture is usually another server's, and
// its reset epoch would read as "counters were reset". The previous capture
// must be the newest server's own earlier one.
func TestPreviousQueryStats_RotatingLabelPairsWithinServer(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	putNodeQueryFingerprintAt(t, store, k, "qa1", "pool", t0, bootA)
	putNodeQueryFingerprintAt(t, store, k, "qb2", "pool", t0.Add(time.Hour), bootB)
	putNodeQueryFingerprintAt(t, store, k, "qa3", "pool", t0.Add(2*time.Hour), bootA)
	putNodeQueryFingerprintAt(t, store, k, "qb4", "pool", t0.Add(3*time.Hour), bootB)

	prev, err := store.PreviousQueryStats(ctx, k)
	if err != nil {
		t.Fatalf("previous: %v", err)
	}
	if len(prev) != 1 || prev[0].ContentHash != "qb2" {
		t.Fatalf("want qb2, the newest server's own earlier capture, got %+v", hashes(prev))
	}
}

// the newest server joined the label after the others: nothing of its own to
// compare with, so no previous capture rather than another server's
func TestPreviousQueryStats_RotatingLabelWithoutOwnCaptureIsAbsent(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB, bootC := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour), t0.Add(-time.Hour)

	putNodeQueryFingerprintAt(t, store, k, "qa1", "pool", t0, bootA)
	putNodeQueryFingerprintAt(t, store, k, "qb2", "pool", t0.Add(time.Hour), bootB)
	putNodeQueryFingerprintAt(t, store, k, "qa3", "pool", t0.Add(2*time.Hour), bootA)
	putNodeQueryFingerprintAt(t, store, k, "qc4", "pool", t0.Add(3*time.Hour), bootC)

	prev, err := store.PreviousQueryStats(ctx, k)
	if err != nil {
		t.Fatalf("previous: %v", err)
	}
	if len(prev) != 0 {
		t.Fatalf("want no previous capture, got %+v", hashes(prev))
	}
}

// A one-way boot change is a restart, not rotation: the second-newest capture
// is still the previous one, and a real reset there is a real caveat.
func TestPreviousQueryStats_RestartKeepsSecondNewest(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootA2 := t0.Add(-72*time.Hour), t0.Add(90*time.Minute)

	putNodeQueryFingerprintAt(t, store, k, "qa1", "primary", t0, bootA)
	putNodeQueryFingerprintAt(t, store, k, "qa2", "primary", t0.Add(time.Hour), bootA)
	putNodeQueryFingerprintAt(t, store, k, "qa3", "primary", t0.Add(2*time.Hour), bootA2)

	prev, err := store.PreviousQueryStats(ctx, k)
	if err != nil {
		t.Fatalf("previous: %v", err)
	}
	if len(prev) != 1 || prev[0].ContentHash != "qa2" {
		t.Fatalf("a restart must keep the second-newest capture qa2, got %+v", hashes(prev))
	}
}

func hashes(qs []schema.QueryStatsSnapshot) []string {
	out := make([]string, len(qs))
	for i, q := range qs {
		out[i] = q.ContentHash
	}
	return out
}

// A rotating label next to a restarted single node, in one call: the pool pass
// must not touch the plain label, and output keeps node_source order.
func TestPreviousQueryStats_RotatingAndPlainLabelsTogether(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB, bootP, bootP2 := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour), t0.Add(-96*time.Hour), t0.Add(90*time.Minute)

	putNodeQueryFingerprintAt(t, store, k, "qa1", "pool", t0, bootA)
	putNodeQueryFingerprintAt(t, store, k, "qb2", "pool", t0.Add(time.Hour), bootB)
	putNodeQueryFingerprintAt(t, store, k, "qa3", "pool", t0.Add(2*time.Hour), bootA)
	putNodeQueryFingerprintAt(t, store, k, "qb4", "pool", t0.Add(3*time.Hour), bootB)
	putNodeQueryFingerprintAt(t, store, k, "qp1", "aaa-primary", t0, bootP)
	putNodeQueryFingerprintAt(t, store, k, "qp2", "aaa-primary", t0.Add(2*time.Hour), bootP2)

	prev, err := store.PreviousQueryStats(ctx, k)
	if err != nil {
		t.Fatalf("previous: %v", err)
	}
	if got := hashes(prev); len(got) != 2 || got[0] != "qp1" || got[1] != "qb2" {
		t.Fatalf("want [qp1 qb2] in node order, got %v", got)
	}
}

// the previous capture predates fingerprints; the newest server is known and
// the label rotates, so it still pairs with that server's own capture
func TestPreviousQueryStats_UnfingerprintedPreviousOnRotatingLabel(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	putNodeQueryFingerprintAt(t, store, k, "qa1", "pool", t0, bootA)
	putNodeQueryFingerprintAt(t, store, k, "qb2", "pool", t0.Add(time.Hour), bootB)
	putNodeQueryFingerprintAt(t, store, k, "qa3", "pool", t0.Add(2*time.Hour), bootA)
	putNodeQueryStatsAt(t, store, k, "qx4", "pool", t0.Add(150*time.Minute))
	putNodeQueryFingerprintAt(t, store, k, "qb5", "pool", t0.Add(3*time.Hour), bootB)

	prev, err := store.PreviousQueryStats(ctx, k)
	if err != nil {
		t.Fatalf("previous: %v", err)
	}
	if got := hashes(prev); len(got) != 1 || got[0] != "qb2" {
		t.Fatalf("want qb2, got %v", got)
	}
}

// the newest capture carries no start time: nothing to pair on, so the
// second-newest stays, as for any label
func TestPreviousQueryStats_UnfingerprintedNewestKeepsSecondNewest(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	putNodeQueryFingerprintAt(t, store, k, "qa1", "pool", t0, bootA)
	putNodeQueryFingerprintAt(t, store, k, "qb2", "pool", t0.Add(time.Hour), bootB)
	putNodeQueryFingerprintAt(t, store, k, "qa3", "pool", t0.Add(2*time.Hour), bootA)
	putNodeQueryStatsAt(t, store, k, "qx4", "pool", t0.Add(3*time.Hour))

	prev, err := store.PreviousQueryStats(ctx, k)
	if err != nil {
		t.Fatalf("previous: %v", err)
	}
	if got := hashes(prev); len(got) != 1 || got[0] != "qa3" {
		t.Fatalf("want qa3, got %v", got)
	}
}

// a store that cannot answer the pool check leaves the label out instead of
// failing every label's caveats
func TestPreviousQueryStats_FailedPoolCheckDropsOnlyThatLabel(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	t0 := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Hour)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)

	putNodeQueryFingerprintAt(t, store, k, "qa1", "pool", t0, bootA)
	putNodeQueryFingerprintAt(t, store, k, "qb2", "pool", t0.Add(time.Hour), bootB)
	putNodeQueryStatsAt(t, store, k, "qp1", "aaa-primary", t0)
	putNodeQueryStatsAt(t, store, k, "qp2", "aaa-primary", t0.Add(time.Hour))

	plain := queryStatsFixture("sr", "qp1", "aaa-primary")
	pooled := queryStatsFixture("sr", "qa1", "pool")
	pooled.Node.PostmasterStartTime = &bootA
	prev := []schema.QueryStatsSnapshot{*plain, *pooled}

	// IsPool reads activity_stats in its union; LatestQueryStats does not
	if _, err := store.db.ExecContext(ctx, "DROP TABLE activity_stats"); err != nil {
		t.Fatal(err)
	}
	got, err := store.previousWithinMember(ctx, k, prev)
	if err != nil {
		t.Fatalf("a failed pool check must not fail the call: %v", err)
	}
	if h := hashes(got); len(h) != 1 || h[0] != "qp1" {
		t.Fatalf("want only the plain label kept, got %v", h)
	}
}
