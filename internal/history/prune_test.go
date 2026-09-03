package history

import (
	"context"
	"testing"
	"time"
)

// Prune is destructive and key-scoped, so these tests pin down exactly which
// rows it is allowed to touch: only activity/query rows belonging to the
// requested (project, database), only those older than the cutoff, and never
// the newest row per node — an idle node whose every capture has aged out must
// still have something for the "latest reads" paths to return.

// The cases below predate schema/planner retention and pin down the per-node
// series rules only; the newest-N floors are exercised in prune_retention_test.go.
func pruneStatsOnly(ctx context.Context, s *Store, k SnapshotKey, cutoff time.Time) (int64, error) {
	res, err := s.Prune(ctx, k, PruneOptions{Cutoff: cutoff})
	return res.Total(), err
}

func putActivityAt(t *testing.T, s *Store, k SnapshotKey, source, contentHash string, ts time.Time) {
	t.Helper()
	a := activityFixture("sref-A", contentHash, source, false)
	a.Node.Timestamp = ts
	if _, err := s.PutActivity(context.Background(), k, a); err != nil {
		t.Fatalf("put activity %s@%s: %v", source, ts.Format(time.RFC3339), err)
	}
}

func putQueryStatsAt(t *testing.T, s *Store, k SnapshotKey, source, contentHash string, ts time.Time) {
	t.Helper()
	q := queryStatsFixture("sref-A", contentHash, source)
	q.Node.Timestamp = ts
	if _, err := s.PutQueryStats(context.Background(), k, q); err != nil {
		t.Fatalf("put query stats %s@%s: %v", source, ts.Format(time.RFC3339), err)
	}
}

func survivingHashes(t *testing.T, s *Store, table string, k SnapshotKey) map[string]string {
	t.Helper()
	rows, err := s.db.Query(
		"SELECT node_source, content_hash FROM "+table+" WHERE project_id = ? AND database_id = ?",
		string(k.ProjectID), string(k.DatabaseID),
	)
	if err != nil {
		t.Fatalf("select %s: %v", table, err)
	}
	defer rows.Close()

	out := map[string]string{}
	for rows.Next() {
		var node, hash string
		if err := rows.Scan(&node, &hash); err != nil {
			t.Fatalf("scan %s: %v", table, err)
		}
		out[node] = hash
	}
	return out
}

// The ordinary case: rows older than the cutoff go, newer rows stay, and both
// stats tables are pruned in the same call.
func TestPrune_DropsOldRowsKeepsRecent(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-100 * 24 * time.Hour)
	recent := now.Add(-1 * time.Hour)

	// two aged rows plus a recent one per node, so the survivor guard is not
	// what is keeping the recent row alive.
	putActivityAt(t, store, k, "node-a", "act-old-1", old)
	putActivityAt(t, store, k, "node-a", "act-old-2", old.Add(time.Hour))
	putActivityAt(t, store, k, "node-a", "act-recent", recent)
	putQueryStatsAt(t, store, k, "node-a", "qs-old-1", old)
	putQueryStatsAt(t, store, k, "node-a", "qs-recent", recent)

	cutoff := now.Add(-90 * 24 * time.Hour)
	n, err := pruneStatsOnly(ctx, store, k, cutoff)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 3 {
		t.Errorf("pruned %d rows, want 3 (2 activity + 1 query)", n)
	}

	if got := countRows(t, store, "activity_stats", k); got != 1 {
		t.Errorf("activity rows left = %d, want 1", got)
	}
	if got := countRows(t, store, "query_stats", k); got != 1 {
		t.Errorf("query rows left = %d, want 1", got)
	}
	if got := survivingHashes(t, store, "activity_stats", k)["node-a"]; got != "act-recent" {
		t.Errorf("surviving activity row = %q, want act-recent", got)
	}
}

// When every row for a node has aged out, one must survive — and it must be
// the newest by timestamp. Rows are inserted out of order here because
// `snapshot pull` backfills older captures after newer local ones, so the
// highest rowid is not necessarily the latest take.
func TestPrune_KeepsNewestPerNodeByTimestampNotRowID(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	newest := now.Add(-100 * 24 * time.Hour)
	older := now.Add(-120 * 24 * time.Hour)
	oldest := now.Add(-140 * 24 * time.Hour)

	// newest first so it gets the LOWEST id; a MAX(id) survivor rule keeps the
	// wrong row and this test fails.
	putActivityAt(t, store, k, "node-a", "act-newest", newest)
	putActivityAt(t, store, k, "node-a", "act-oldest", oldest)
	putActivityAt(t, store, k, "node-a", "act-older", older)
	putQueryStatsAt(t, store, k, "node-a", "qs-newest", newest)
	putQueryStatsAt(t, store, k, "node-a", "qs-oldest", oldest)

	if _, err := pruneStatsOnly(ctx, store, k, now.Add(-90*24*time.Hour)); err != nil {
		t.Fatalf("prune: %v", err)
	}

	if got := survivingHashes(t, store, "activity_stats", k)["node-a"]; got != "act-newest" {
		t.Errorf("surviving activity row = %q, want act-newest", got)
	}
	if got := survivingHashes(t, store, "query_stats", k)["node-a"]; got != "qs-newest" {
		t.Errorf("surviving query row = %q, want qs-newest", got)
	}
}

// The survivor guard is per node, not per key: two nodes that have both gone
// quiet each keep their own newest row.
func TestPrune_SurvivorIsPerNode(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-140 * 24 * time.Hour)

	for _, node := range []string{"node-a", "node-b"} {
		putActivityAt(t, store, k, node, node+"-old", old)
		putActivityAt(t, store, k, node, node+"-newest", old.Add(24*time.Hour))
	}

	if _, err := pruneStatsOnly(ctx, store, k, now.Add(-90*24*time.Hour)); err != nil {
		t.Fatalf("prune: %v", err)
	}

	got := survivingHashes(t, store, "activity_stats", k)
	if len(got) != 2 {
		t.Fatalf("surviving nodes = %d, want 2 (%v)", len(got), got)
	}
	for _, node := range []string{"node-a", "node-b"} {
		if got[node] != node+"-newest" {
			t.Errorf("node %s survivor = %q, want %s-newest", node, got[node], node)
		}
	}
}

// Pruning one project/database must not reach into another's rows, however
// old they are.
func TestPrune_DoesNotTouchOtherKeys(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	target := key("acme", "primary")
	otherDB := key("acme", "replica")
	otherProject := key("globex", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-140 * 24 * time.Hour)

	for _, k := range []SnapshotKey{target, otherDB, otherProject} {
		putActivityAt(t, store, k, "node-a", "old-1", old)
		putActivityAt(t, store, k, "node-a", "old-2", old.Add(time.Hour))
		putQueryStatsAt(t, store, k, "node-a", "qs-old-1", old)
		putQueryStatsAt(t, store, k, "node-a", "qs-old-2", old.Add(time.Hour))
	}

	if _, err := pruneStatsOnly(ctx, store, target, now.Add(-90*24*time.Hour)); err != nil {
		t.Fatalf("prune: %v", err)
	}

	if got := countRows(t, store, "activity_stats", target); got != 1 {
		t.Errorf("target activity rows = %d, want 1", got)
	}
	for _, k := range []SnapshotKey{otherDB, otherProject} {
		if got := countRows(t, store, "activity_stats", k); got != 2 {
			t.Errorf("%s/%s activity rows = %d, want 2 (untouched)", k.ProjectID, k.DatabaseID, got)
		}
		if got := countRows(t, store, "query_stats", k); got != 2 {
			t.Errorf("%s/%s query rows = %d, want 2 (untouched)", k.ProjectID, k.DatabaseID, got)
		}
	}
}

// Retention covers the stats tables only. Schema snapshots and planner stats
// are excluded by design — pruning a schema snapshot would strand the stats
// rows that reference its hash.
func TestPrune_LeavesSchemaAndPlannerRows(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-140 * 24 * time.Hour)

	p := plannerFixture("sref-A", "ch-A", "appdb")
	p.Timestamp = old
	if _, err := store.PutPlanner(ctx, k, p); err != nil {
		t.Fatalf("put planner: %v", err)
	}
	putActivityAt(t, store, k, "node-a", "act-old", old)

	if _, err := pruneStatsOnly(ctx, store, k, now.Add(-90*24*time.Hour)); err != nil {
		t.Fatalf("prune: %v", err)
	}

	if got := countRows(t, store, "planner_stats", k); got != 1 {
		t.Errorf("planner rows = %d, want 1 (never pruned)", got)
	}
}

// A cutoff older than everything is a no-op, and an empty store prunes cleanly
// rather than erroring.
func TestPrune_NoMatchingRows(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	n, err := pruneStatsOnly(ctx, store, k, time.Now().UTC().Add(-90*24*time.Hour))
	if err != nil {
		t.Fatalf("prune on empty store: %v", err)
	}
	if n != 0 {
		t.Errorf("pruned %d rows from empty store, want 0", n)
	}

	now := time.Now().UTC().Truncate(time.Second)
	putActivityAt(t, store, k, "node-a", "act-1", now.Add(-time.Hour))
	putActivityAt(t, store, k, "node-a", "act-2", now)

	n, err = pruneStatsOnly(ctx, store, k, now.Add(-365*24*time.Hour))
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 0 {
		t.Errorf("pruned %d rows, want 0 (all newer than cutoff)", n)
	}
	if got := countRows(t, store, "activity_stats", k); got != 2 {
		t.Errorf("activity rows = %d, want 2", got)
	}
}
