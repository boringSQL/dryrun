package history

import (
	"context"
	"errors"
	"testing"
	"time"
)

// seedSchema inserts a schema snapshot at a fixed timestamp so ordering is
// deterministic across the test.
func seedSchema(t *testing.T, store *Store, k SnapshotKey, hash string, ts time.Time) {
	t.Helper()
	s := testSnapshot(hash, string(k.DatabaseID))
	s.Timestamp = ts
	if _, err := store.PutSchema(context.Background(), k, s); err != nil {
		t.Fatal(err)
	}
}

func countRows(t *testing.T, store *Store, table string, k SnapshotKey) int {
	t.Helper()
	var n int
	if err := store.db.QueryRow(
		"SELECT COUNT(*) FROM "+table+" WHERE project_id = ? AND database_id = ?",
		string(k.ProjectID), string(k.DatabaseID)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// ResolveSchemaSnapshot matches a unique prefix and returns the full summary,
// but rejects a prefix that hits more than one row so a delete can't be
// misdirected.
func TestResolveSchemaSnapshot(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	base := time.Now().UTC().Truncate(time.Second)
	seedSchema(t, store, k, "abc111", base)
	seedSchema(t, store, k, "abc222", base.Add(time.Minute))
	seedSchema(t, store, k, "def333", base.Add(2*time.Minute))

	got, err := store.ResolveSchemaSnapshot(ctx, k, "def")
	if err != nil {
		t.Fatalf("unique prefix: %v", err)
	}
	if got.ContentHash != "def333" {
		t.Errorf("resolved hash = %q, want def333", got.ContentHash)
	}
	if got.ID == 0 {
		t.Error("resolved summary carries no rowid")
	}

	if _, err := store.ResolveSchemaSnapshot(ctx, k, "abc"); err == nil {
		t.Error("ambiguous prefix should error")
	}

	_, err = store.ResolveSchemaSnapshot(ctx, k, "zzz")
	if !errors.Is(err, ErrSnapshotNotFound) {
		t.Errorf("missing prefix: got %v, want ErrSnapshotNotFound", err)
	}
}

// Deleting a schema snapshot cascades to the planner/activity/query stats bound
// to it via schema_ref_hash, and leaves other snapshots untouched.
func TestDeleteSchemaSnapshotCascades(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	base := time.Now().UTC().Truncate(time.Second)
	seedSchema(t, store, k, "keep-me", base)
	seedSchema(t, store, k, "drop-me", base.Add(time.Minute))

	// bind stats to the doomed schema
	if _, err := store.PutPlanner(ctx, k, plannerFixture("drop-me", "p-1", "appdb")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutActivity(ctx, k, activityFixture("drop-me", "a-1", "primary", false)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutQueryStats(ctx, k, queryStatsFixture("drop-me", "q-1", "primary")); err != nil {
		t.Fatal(err)
	}
	// stats bound to the survivor must not be swept up
	if _, err := store.PutPlanner(ctx, k, plannerFixture("keep-me", "p-2", "appdb")); err != nil {
		t.Fatal(err)
	}

	target, err := store.ResolveSchemaSnapshot(ctx, k, "drop-me")
	if err != nil {
		t.Fatal(err)
	}
	res, err := store.DeleteSchemaSnapshot(ctx, k, target)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !res.Cascaded {
		t.Error("expected cascade with no content twin present")
	}
	if res.PlannerRemoved != 1 || res.ActivityRemoved != 1 || res.QueryStatsRemoved != 1 {
		t.Errorf("removed planner=%d activity=%d query_stats=%d, want 1/1/1",
			res.PlannerRemoved, res.ActivityRemoved, res.QueryStatsRemoved)
	}

	if n := countRows(t, store, "snapshots", k); n != 1 {
		t.Errorf("snapshots left = %d, want 1", n)
	}
	if n := countRows(t, store, "planner_stats", k); n != 1 {
		t.Errorf("planner_stats left = %d, want 1 (survivor's)", n)
	}
	if n := countRows(t, store, "activity_stats", k); n != 0 {
		t.Errorf("activity_stats left = %d, want 0", n)
	}
	if n := countRows(t, store, "query_stats", k); n != 0 {
		t.Errorf("query_stats left = %d, want 0", n)
	}

	// survivor still resolvable
	if _, err := store.ResolveSchemaSnapshot(ctx, k, "keep-me"); err != nil {
		t.Errorf("survivor gone: %v", err)
	}
}

// When a content-identical schema row still exists, the bound stats stay put —
// they remain valid for the surviving twin.
func TestDeleteSchemaSnapshotKeepsStatsWhenTwinRemains(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	base := time.Now().UTC().Truncate(time.Second)
	// PutSchema dedups consecutive identical hashes, so twins only arise
	// non-consecutively: A -> B -> A (schema drifted, then reverted).
	seedSchema(t, store, k, "same-hash", base)
	seedSchema(t, store, k, "other", base.Add(time.Minute))
	seedSchema(t, store, k, "same-hash", base.Add(2*time.Minute))
	if _, err := store.PutPlanner(ctx, k, plannerFixture("same-hash", "p-1", "appdb")); err != nil {
		t.Fatal(err)
	}

	// delete the newer of the twins by rowid (ResolveSchemaSnapshot would reject
	// the ambiguous hash, so target the latest directly)
	target, err := store.LatestSchema(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	res, err := store.DeleteSchemaSnapshot(ctx, k, *target)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if res.Cascaded {
		t.Error("cascade must not run while a content twin survives")
	}
	if n := countRows(t, store, "snapshots", k); n != 2 {
		t.Errorf("snapshots left = %d, want 2", n)
	}
	if n := countRows(t, store, "planner_stats", k); n != 1 {
		t.Errorf("planner_stats left = %d, want 1", n)
	}
}

// Deleting an already-gone rowid is a not-found error, not a silent success.
func TestDeleteSchemaSnapshotMissing(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	_, err := store.DeleteSchemaSnapshot(ctx, k, SnapshotSummary{ID: 999, ContentHash: "nope"})
	if !errors.Is(err, ErrSnapshotNotFound) {
		t.Errorf("got %v, want ErrSnapshotNotFound", err)
	}
}

// ResolveSnapshot resolves a hash prefix against any of the three kinds the
// "snapshot list" surface prints, not just schema. This is the regression the
// user hit: a planner/activity content hash copied from the listing was
// rejected because delete only searched the schema table.
func TestResolveSnapshotAcrossKinds(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	seedSchema(t, store, k, "5cheema00", time.Now().UTC().Truncate(time.Second))
	if _, err := store.PutPlanner(ctx, k, plannerFixture("5cheema00", "p1annel0", "appdb")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutActivity(ctx, k, activityFixture("5cheema00", "acc1de00", "primary", false)); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		prefix   string
		wantKind SnapshotKindTag
		wantHash string
	}{
		{"5cheem", KindSchema, "5cheema00"},
		{"p1anne", KindPlanner, "p1annel0"},
		{"acc1de", KindActivity, "acc1de00"},
	}
	for _, tc := range cases {
		got, err := store.ResolveSnapshot(ctx, k, tc.prefix)
		if err != nil {
			t.Fatalf("prefix %q: %v", tc.prefix, err)
		}
		if got.Kind.Tag != tc.wantKind {
			t.Errorf("prefix %q kind = %v, want %v", tc.prefix, got.Kind.Tag, tc.wantKind)
		}
		if got.ContentHash != tc.wantHash {
			t.Errorf("prefix %q hash = %q, want %q", tc.prefix, got.ContentHash, tc.wantHash)
		}
		if got.ID == 0 {
			t.Errorf("prefix %q resolved summary carries no rowid", tc.prefix)
		}
	}

	if _, err := store.ResolveSnapshot(ctx, k, "zzz"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Errorf("missing prefix: got %v, want ErrSnapshotNotFound", err)
	}
}

// A prefix that hits rows in more than one table is ambiguous and must be
// rejected rather than deleting whichever kind happened to sort first.
func TestResolveSnapshotAmbiguousAcrossKinds(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	seedSchema(t, store, k, "dup00000", time.Now().UTC().Truncate(time.Second))
	if _, err := store.PutPlanner(ctx, k, plannerFixture("dup00000", "dup00001", "appdb")); err != nil {
		t.Fatal(err)
	}

	if _, err := store.ResolveSnapshot(ctx, k, "dup000"); err == nil {
		t.Error("prefix matching both schema and planner should be ambiguous")
	}
}

// DeleteSnapshot dispatches on the resolved kind: a planner/activity row is
// removed on its own, leaving the schema snapshot it was bound to intact.
func TestDeleteSnapshotStatsKinds(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	seedSchema(t, store, k, "keepschema", time.Now().UTC().Truncate(time.Second))
	if _, err := store.PutPlanner(ctx, k, plannerFixture("keepschema", "dropplan", "appdb")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutActivity(ctx, k, activityFixture("keepschema", "dropact", "primary", false)); err != nil {
		t.Fatal(err)
	}

	plan, err := store.ResolveSnapshot(ctx, k, "dropplan")
	if err != nil {
		t.Fatal(err)
	}
	res, err := store.DeleteSnapshot(ctx, k, plan)
	if err != nil {
		t.Fatalf("delete planner: %v", err)
	}
	if res.Cascaded {
		t.Error("deleting a planner row must not report a schema cascade")
	}

	act, err := store.ResolveSnapshot(ctx, k, "dropact")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeleteSnapshot(ctx, k, act); err != nil {
		t.Fatalf("delete activity: %v", err)
	}

	if n := countRows(t, store, "planner_stats", k); n != 0 {
		t.Errorf("planner_stats left = %d, want 0", n)
	}
	if n := countRows(t, store, "activity_stats", k); n != 0 {
		t.Errorf("activity_stats left = %d, want 0", n)
	}
	if n := countRows(t, store, "snapshots", k); n != 1 {
		t.Errorf("schema snapshot swept up: snapshots left = %d, want 1", n)
	}
}

// Deleting an already-gone stats rowid is a not-found error, matching the
// schema path.
func TestDeleteSnapshotStatsMissing(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	_, err := store.DeleteSnapshot(ctx, k, SnapshotSummary{ID: 999, Kind: PlannerKind(), ContentHash: "nope"})
	if !errors.Is(err, ErrSnapshotNotFound) {
		t.Errorf("got %v, want ErrSnapshotNotFound", err)
	}
}
