package history

import (
	"context"
	"testing"
	"time"
)

// Schema retention is the dangerous half of Prune: DeleteSchemaSnapshot
// cascades every stats row bound to a hash that loses its last snapshot, so
// these tests pin down that reclaiming space never costs history, and that a
// planner row is enough on its own to pin a schema.

func putSchemaAt(t *testing.T, s *Store, k SnapshotKey, hash string, ts time.Time) {
	t.Helper()
	snap := testSnapshot(hash, "acme")
	snap.Timestamp = ts
	if _, err := s.PutSchema(context.Background(), k, snap); err != nil {
		t.Fatalf("put schema %s@%s: %v", hash, ts.Format(time.RFC3339), err)
	}
}

func putPlannerAt(t *testing.T, s *Store, k SnapshotKey, schemaRef, contentHash string, ts time.Time) {
	t.Helper()
	p := plannerFixture(schemaRef, contentHash, "acme")
	p.Timestamp = ts
	if _, err := s.PutPlanner(context.Background(), k, p); err != nil {
		t.Fatalf("put planner %s@%s: %v", contentHash, ts.Format(time.RFC3339), err)
	}
}

func hashesIn(t *testing.T, s *Store, table string, k SnapshotKey) map[string]bool {
	t.Helper()
	col := "content_hash"
	rows, err := s.db.Query(
		"SELECT "+col+" FROM "+table+" WHERE project_id = ? AND database_id = ?",
		string(k.ProjectID), string(k.DatabaseID))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			t.Fatal(err)
		}
		out[h] = true
	}
	return out
}

// A planner row binding an aged schema keeps that schema alive. Without this,
// reclaiming a 7 MB snapshot silently cascades away the planner history that
// annotates it.
func TestPrune_SchemaPinnedByPlannerRow(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-100 * 24 * time.Hour)

	putSchemaAt(t, store, k, "sch-old", old)
	putSchemaAt(t, store, k, "sch-new", now)
	// aged planner row bound to the aged schema, kept by the newest-N floor
	putPlannerAt(t, store, k, "sch-old", "pl-old", old)

	res, err := store.Prune(ctx, k, PruneOptions{
		Cutoff: now.Add(-90 * 24 * time.Hour), KeepSchemas: 1, KeepPlanner: 1,
	})
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if res.Schema != 0 {
		t.Errorf("pruned %d schema rows, want 0 (planner still binds sch-old)", res.Schema)
	}
	if res.SchemaPinned != 1 {
		t.Errorf("SchemaPinned = %d, want 1", res.SchemaPinned)
	}
	if got := countRows(t, store, "planner_stats", k); got != 1 {
		t.Fatalf("planner rows = %d, want 1 — a cascade ate history", got)
	}
}

// Once the planner row that pinned it is gone, the aged schema is removable.
// This is the ordering guarantee: planner runs before schema in one pass, so
// retention is not a no-op that needs a second run to take effect.
func TestPrune_PlannerBeforeSchemaInOnePass(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-100 * 24 * time.Hour)

	putSchemaAt(t, store, k, "sch-old", old)
	putSchemaAt(t, store, k, "sch-new", now)
	putPlannerAt(t, store, k, "sch-old", "pl-old", old)
	putPlannerAt(t, store, k, "sch-new", "pl-new", now)

	res, err := store.Prune(ctx, k, PruneOptions{
		Cutoff: now.Add(-90 * 24 * time.Hour), KeepSchemas: 1, KeepPlanner: 1,
	})
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if res.Planner != 1 {
		t.Errorf("pruned %d planner rows, want 1", res.Planner)
	}
	if res.Schema != 1 {
		t.Errorf("pruned %d schema rows, want 1 (sch-old unpinned in the same pass)", res.Schema)
	}
	if res.SchemaPinned != 0 {
		t.Errorf("SchemaPinned = %d, want 0", res.SchemaPinned)
	}
	if h := hashesIn(t, store, "snapshots", k); !h["sch-new"] || h["sch-old"] {
		t.Errorf("surviving schema hashes = %v, want only sch-new", h)
	}
	if res.BytesFreed <= 0 {
		t.Errorf("BytesFreed = %d, want > 0", res.BytesFreed)
	}
}

// The newest-N floor outranks the cutoff: a project that captured a burst and
// went quiet must keep something for the "latest reads" paths.
func TestPrune_KeepsNewestNSchemasPastCutoff(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	for i, h := range []string{"sch-1", "sch-2", "sch-3", "sch-4"} {
		putSchemaAt(t, store, k, h, now.Add(-time.Duration(200-i*10)*24*time.Hour))
	}

	res, err := store.Prune(ctx, k, PruneOptions{
		Cutoff: now, KeepSchemas: 2, KeepPlanner: 1,
	})
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if res.Schema != 2 {
		t.Errorf("pruned %d schema rows, want 2 (4 aged, newest 2 kept)", res.Schema)
	}
	h := hashesIn(t, store, "snapshots", k)
	if !h["sch-3"] || !h["sch-4"] || h["sch-1"] || h["sch-2"] {
		t.Errorf("surviving schema hashes = %v, want sch-3 and sch-4", h)
	}
}

// KeepSchemas/KeepPlanner of 0 is an explicit opt-out, not an unset default:
// an existing [history] block that names neither must not start deleting
// planner or schema rows on upgrade.
func TestPrune_ZeroKeepDisablesThatKind(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-100 * 24 * time.Hour)
	putSchemaAt(t, store, k, "sch-old", old)
	putSchemaAt(t, store, k, "sch-new", now)
	putPlannerAt(t, store, k, "sch-old", "pl-old", old)

	res, err := store.Prune(ctx, k, PruneOptions{Cutoff: now.Add(-90 * 24 * time.Hour)})
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if res.Planner != 0 || res.Schema != 0 {
		t.Errorf("planner=%d schema=%d, want 0/0 with retention disabled", res.Planner, res.Schema)
	}
	if got := countRows(t, store, "snapshots", k); got != 2 {
		t.Errorf("schema rows = %d, want 2", got)
	}
}

// A content twin outside the candidate set keeps the binding alive, so the
// aged duplicate is removable even though stats still reference the hash.
func TestPrune_RemovesAgedTwinWhenBindingSurvives(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-100 * 24 * time.Hour)

	// PutSchema dedups against the newest row only, so A -> B -> A writes a
	// third row carrying the same hash as the first.
	putSchemaAt(t, store, k, "sch-a", old)
	putSchemaAt(t, store, k, "sch-b", old.Add(time.Hour))
	putSchemaAt(t, store, k, "sch-a", now)
	putPlannerAt(t, store, k, "sch-a", "pl-a", now)

	res, err := store.Prune(ctx, k, PruneOptions{
		Cutoff: now.Add(-90 * 24 * time.Hour), KeepSchemas: 1, KeepPlanner: 1,
	})
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	// the aged sch-a row goes (the recent twin still binds pl-a); sch-b goes
	// because nothing references it
	if res.Schema != 2 {
		t.Errorf("pruned %d schema rows, want 2", res.Schema)
	}
	if got := countRows(t, store, "planner_stats", k); got != 1 {
		t.Fatalf("planner rows = %d, want 1 — the twin did not protect the binding", got)
	}
}

// Retention is key-scoped like the rest of Prune: another project's schema and
// planner rows are untouched.
func TestPrune_RetentionIsKeyScoped(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	target := key("acme", "primary")
	other := key("acme", "reporting")

	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-100 * 24 * time.Hour)
	for _, k := range []SnapshotKey{target, other} {
		putSchemaAt(t, store, k, "sch-old", old)
		putSchemaAt(t, store, k, "sch-new", now)
	}

	if _, err := store.Prune(ctx, target, PruneOptions{
		Cutoff: now.Add(-90 * 24 * time.Hour), KeepSchemas: 1, KeepPlanner: 1,
	}); err != nil {
		t.Fatalf("prune: %v", err)
	}
	if got := countRows(t, store, "snapshots", target); got != 1 {
		t.Errorf("target schema rows = %d, want 1", got)
	}
	if got := countRows(t, store, "snapshots", other); got != 2 {
		t.Errorf("other-key schema rows = %d, want 2 (untouched)", got)
	}
}
