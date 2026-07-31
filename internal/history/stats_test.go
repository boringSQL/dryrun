package history

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

func plannerFixture(schemaRef, contentHash, database string) *schema.PlannerStatsSnapshot {
	return &schema.PlannerStatsSnapshot{
		SchemaRefHash: schemaRef,
		ContentHash:   contentHash,
		Database:      database,
		Timestamp:     time.Now().UTC().Truncate(time.Second),
		Tables: []schema.TableSizingEntry{
			{Table: schema.QualifiedName{Schema: "public", Name: "users"},
				Sizing: schema.TableSizing{Reltuples: 100, Relpages: 5, TableSize: 8192}},
		},
	}
}

func activityFixture(schemaRef, contentHash, source string, standby bool) *schema.ActivityStatsSnapshot {
	return &schema.ActivityStatsSnapshot{
		SchemaRefHash: schemaRef,
		ContentHash:   contentHash,
		Node: schema.NodeIdentity{
			Source: source, IsStandby: standby, PgVersion: "PostgreSQL 17.0",
			Timestamp: time.Now().UTC().Truncate(time.Second),
		},
		Tables: []schema.TableActivityEntry{
			{Table: schema.QualifiedName{Schema: "public", Name: "users"},
				Activity: schema.TableActivity{SeqScan: 1, IdxScan: 2}},
		},
	}
}

func queryStatsFixture(schemaRef, contentHash, source string) *schema.QueryStatsSnapshot {
	return &schema.QueryStatsSnapshot{
		SchemaRefHash: schemaRef,
		ContentHash:   contentHash,
		Node: schema.NodeIdentity{
			Source: source, PgVersion: "PostgreSQL 17.0",
			Timestamp: time.Now().UTC().Truncate(time.Second),
		},
		Queries: []schema.QueryStatsEntry{
			{Fingerprint: "sha1:abc", Canonical: "SELECT id FROM users WHERE id = $1", Calls: 5},
		},
	}
}

// PutPlanner is idempotent on (schema_ref_hash, content_hash) — re-putting
// the exact same payload must collapse to a deduped no-op so probe loops
// running on a cron don't bloat history.db with byte-identical rows.
func TestPutPlanner_IdempotentOnSchemaRefAndContentHash(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	p := plannerFixture("sref-A", "ch-A", "appdb")

	out, err := store.PutPlanner(ctx, k, p)
	if err != nil {
		t.Fatalf("first put: %v", err)
	}
	if out != PutInserted {
		t.Errorf("first put outcome = %v, want PutInserted", out)
	}

	// Same hashes -> dedup, even if Timestamp shifts.
	p2 := *p
	p2.Timestamp = p.Timestamp.Add(5 * time.Minute)
	out, err = store.PutPlanner(ctx, k, &p2)
	if err != nil {
		t.Fatalf("second put: %v", err)
	}
	if out != PutDeduped {
		t.Errorf("duplicate put outcome = %v, want PutDeduped", out)
	}

	// Different content_hash under the same schema_ref must insert a fresh row
	// (e.g. ANALYZE moved row estimates without altering DDL).
	p3 := *p
	p3.ContentHash = "ch-B"
	out, err = store.PutPlanner(ctx, k, &p3)
	if err != nil {
		t.Fatalf("third put: %v", err)
	}
	if out != PutInserted {
		t.Errorf("changed-content put outcome = %v, want PutInserted", out)
	}
}

// LatestPlanner returns the most recent row by timestamp regardless of
// insert order. Confirms the ORDER BY DESC LIMIT 1 contract holds end-to-end.
func TestLatestPlanner_ReturnsMostRecent(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	older := plannerFixture("sref-A", "ch-A", "appdb")
	older.Timestamp = time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Second)

	newer := plannerFixture("sref-A", "ch-B", "appdb")
	newer.Timestamp = time.Now().UTC().Truncate(time.Second)

	if _, err := store.PutPlanner(ctx, k, newer); err != nil {
		t.Fatalf("put newer: %v", err)
	}
	if _, err := store.PutPlanner(ctx, k, older); err != nil {
		t.Fatalf("put older: %v", err)
	}

	got, err := store.LatestPlanner(ctx, k)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if got.ContentHash != "ch-B" {
		t.Errorf("LatestPlanner content_hash = %q, want ch-B", got.ContentHash)
	}
}

// PutActivity is append-only — every call inserts a row in the underlying
// table, even when content_hash repeats. We verify with a direct row count
// rather than via LatestActivity, which collapses per node_source.
func TestPutActivity_AppendsEveryCall(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	base := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 3; i++ {
		a := activityFixture("sref-A", "ach-1", "primary", false)
		a.Node.Timestamp = base.Add(time.Duration(i) * time.Minute)
		if _, err := store.PutActivity(ctx, k, a); err != nil {
			t.Fatalf("put activity #%d: %v", i, err)
		}
	}

	var count int
	if err := store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM activity_stats WHERE project_id = ? AND database_id = ?`,
		string(k.ProjectID), string(k.DatabaseID),
	).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 3 {
		t.Errorf("activity_stats row count = %d, want 3", count)
	}

	// LatestActivity still collapses to one row per node (the most recent).
	latest, err := store.LatestActivity(ctx, k)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if len(latest) != 1 {
		t.Errorf("LatestActivity rows = %d, want 1 (per-node collapse)", len(latest))
	}
}

// PutQueryStats appends every distinct capture; identical content hashes
// dedup via the UNIQUE constraint.
func TestPutQueryStats_AppendsEveryCall(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	base := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 3; i++ {
		q := queryStatsFixture("sref-A", fmt.Sprintf("qch-%d", i), "primary")
		q.Node.Timestamp = base.Add(time.Duration(i) * time.Minute)
		if _, err := store.PutQueryStats(ctx, k, q); err != nil {
			t.Fatalf("put query stats #%d: %v", i, err)
		}
	}

	var count int
	if err := store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM query_stats WHERE project_id = ? AND database_id = ?`,
		string(k.ProjectID), string(k.DatabaseID),
	).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 3 {
		t.Errorf("query_stats row count = %d, want 3", count)
	}

	// GetQueryStats still collapses to one row per node (the most recent).
	latest, err := store.GetQueryStats(ctx, k, "sref-A")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(latest) != 1 {
		t.Errorf("GetQueryStats rows = %d, want 1 (per-node collapse)", len(latest))
	}
}

func TestGetQueryStats_FiltersBySchemaRefHash(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	matched := queryStatsFixture("sref-A", "q-1", "primary")
	drifted := queryStatsFixture("sref-B", "q-2", "replica-x")
	if _, err := store.PutQueryStats(ctx, k, matched); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutQueryStats(ctx, k, drifted); err != nil {
		t.Fatal(err)
	}

	rows, err := store.GetQueryStats(ctx, k, "sref-A")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(rows) != 1 || rows[0].Node.Source != "primary" {
		t.Errorf("GetQueryStats didn't filter by schema_ref: %+v", rows)
	}
}

// LatestActivity returns one row per node_source — the multi-node fanout
// for an HA cluster. Each replica's most recent probe must be represented
// exactly once so AnnotatedSchema can build a per-node MergedActivity.
func TestLatestActivity_OneRowPerNode(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	put := func(source string, contentHash string, at time.Time) {
		a := activityFixture("sref-A", contentHash, source, source != "primary")
		a.Node.Timestamp = at
		if _, err := store.PutActivity(ctx, k, a); err != nil {
			t.Fatalf("put %s/%s: %v", source, contentHash, err)
		}
	}
	put("primary", "p-1", now.Add(-2*time.Minute))
	put("primary", "p-2", now) // newest for primary
	put("replica-a", "r-1", now.Add(-time.Minute))
	put("replica-b", "r-1", now.Add(-30*time.Second))

	rows, err := store.LatestActivity(ctx, k)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("got %d node rows, want 3 (primary, replica-a, replica-b)", len(rows))
	}

	bySource := map[string]string{}
	for _, r := range rows {
		bySource[r.Node.Source] = r.ContentHash
	}
	if bySource["primary"] != "p-2" {
		t.Errorf("primary latest = %q, want p-2", bySource["primary"])
	}
	if bySource["replica-a"] != "r-1" || bySource["replica-b"] != "r-1" {
		t.Errorf("replica latest mismatch: %+v", bySource)
	}
}

// GetActivity scopes results to a given schema_ref_hash, so two nodes
// reporting against drifted DDL don't pollute each other. This is the
// defensive cut MergedActivity relies on.
func TestGetActivity_FiltersBySchemaRefHash(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	matched := activityFixture("sref-A", "a-1", "primary", false)
	drifted := activityFixture("sref-B", "a-2", "replica-x", true)
	if _, err := store.PutActivity(ctx, k, matched); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutActivity(ctx, k, drifted); err != nil {
		t.Fatal(err)
	}

	rows, err := store.GetActivity(ctx, k, "sref-A")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(rows) != 1 || rows[0].Node.Source != "primary" {
		t.Errorf("GetActivity didn't filter by schema_ref: %+v", rows)
	}
}

// GetPlanner targets a specific schema_ref; rows under other schema_refs
// must not bleed through, mirroring the same defensive scoping as activity.
func TestGetPlanner_FiltersBySchemaRefHash(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	if _, err := store.PutPlanner(ctx, k, plannerFixture("sref-A", "p-1", "appdb")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutPlanner(ctx, k, plannerFixture("sref-B", "p-2", "appdb")); err != nil {
		t.Fatal(err)
	}

	got, err := store.GetPlanner(ctx, k, "sref-B")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ContentHash != "p-2" {
		t.Errorf("GetPlanner returned wrong row: %q", got.ContentHash)
	}
}
