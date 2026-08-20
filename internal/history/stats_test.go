package history

import (
	"context"
	"fmt"
	"hash/fnv"
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

// PutQueryStats recomputes the digest from the raw members, so the caller's
// contentHash is only a label: fold it into a member queryid, otherwise every
// fixture hashes alike and the unique index collapses them into one row.
func queryStatsFixture(schemaRef, contentHash, source string) *schema.QueryStatsSnapshot {
	h := fnv.New64a()
	h.Write([]byte(contentHash))
	queryID := int64(h.Sum64() >> 1)

	return &schema.QueryStatsSnapshot{
		SchemaRefHash: schemaRef,
		ContentHash:   contentHash,
		Node: schema.NodeIdentity{
			Source: source, PgVersion: "PostgreSQL 17.0",
			Timestamp: time.Now().UTC().Truncate(time.Second),
		},
		Queries: []schema.QueryStatsEntry{{
			Fingerprint: "sha1:abc",
			Canonical:   "SELECT id FROM users WHERE id = $1",
			Members:     []schema.QueryStatsMember{{QueryID: queryID, Calls: 5}},
			Calls:       5,
		}},
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

// LatestNodeRole backs the v0.16 role-flip guard on `snapshot activity`. It
// answers "what role did this label last capture as" WITHOUT a node_role column
// -- the column, its backfill and the migration machinery are v0.17 -- by
// reading $.node.is_standby back out of the stored payload with SQLite's
// json_extract.
//
// That makes three things load-bearing that no compiler checks, so they are
// pinned here:
//
//   - NodeIdentity.IsStandby has NO omitempty, so a primary serializes
//     "is_standby":false and json_extract yields 0, not NULL. If that tag ever
//     gains omitempty, every primary silently reads as unknown and the guard
//     quietly stops guarding. The "primary" subtests below fail if that happens.
//   - a row with no recorded role must read as unknown, never as primary --
//     guessing would refuse legitimate captures on legacy rows.
//   - the newest row wins, including across a promotion, and timestamps are
//     only second-granularity today, so the id tiebreak carries same-second
//     captures.
func TestLatestNodeRole(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	key := SnapshotKey{ProjectID: "p", DatabaseID: "d"}

	if _, err := store.PutActivity(ctx, key, activityFixture("sr", "ac-1", "node-a", true)); err != nil {
		t.Fatal(err)
	}
	// node-b never captured activity, only query stats: the fallback arm
	if _, err := store.PutQueryStats(ctx, key, queryStatsFixture("sr", "qs-1", "node-b")); err != nil {
		t.Fatal(err)
	}

	t.Run("standby read from activity_stats", func(t *testing.T) {
		got, err := store.LatestNodeRole(ctx, key, "node-a")
		if err != nil {
			t.Fatal(err)
		}
		if got != NodeRoleStandby {
			t.Errorf("got %q, want %q", got, NodeRoleStandby)
		}
	})

	// queryStatsFixture builds a non-standby node, so this also pins that
	// json_extract of a JSON false is 0 and maps to primary rather than unknown
	t.Run("primary read from query_stats when activity has no row", func(t *testing.T) {
		got, err := store.LatestNodeRole(ctx, key, "node-b")
		if err != nil {
			t.Fatal(err)
		}
		if got != NodeRolePrimary {
			t.Errorf("got %q, want %q -- a JSON false must not read as unknown", got, NodeRolePrimary)
		}
	})

	t.Run("label never captured is unknown", func(t *testing.T) {
		got, err := store.LatestNodeRole(ctx, key, "never-seen")
		if err != nil {
			t.Fatal(err)
		}
		if got != NodeRoleUnknown {
			t.Errorf("got %q, want unknown", got)
		}
	})

	// a label is scoped to its (project, database); another project's rows
	// under the same label must not answer for it
	t.Run("other project's rows do not leak in", func(t *testing.T) {
		other := SnapshotKey{ProjectID: "other", DatabaseID: "d"}
		got, err := store.LatestNodeRole(ctx, other, "node-a")
		if err != nil {
			t.Fatal(err)
		}
		if got != NodeRoleUnknown {
			t.Errorf("got %q for a different project, want unknown", got)
		}
	})

	t.Run("newest row wins after a promotion", func(t *testing.T) {
		if _, err := store.PutActivity(ctx, key, activityFixture("sr", "ac-2", "node-a", false)); err != nil {
			t.Fatal(err)
		}
		got, err := store.LatestNodeRole(ctx, key, "node-a")
		if err != nil {
			t.Fatal(err)
		}
		if got != NodeRolePrimary {
			t.Errorf("got %q, want %q -- the newer row should win", got, NodeRolePrimary)
		}
	})

	// timestamps are second-granularity RFC3339 strings, so two captures inside
	// one second tie and only the id tiebreak resolves them
	t.Run("same-second captures resolve by id", func(t *testing.T) {
		ts := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
		for i, standby := range []bool{false, true} {
			payload := fmt.Sprintf(`{"node":{"source":"tied","is_standby":%t}}`, standby)
			if _, err := store.db.ExecContext(ctx,
				`INSERT INTO activity_stats
				   (project_id, database_id, schema_ref_hash, content_hash, node_source, timestamp, payload_json)
				   VALUES (?, ?, 'sr', ?, 'tied', ?, ?)`,
				string(key.ProjectID), string(key.DatabaseID),
				fmt.Sprintf("tied-%d", i), ts, payload); err != nil {
				t.Fatal(err)
			}
		}
		got, err := store.LatestNodeRole(ctx, key, "tied")
		if err != nil {
			t.Fatal(err)
		}
		// the standby row was inserted second, so it holds the higher id
		if got != NodeRoleStandby {
			t.Errorf("got %q, want %q -- the later insert should win the tie", got, NodeRoleStandby)
		}
	})

	// a payload with no $.node.is_standby at all: json_extract returns NULL and
	// the guard must stay silent rather than invent a role to compare against
	t.Run("payload without the field is unknown, not primary", func(t *testing.T) {
		if _, err := store.db.ExecContext(ctx,
			`INSERT INTO activity_stats
			   (project_id, database_id, schema_ref_hash, content_hash, node_source, timestamp, payload_json)
			   VALUES (?, ?, 'sr', 'legacy-1', 'node-legacy', ?, '{"tables":[]}')`,
			string(key.ProjectID), string(key.DatabaseID),
			time.Now().UTC().Format(time.RFC3339)); err != nil {
			t.Fatal(err)
		}
		got, err := store.LatestNodeRole(ctx, key, "node-legacy")
		if err != nil {
			t.Fatal(err)
		}
		if got != NodeRoleUnknown {
			t.Errorf("got %q, want unknown -- a missing field must never read as primary", got)
		}
	})
}
