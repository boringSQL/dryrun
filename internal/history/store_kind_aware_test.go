package history

import (
	"context"
	"testing"
	"time"
)

// TestStoredSnapshotRoundTrip drives a StoredSnapshot of each variant
// (Schema, Planner, Activity, Query) through the generic Put -> Get -> List
// path on the SQLite Store. This is the contract the V2 FilesystemStore will
// implement against the same interface, so kind dispatch must be lossless:
// what goes in via Put(WrapX) must come back out via Get(kind) and surface
// in List(kind) with the right ContentHash and SchemaRefHash. Query stats
// were bolted on after schema/planner/activity, riding the same StoredSnapshot
// tagged union (WrapQueryStats/AsQueryStats added to snapshot_store.go), so
// this test is the one place that proves the fourth variant didn't get a
// half-finished Kind()/Timestamp()/ContentHash()/SchemaRefHash() case.
func TestStoredSnapshotRoundTrip(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	schemaSnap := testSnapshot("schema-hash-1", "appdb")
	if _, err := store.Put(ctx, k, WrapSchema(schemaSnap)); err != nil {
		t.Fatalf("put schema: %v", err)
	}
	plannerSnap := plannerFixture(schemaSnap.ContentHash, "planner-hash-1", "appdb")
	if _, err := store.Put(ctx, k, WrapPlanner(plannerSnap)); err != nil {
		t.Fatalf("put planner: %v", err)
	}
	activitySnap := activityFixture(schemaSnap.ContentHash, "activity-hash-1", "primary", false)
	if _, err := store.Put(ctx, k, WrapActivity(activitySnap)); err != nil {
		t.Fatalf("put activity: %v", err)
	}
	queryStatsSnap := queryStatsFixture(schemaSnap.ContentHash, "query-hash-1", "primary")
	if _, err := store.Put(ctx, k, WrapQueryStats(queryStatsSnap)); err != nil {
		t.Fatalf("put query stats: %v", err)
	}

	t.Run("Get_Schema", func(t *testing.T) {
		got, err := store.Get(ctx, k, SchemaKind(), NewRefHash("schema-hash-1"))
		if err != nil {
			t.Fatal(err)
		}
		if got.AsSchema() == nil || got.AsSchema().ContentHash != "schema-hash-1" {
			t.Errorf("got %+v, want schema-hash-1", got.AsSchema())
		}
	})

	t.Run("Get_Planner", func(t *testing.T) {
		got, err := store.Get(ctx, k, PlannerKind(), NewRefHash("planner-hash-1"))
		if err != nil {
			t.Fatal(err)
		}
		if got.AsPlanner() == nil || got.AsPlanner().ContentHash != "planner-hash-1" {
			t.Errorf("got %+v, want planner-hash-1", got.AsPlanner())
		}
		if got.SchemaRefHash() != schemaSnap.ContentHash {
			t.Errorf("schema_ref_hash mismatch: got %q, want %q", got.SchemaRefHash(), schemaSnap.ContentHash)
		}
	})

	t.Run("Get_Activity_ByNodeLabel", func(t *testing.T) {
		got, err := store.Get(ctx, k, ActivityKind("primary"), NewRefHash("activity-hash-1"))
		if err != nil {
			t.Fatal(err)
		}
		if got.AsActivity() == nil || got.AsActivity().ContentHash != "activity-hash-1" {
			t.Errorf("got %+v, want activity-hash-1", got.AsActivity())
		}
		if got.Kind().NodeLabel != "primary" {
			t.Errorf("node label: got %q, want primary", got.Kind().NodeLabel)
		}
	})

	// The Query case is the point of this test: before the sync/dispatch work,
	// StoredSnapshot only knew how to be a schema, a planner, or an activity row,
	// and Get/Kind/SchemaRefHash all had three-armed switches. If a fourth arm
	// were ever missing, this subtest is what would notice — AsQueryStats()
	// would come back nil even though the row is genuinely there in query_stats.
	t.Run("Get_QueryStats_ByNodeLabel", func(t *testing.T) {
		got, err := store.Get(ctx, k, QueryKind("primary"), NewRefHash("query-hash-1"))
		if err != nil {
			t.Fatal(err)
		}
		if got.AsQueryStats() == nil || got.AsQueryStats().ContentHash != "query-hash-1" {
			t.Errorf("got %+v, want query-hash-1", got.AsQueryStats())
		}
		if got.Kind().NodeLabel != "primary" {
			t.Errorf("node label: got %q, want primary", got.Kind().NodeLabel)
		}
		if got.SchemaRefHash() != schemaSnap.ContentHash {
			t.Errorf("schema_ref_hash mismatch: got %q, want %q", got.SchemaRefHash(), schemaSnap.ContentHash)
		}
	})

	t.Run("List_per_kind", func(t *testing.T) {
		sl, err := store.List(ctx, k, SchemaKind(), TimeRange{})
		if err != nil || len(sl) != 1 || sl[0].ContentHash != "schema-hash-1" {
			t.Errorf("schema list: got %+v err=%v", sl, err)
		}
		pl, err := store.List(ctx, k, PlannerKind(), TimeRange{})
		if err != nil || len(pl) != 1 || pl[0].ContentHash != "planner-hash-1" {
			t.Errorf("planner list: got %+v err=%v", pl, err)
		}
		al, err := store.List(ctx, k, ActivityKind(""), TimeRange{})
		if err != nil || len(al) != 1 || al[0].ContentHash != "activity-hash-1" || al[0].NodeLabel != "primary" {
			t.Errorf("activity list: got %+v err=%v", al, err)
		}
		ql, err := store.List(ctx, k, QueryKind(""), TimeRange{})
		if err != nil || len(ql) != 1 || ql[0].ContentHash != "query-hash-1" || ql[0].NodeLabel != "primary" {
			t.Errorf("query stats list: got %+v err=%v", ql, err)
		}
	})
}

// TestListKindsReportsPopulatedSubset seeds only schema + one activity node
// (no planner row), then asserts ListKinds returns exactly those kinds and
// that activity entries carry the node label. The omission of planner is
// the discriminating case: a partially-populated key must not advertise the
// empty stream.
func TestListKindsReportsPopulatedSubset(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	schemaSnap := testSnapshot("sh-1", "appdb")
	if _, err := store.PutSchema(ctx, k, schemaSnap); err != nil {
		t.Fatal(err)
	}
	a := activityFixture(schemaSnap.ContentHash, "ac-1", "replica-a", true)
	if _, err := store.PutActivity(ctx, k, a); err != nil {
		t.Fatal(err)
	}

	kinds, err := store.ListKinds(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if len(kinds) != 2 {
		t.Fatalf("got %d kinds (%+v), want 2 (schema, activity:replica-a)", len(kinds), kinds)
	}
	if kinds[0].Tag != KindSchema {
		t.Errorf("kinds[0] = %v, want schema", kinds[0])
	}
	if kinds[1].Tag != KindActivity || kinds[1].NodeLabel != "replica-a" {
		t.Errorf("kinds[1] = %v, want activity:replica-a", kinds[1])
	}
}

// TestListKindsActivityMultiNode confirms each distinct node_source surfaces
// as its own ActivityKind entry, matching the bundle-by-node semantics V2's
// FilesystemStore must preserve.
func TestListKindsActivityMultiNode(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	schemaSnap := testSnapshot("sh-1", "appdb")
	if _, err := store.PutSchema(ctx, k, schemaSnap); err != nil {
		t.Fatal(err)
	}
	for _, src := range []string{"replica-b", "replica-a", "primary"} {
		a := activityFixture(schemaSnap.ContentHash, "ac-"+src, src, src != "primary")
		if _, err := store.PutActivity(ctx, k, a); err != nil {
			t.Fatal(err)
		}
	}

	kinds, err := store.ListKinds(ctx, k)
	if err != nil {
		t.Fatal(err)
	}

	var labels []string
	for _, kk := range kinds {
		if kk.Tag == KindActivity {
			labels = append(labels, kk.NodeLabel)
		}
	}
	want := []string{"primary", "replica-a", "replica-b"}
	if len(labels) != len(want) {
		t.Fatalf("activity labels: got %v, want %v", labels, want)
	}
	for i := range want {
		if labels[i] != want[i] {
			t.Errorf("labels[%d]: got %q, want %q", i, labels[i], want[i])
		}
	}
}

// TestListKindsQueryMultiNode is TestListKindsActivityMultiNode's twin for
// query stats. Query stats are captured per-node exactly the way activity is
// (a primary and every replica each run their own pg_stat_statements), and
// ListKinds' query_stats branch was written by copying the activity_stats
// SELECT DISTINCT node_source query and swapping the table name — so the
// failure mode this guards against is a copy-paste slip that left the query
// branch reading from the wrong table, or an off-by-one that dropped one
// node's label.
func TestListKindsQueryMultiNode(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	schemaSnap := testSnapshot("sh-1", "appdb")
	if _, err := store.PutSchema(ctx, k, schemaSnap); err != nil {
		t.Fatal(err)
	}
	for _, src := range []string{"replica-b", "replica-a", "primary"} {
		q := queryStatsFixture(schemaSnap.ContentHash, "qh-"+src, src)
		if _, err := store.PutQueryStats(ctx, k, q); err != nil {
			t.Fatal(err)
		}
	}

	kinds, err := store.ListKinds(ctx, k)
	if err != nil {
		t.Fatal(err)
	}

	var labels []string
	for _, kk := range kinds {
		if kk.Tag == KindQuery {
			labels = append(labels, kk.NodeLabel)
		}
	}
	// ListKinds' node_source query carries an ORDER BY, so the query-stats
	// labels must come back alphabetical regardless of insertion order
	// (we inserted replica-b, replica-a, primary — deliberately out of order).
	want := []string{"primary", "replica-a", "replica-b"}
	if len(labels) != len(want) {
		t.Fatalf("query stats labels: got %v, want %v", labels, want)
	}
	for i := range want {
		if labels[i] != want[i] {
			t.Errorf("labels[%d]: got %q, want %q", i, labels[i], want[i])
		}
	}

	// And query_stats must not have displaced activity/planner as separate
	// kinds — ListKinds is additive across all four tables, not a replacement.
	var hasSchema, hasQuery bool
	for _, kk := range kinds {
		switch kk.Tag {
		case KindSchema:
			hasSchema = true
		case KindQuery:
			hasQuery = true
		}
	}
	if !hasSchema || !hasQuery {
		t.Errorf("expected both schema and query kinds present, got %+v", kinds)
	}
}

// TestLatestPicksPerKind: with planner and activity rows that are newer than
// the schema row, Latest(SchemaKind) must still return the schema timestamp,
// not the most-recent-across-kinds. Kind dispatch on Latest must isolate
// streams; otherwise V3's per-kind sync diff would compare apples to oranges.
func TestLatestPicksPerKind(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	schemaSnap := testSnapshot("sh-old", "appdb")
	schemaSnap.Timestamp = now.Add(-2 * time.Hour)
	if _, err := store.PutSchema(ctx, k, schemaSnap); err != nil {
		t.Fatal(err)
	}

	planner := plannerFixture(schemaSnap.ContentHash, "pl-newer", "appdb")
	planner.Timestamp = now.Add(-30 * time.Minute)
	if _, err := store.PutPlanner(ctx, k, planner); err != nil {
		t.Fatal(err)
	}

	activity := activityFixture(schemaSnap.ContentHash, "ac-newest", "primary", false)
	activity.Node.Timestamp = now
	if _, err := store.PutActivity(ctx, k, activity); err != nil {
		t.Fatal(err)
	}

	// Query stats are captured at yet another timestamp — squarely between
	// planner and activity — precisely so this test can't pass by accident
	// (e.g. a bug that always returns the single newest row across all four
	// tables would still coincidentally get schema/planner/activity right if
	// query happened to share a timestamp with one of them).
	queryStats := queryStatsFixture(schemaSnap.ContentHash, "qh-newer", "primary")
	queryStats.Node.Timestamp = now.Add(-15 * time.Minute)
	if _, err := store.PutQueryStats(ctx, k, queryStats); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name     string
		kind     SnapshotKind
		wantHash string
	}{
		{"schema", SchemaKind(), "sh-old"},
		{"planner", PlannerKind(), "pl-newer"},
		{"activity", ActivityKind("primary"), "ac-newest"},
		{"query", QueryKind("primary"), "qh-newer"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := store.Latest(ctx, k, c.kind)
			if err != nil || got == nil {
				t.Fatalf("got (%+v, %v)", got, err)
			}
			if got.ContentHash != c.wantHash {
				t.Errorf("got %q, want %q", got.ContentHash, c.wantHash)
			}
		})
	}
}

// TestDeleteBeforePerKindIsolated: DeleteBefore on planner must not affect
// schema, activity, or query rows. The retention path in V3 will iterate per
// kind, so cross-kind cascade would silently prune unrelated streams. Query
// stats are included here specifically because DeleteBefore's KindActivity
// and KindQuery cases were both refactored to share a single
// deleteNodeStatsBefore(table string, ...) helper — the exact kind of change
// where a copy-paste of the table name argument could quietly make one kind's
// DeleteBefore call delete from the other kind's table instead.
func TestDeleteBeforePerKindIsolated(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	now := time.Now().UTC().Truncate(time.Second)
	old := func(t time.Time) time.Time { return t.Add(-24 * time.Hour) }

	s := testSnapshot("sh-1", "appdb")
	s.Timestamp = old(now)
	if _, err := store.PutSchema(ctx, k, s); err != nil {
		t.Fatal(err)
	}
	p := plannerFixture("sh-1", "pl-1", "appdb")
	p.Timestamp = old(now)
	if _, err := store.PutPlanner(ctx, k, p); err != nil {
		t.Fatal(err)
	}
	a := activityFixture("sh-1", "ac-1", "primary", false)
	a.Node.Timestamp = old(now)
	if _, err := store.PutActivity(ctx, k, a); err != nil {
		t.Fatal(err)
	}
	q := queryStatsFixture("sh-1", "qh-1", "primary")
	q.Node.Timestamp = old(now)
	if _, err := store.PutQueryStats(ctx, k, q); err != nil {
		t.Fatal(err)
	}

	n, err := store.DeleteBefore(ctx, k, PlannerKind(), now)
	if err != nil || n != 1 {
		t.Fatalf("delete planner: n=%d err=%v", n, err)
	}

	sl, _ := store.List(ctx, k, SchemaKind(), TimeRange{})
	pl, _ := store.List(ctx, k, PlannerKind(), TimeRange{})
	al, _ := store.List(ctx, k, ActivityKind(""), TimeRange{})
	ql, _ := store.List(ctx, k, QueryKind(""), TimeRange{})
	if len(sl) != 1 || len(pl) != 0 || len(al) != 1 || len(ql) != 1 {
		t.Errorf("after delete planner: schema=%d planner=%d activity=%d query=%d, want 1/0/1/1",
			len(sl), len(pl), len(al), len(ql))
	}

	// Now the mirror case: delete the old query row and confirm activity — the
	// other tenant of deleteNodeStatsBefore — survives untouched.
	qn, err := store.DeleteBefore(ctx, k, QueryKind(""), now)
	if err != nil || qn != 1 {
		t.Fatalf("delete query stats: n=%d err=%v", qn, err)
	}
	al2, _ := store.List(ctx, k, ActivityKind(""), TimeRange{})
	ql2, _ := store.List(ctx, k, QueryKind(""), TimeRange{})
	if len(al2) != 1 || len(ql2) != 0 {
		t.Errorf("after delete query: activity=%d query=%d, want 1/0", len(al2), len(ql2))
	}
}
