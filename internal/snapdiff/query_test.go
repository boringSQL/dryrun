package snapdiff

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

func mkQuery(schemaRef, hash, node string, ts time.Time, calls int64, ms float64) *snapshot.QueryStatsSnapshot {
	return &snapshot.QueryStatsSnapshot{
		SchemaRefHash: schemaRef, ContentHash: hash, QshapeVersion: 3, RowCap: 500, RawRows: 1,
		Node: snapshot.NodeIdentity{Source: node, PgVersion: "PostgreSQL 17.0", Timestamp: ts},
		Queries: []snapshot.QueryStatsEntry{{
			Fingerprint:     "fp-users",
			Canonical:       "SELECT * FROM users WHERE id = $1",
			Calls:           calls,
			TotalExecTimeMs: ms,
			Members:         []snapshot.QueryStatsMember{{QueryID: 1, Calls: calls}},
		}},
	}
}

// The MCP diff correlates schema, planner and activity around an anchor. Query
// stats were missing, so an agent asking for kind=query got a diff with no
// query content in it.
func TestBuild_CorrelatesQueryStats(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	k := key()
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)

	for _, s := range []struct {
		hash string
		at   time.Time
	}{{"sch-1", t0}, {"sch-2", t1}} {
		if _, err := store.PutSchema(ctx, k, mkSchema(s.hash, s.at, table("users", "id"))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.PutQueryStats(ctx, k, mkQuery("sch-1", "q-1", "primary", t0, 100, 200)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutQueryStats(ctx, k, mkQuery("sch-2", "q-2", "primary", t1, 400, 3200)); err != nil {
		t.Fatal(err)
	}

	res, err := Build(ctx, store, k, Options{From: "latest~1", To: "latest", Kind: "query", Node: "primary"})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.QueryDelta) != 1 {
		t.Fatalf("got %d query deltas, want one node's", len(res.QueryDelta))
	}
	nd := res.QueryDelta[0]
	if nd.Node != "primary" {
		t.Errorf("node %q, want primary", nd.Node)
	}
	if len(nd.Delta.Entries) != 1 {
		t.Fatalf("got %d entries", len(nd.Delta.Entries))
	}
	e := nd.Delta.Entries[0]
	if e.CallsDelta != 300 {
		t.Errorf("calls delta %d, want 300", e.CallsDelta)
	}
	// the point of the window mean: 3000ms over 300 calls is 10ms, up from 2ms
	if e.WindowMeanMs == nil || *e.WindowMeanMs != 10 {
		t.Errorf("window mean %v, want 10", e.WindowMeanMs)
	}
	if res.Summary.QueryMovers != 1 {
		t.Errorf("query movers %d, want 1", res.Summary.QueryMovers)
	}
	if res.IsEmpty() {
		t.Error("a result carrying query drift reported itself empty")
	}
}

// A query capture is correlated by time like activity is, so a schema anchor
// still picks up the query drift around it.
func TestBuild_QueryCorrelatedFromSchemaAnchor(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	k := key()
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)

	if _, err := store.PutSchema(ctx, k, mkSchema("sch-1", t0, table("users", "id"))); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSchema(ctx, k, mkSchema("sch-2", t1, table("users", "id", "email"))); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutQueryStats(ctx, k, mkQuery("sch-1", "q-1", "primary", t0, 10, 20)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutQueryStats(ctx, k, mkQuery("sch-2", "q-2", "primary", t1, 60, 320)); err != nil {
		t.Fatal(err)
	}

	res, err := Build(ctx, store, k, Options{From: "latest~1", To: "latest", Kind: "schema"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.QueryDelta) != 1 {
		t.Fatalf("schema anchor did not pick up query drift: %+v", res.QueryDelta)
	}
	if res.Summary.QueryMovers == 0 {
		t.Error("query movers not counted in the summary")
	}
}

// Two captures of the same node with nothing moving must not manufacture a
// delta -- an agent reading "query drift" on a quiet database learns nothing.
func TestBuild_QuietQueryStatsAreNotReported(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	k := key()
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)

	if _, err := store.PutSchema(ctx, k, mkSchema("sch-1", t0, table("users", "id"))); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSchema(ctx, k, mkSchema("sch-2", t1, table("users", "id"))); err != nil {
		t.Fatal(err)
	}
	// identical counters, different content hash so both rows persist
	a := mkQuery("sch-1", "q-1", "primary", t0, 10, 20)
	b := mkQuery("sch-2", "q-2", "primary", t1, 10, 20)
	if _, err := store.PutQueryStats(ctx, k, a); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutQueryStats(ctx, k, b); err != nil {
		t.Fatal(err)
	}

	res, err := Build(ctx, store, k, Options{From: "latest~1", To: "latest", Kind: "query", Node: "primary"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary.QueryMovers != 0 {
		t.Errorf("query movers %d on an unchanged pair", res.Summary.QueryMovers)
	}
	// asserting only the count would pass with query correlation deleted
	// entirely; what matters is that nothing is reported at all
	if len(res.QueryDelta) != 0 {
		t.Errorf("reported a delta for a pair where nothing moved: %+v", res.QueryDelta)
	}
	if !res.IsEmpty() {
		t.Errorf("an unchanged pair reported itself non-empty: %s", res.Summary.Headline)
	}
}

// A query-only diff has no schema objects to count, and "across 0 objects"
// reads as a contradiction next to "1 shape of query drift".
func TestHeadline_QueryOnlyOmitsObjectCount(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	k := key()
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)

	// identical schemas, so nothing structural moves and no object is built
	if _, err := store.PutSchema(ctx, k, mkSchema("sch-1", t0, table("users", "id"))); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSchema(ctx, k, mkSchema("sch-2", t1, table("users", "id"))); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutQueryStats(ctx, k, mkQuery("sch-1", "q-1", "primary", t0, 100, 200)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutQueryStats(ctx, k, mkQuery("sch-2", "q-2", "primary", t1, 400, 3200)); err != nil {
		t.Fatal(err)
	}

	res, err := Build(ctx, store, k, Options{From: "latest~1", To: "latest", Kind: "query", Node: "primary"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(res.Summary.Headline, "0 objects") {
		t.Errorf("headline counts objects it does not have: %s", res.Summary.Headline)
	}
	if !strings.Contains(res.Summary.Headline, "query drift") {
		t.Errorf("headline omits the query drift: %s", res.Summary.Headline)
	}
}

// Nodes were discovered from activity captures only, so a project that
// captures query stats and nothing else had no nodes at all -- and answered
// "no changes" for a diff of two query snapshots.
func TestBuild_QueryOnlyHistoryHasNodes(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	k := key()
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)

	// no activity captures anywhere
	if _, err := store.PutQueryStats(ctx, k, mkQuery("", "q-1", "primary", t0, 100, 200)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutQueryStats(ctx, k, mkQuery("", "q-2", "primary", t1, 400, 3200)); err != nil {
		t.Fatal(err)
	}

	res, err := Build(ctx, store, k, Options{From: "latest~1", To: "latest", Kind: "query"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.QueryDelta) != 1 {
		t.Fatalf("query-only history produced no delta: %+v", res)
	}
	if res.IsEmpty() {
		t.Errorf("reported no changes for a diff of two query captures: %s", res.Summary.Headline)
	}
}

// A planner or activity anchor picks up query drift by time, and a capture
// outside the window is not pulled in.
func TestBuild_QueryFoundByWindow(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	k := key()
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)

	for _, s := range []struct {
		hash string
		at   time.Time
	}{{"sch-1", t0}, {"sch-2", t1}} {
		if _, err := store.PutSchema(ctx, k, mkSchema(s.hash, s.at, table("users", "id"))); err != nil {
			t.Fatal(err)
		}
	}
	// activity anchors, query captured a minute later each time
	if _, err := store.PutActivity(ctx, k, mkActivity("sch-1", "a-1", "primary", t0, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutActivity(ctx, k, mkActivity("sch-2", "a-2", "primary", t1, 90)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutQueryStats(ctx, k, mkQuery("", "q-1", "primary", t0.Add(time.Minute), 100, 200)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutQueryStats(ctx, k, mkQuery("", "q-2", "primary", t1.Add(time.Minute), 400, 3200)); err != nil {
		t.Fatal(err)
	}

	res, err := Build(ctx, store, k, Options{From: "latest~1", To: "latest", Kind: "activity", Node: "primary"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.QueryDelta) != 1 {
		t.Fatalf("an activity anchor did not correlate query drift: %+v", res.QueryDelta)
	}

	t.Run("a capture outside the window is not pulled in", func(t *testing.T) {
		res, err := Build(ctx, store, k, Options{
			From: "latest~1", To: "latest", Kind: "activity", Node: "primary",
			Window: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.QueryDelta) != 0 {
			t.Errorf("query captures a minute away were correlated into a 1s window")
		}
	})
}

// A refusal is the one case worth surfacing with no entries: an agent must
// learn the diff could not be taken rather than read silence as "no change".
func TestBuild_IncomparableQueryIsSurfaced(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	k := key()
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)

	if _, err := store.PutSchema(ctx, k, mkSchema("sch-1", t0, table("users", "id"))); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSchema(ctx, k, mkSchema("sch-2", t1, table("users", "id"))); err != nil {
		t.Fatal(err)
	}
	a := mkQuery("sch-1", "q-1", "pool", t0, 100, 200)
	a.Node.ServerAddr = "10.0.0.1"
	b := mkQuery("sch-2", "q-2", "pool", t1, 40, 80)
	b.Node.ServerAddr = "10.0.0.2" // a different machine under one label
	if _, err := store.PutQueryStats(ctx, k, a); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutQueryStats(ctx, k, b); err != nil {
		t.Fatal(err)
	}

	res, err := Build(ctx, store, k, Options{From: "latest~1", To: "latest", Kind: "query", Node: "pool"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.QueryDelta) != 1 || res.QueryDelta[0].Delta.Incomparable == "" {
		t.Fatalf("a refused pair was not surfaced: %+v", res.QueryDelta)
	}
	if res.Summary.QueryRefused != 1 {
		t.Errorf("refused nodes %d, want 1", res.Summary.QueryRefused)
	}
	// a non-empty result must never leave the headline a bare fragment
	if strings.HasPrefix(res.Summary.Headline, ",") || !strings.Contains(res.Summary.Headline, "not comparable") {
		t.Errorf("headline does not say the diff was refused: %q", res.Summary.Headline)
	}
}

// The summary view is what an agent reads first; shipping every shape's SQL
// text in it is the payload regression to avoid.
func TestForView_SummaryDropsQueryEntries(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	k := key()
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)

	if _, err := store.PutQueryStats(ctx, k, mkQuery("", "q-1", "primary", t0, 100, 200)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutQueryStats(ctx, k, mkQuery("", "q-2", "primary", t1, 400, 3200)); err != nil {
		t.Fatal(err)
	}
	res, err := Build(ctx, store, k, Options{From: "latest~1", To: "latest", Kind: "query"})
	if err != nil {
		t.Fatal(err)
	}

	if got := res.ForView("summary", 50); len(got.QueryDelta) != 0 {
		t.Errorf("summary carried %d query deltas", len(got.QueryDelta))
	}
	// the counts survive, so the agent still knows there is drift to fetch
	if res.ForView("summary", 50).Summary.QueryMovers == 0 {
		t.Error("summary lost the query mover count")
	}
	if got := res.ForView("full", 50); len(got.QueryDelta) != 1 {
		t.Errorf("full view dropped query deltas: %+v", got.QueryDelta)
	}
}

// A node that captures query stats but no activity is invisible if node
// discovery reads activity labels alone -- and it is not the anchor's own
// node, so the anchor fallback does not rescue it either.
func TestBuild_QueryNodeWithoutActivityIsFound(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	k := key()
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)

	for _, s := range []struct {
		hash string
		at   time.Time
	}{{"sch-1", t0}, {"sch-2", t1}} {
		if _, err := store.PutSchema(ctx, k, mkSchema(s.hash, s.at, table("users", "id"))); err != nil {
			t.Fatal(err)
		}
	}
	// activity only on primary
	if _, err := store.PutActivity(ctx, k, mkActivity("sch-1", "a-1", "primary", t0, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutActivity(ctx, k, mkActivity("sch-2", "a-2", "primary", t1, 90)); err != nil {
		t.Fatal(err)
	}
	// query only on an analytics replica that captures nothing else
	if _, err := store.PutQueryStats(ctx, k, mkQuery("sch-1", "q-1", "replica-analytics", t0, 100, 200)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutQueryStats(ctx, k, mkQuery("sch-2", "q-2", "replica-analytics", t1, 400, 3200)); err != nil {
		t.Fatal(err)
	}

	res, err := Build(ctx, store, k, Options{From: "latest~1", To: "latest", Kind: "schema"})
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, nd := range res.QueryDelta {
		if nd.Node == "replica-analytics" {
			found = true
		}
	}
	if !found {
		t.Errorf("a query-only node was dropped from the diff: %+v", res.QueryDelta)
	}
}
