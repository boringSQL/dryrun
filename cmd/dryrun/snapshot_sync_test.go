package main

import (
	"bytes"
	"context"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

func syncKey(project, database string) history.SnapshotKey {
	return history.SnapshotKey{
		ProjectID:  history.ProjectId(project),
		DatabaseID: history.DatabaseId(database),
	}
}

func syncTestSchema(hash, db string, ts time.Time) *schema.SchemaSnapshot {
	return &schema.SchemaSnapshot{
		PgVersion:   "PostgreSQL 17.0",
		Database:    db,
		Timestamp:   ts,
		ContentHash: hash,
		Tables:      []schema.Table{{Schema: "public", Name: "users"}},
	}
}

func syncTestPlanner(schemaRef, hash, db string, ts time.Time) *schema.PlannerStatsSnapshot {
	return &schema.PlannerStatsSnapshot{
		SchemaRefHash: schemaRef,
		ContentHash:   hash,
		Database:      db,
		Timestamp:     ts,
		Tables: []schema.TableSizingEntry{{
			Table:  schema.QualifiedName{Schema: "public", Name: "users"},
			Sizing: schema.TableSizing{Reltuples: 1, Relpages: 1, TableSize: 8192},
		}},
	}
}

func syncTestActivity(schemaRef, hash, source string, ts time.Time, standby bool) *schema.ActivityStatsSnapshot {
	return &schema.ActivityStatsSnapshot{
		SchemaRefHash: schemaRef,
		ContentHash:   hash,
		Node: schema.NodeIdentity{
			Source: source, IsStandby: standby, PgVersion: "PostgreSQL 17.0",
			Timestamp: ts,
		},
		Tables: []schema.TableActivityEntry{{
			Table:    schema.QualifiedName{Schema: "public", Name: "users"},
			Activity: schema.TableActivity{SeqScan: 1, IdxScan: 2},
		}},
	}
}

func syncTestQueryStats(schemaRef, hash, source string, ts time.Time) *schema.QueryStatsSnapshot {
	return &schema.QueryStatsSnapshot{
		SchemaRefHash: schemaRef,
		ContentHash:   hash,
		Node:          schema.NodeIdentity{Source: source, PgVersion: "PostgreSQL 17.0", Timestamp: ts},
		Queries: []schema.QueryStatsEntry{{
			Fingerprint: "sha1:abc",
			Canonical:   "SELECT id FROM users WHERE id = $1",
			// the store refuses payloads with no members: they digest as empty
			// captures and collide under the unique index
			Members: []schema.QueryStatsMember{{QueryID: 42, Calls: 5}},
			Calls:   5,
		}},
	}
}

func openSQLite(t *testing.T) *history.Store {
	t.Helper()
	store, err := history.Open(filepath.Join(t.TempDir(), "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func openFS(t *testing.T) *history.FilesystemStore {
	t.Helper()
	store, err := history.NewFilesystemStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// TestSyncKeysCopiesEverythingToEmptyDst seeds src with one snapshot per
// kind under a single key, points sync at an empty dst, and asserts every
// row lands as Copied with zero UpToDate. The empty-destination case is
// where push/pull does its actual work; a 0/0 result here would mean the
// content-hash diff is silently dropping rows. Query stats are included
// here as the fourth kind for the same reason schema/planner/activity are:
// SyncOutcome.Query only exists because syncKeys' switch on kind.Tag got a
// history.KindQuery case added, and kindOrder() got history.QueryKind("")
// appended — either omission would leave query stats permanently invisible
// to `snapshot push`/`pull` while compiling and passing every other test.
func TestSyncKeysCopiesEverythingToEmptyDst(t *testing.T) {
	ctx := context.Background()
	src := openSQLite(t)
	dst := openSQLite(t)
	k := syncKey("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)

	s := syncTestSchema("sh-1", "appdb", now.Add(-time.Hour))
	if _, err := src.PutSchema(ctx, k, s); err != nil {
		t.Fatal(err)
	}
	if _, err := src.PutPlanner(ctx, k, syncTestPlanner("sh-1", "pl-1", "appdb", now)); err != nil {
		t.Fatal(err)
	}
	if _, err := src.PutActivity(ctx, k, syncTestActivity("sh-1", "ac-1", "primary", now, false)); err != nil {
		t.Fatal(err)
	}
	if _, err := src.PutQueryStats(ctx, k, syncTestQueryStats("sh-1", "qs-1", "primary", now)); err != nil {
		t.Fatal(err)
	}

	outs, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, fullScope())
	if err != nil {
		t.Fatalf("syncKeys: %v", err)
	}
	if len(outs) != 1 {
		t.Fatalf("got %d outcomes, want 1", len(outs))
	}
	o := outs[0]
	want := func(label string, got, copied, uptodate int) {
		if got != copied {
			t.Errorf("%s.Copied = %d, want %d", label, got, copied)
		}
	}
	want("schema", o.Schema.Copied, 1, 0)
	want("planner", o.Planner.Copied, 1, 0)
	want("activity", o.Activity.Copied, 1, 0)
	want("query", o.Query.Copied, 1, 0)
	if o.Schema.UpToDate+o.Planner.UpToDate+o.Activity.UpToDate+o.Query.UpToDate != 0 {
		t.Errorf("expected zero up-to-date on empty dst, got schema=%d planner=%d activity=%d query=%d",
			o.Schema.UpToDate, o.Planner.UpToDate, o.Activity.UpToDate, o.Query.UpToDate)
	}

	// And the row must genuinely be on dst, not just counted as copied.
	dstList, err := dst.List(ctx, k, history.QueryKind(""), history.TimeRange{})
	if err != nil || len(dstList) != 1 || dstList[0].ContentHash != "qs-1" {
		t.Errorf("dst query stats after sync: got %+v err=%v", dstList, err)
	}
}

// TestSyncKeysReportsUpToDateForMatchingHashes pre-seeds dst with the same
// schema content_hash that src has, then adds a *second* schema only to
// src. The diff must report 1 Copied + 1 UpToDate — anything else means
// the dedup gate is reading the wrong column or the set is being rebuilt
// per row.
func TestSyncKeysReportsUpToDateForMatchingHashes(t *testing.T) {
	ctx := context.Background()
	src := openSQLite(t)
	dst := openSQLite(t)
	k := syncKey("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)

	shared := syncTestSchema("sh-shared", "appdb", now.Add(-2*time.Hour))
	if _, err := src.PutSchema(ctx, k, shared); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.PutSchema(ctx, k, shared); err != nil {
		t.Fatal(err)
	}

	// src-only new snapshot; must be the one Copied count
	fresh := syncTestSchema("sh-fresh", "appdb", now)
	if _, err := src.PutSchema(ctx, k, fresh); err != nil {
		t.Fatal(err)
	}

	outs, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, fullScope())
	if err != nil {
		t.Fatalf("syncKeys: %v", err)
	}
	o := outs[0]
	if o.Schema.Copied != 1 || o.Schema.UpToDate != 1 {
		t.Errorf("schema counts = {Copied:%d UpToDate:%d}, want {1, 1}", o.Schema.Copied, o.Schema.UpToDate)
	}

	// verify dst now actually holds both hashes
	list, err := dst.ListSchema(ctx, k, history.TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, s := range list {
		got[s.ContentHash] = true
	}
	if !got["sh-shared"] || !got["sh-fresh"] {
		t.Errorf("dst missing a hash after sync: got %+v", got)
	}
}

// TestSyncCopiesActivityPerNodeLabel: three activity rows under three
// distinct node_source values must each land on dst keyed by the right
// label. The risk this guards is a regression where ActivityKind("") on
// List loses the label and Put on dst collapses everything under a single
// node — silently destroying the multi-node fanout.
func TestSyncCopiesActivityPerNodeLabel(t *testing.T) {
	ctx := context.Background()
	src := openSQLite(t)
	dst := openSQLite(t)
	k := syncKey("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)

	if _, err := src.PutSchema(ctx, k, syncTestSchema("sh-1", "appdb", now.Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	// schema must exist on dst too so the FilesystemStore-equivalent orphan
	// rule (when dst is a FS store) wouldn't reject; here dst is SQLite, but
	// we seed schema anyway to keep the test reflective of real sync order.
	if _, err := dst.PutSchema(ctx, k, syncTestSchema("sh-1", "appdb", now.Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}

	sources := []string{"primary", "replica-a", "replica-b"}
	for i, src1 := range sources {
		a := syncTestActivity("sh-1", "ac-"+src1, src1, now.Add(time.Duration(i)*time.Minute), src1 != "primary")
		if _, err := src.PutActivity(ctx, k, a); err != nil {
			t.Fatal(err)
		}
	}

	outs, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, fullScope())
	if err != nil {
		t.Fatalf("syncKeys: %v", err)
	}
	if outs[0].Activity.Copied != 3 {
		t.Errorf("activity copied = %d, want 3", outs[0].Activity.Copied)
	}

	dstList, err := dst.List(ctx, k, history.ActivityKind(""), history.TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	gotLabels := make([]string, 0, len(dstList))
	for _, s := range dstList {
		gotLabels = append(gotLabels, s.NodeLabel)
	}
	sort.Strings(gotLabels)
	want := []string{"primary", "replica-a", "replica-b"}
	if len(gotLabels) != len(want) {
		t.Fatalf("dst activity labels = %v, want %v", gotLabels, want)
	}
	for i := range want {
		if gotLabels[i] != want[i] {
			t.Errorf("labels[%d] = %q, want %q", i, gotLabels[i], want[i])
		}
	}
}

// TestSyncCopiesQueryStatsPerNodeLabel is TestSyncCopiesActivityPerNodeLabel's
// twin. Query stats are captured per-node exactly the way activity is (a
// primary and every replica each run their own pg_stat_statements), and
// selectSnapshots feeds QueryKind("") into src.List the same way it feeds
// ActivityKind("") — so this is the same regression guard, just for the newer
// of the two per-node kinds: an accidental ActivityKind("") reused where
// QueryKind("") belongs would compile fine (both are SnapshotKind values) and
// silently sync zero query rows while reporting success.
func TestSyncCopiesQueryStatsPerNodeLabel(t *testing.T) {
	ctx := context.Background()
	src := openSQLite(t)
	dst := openSQLite(t)
	k := syncKey("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)

	if _, err := src.PutSchema(ctx, k, syncTestSchema("sh-1", "appdb", now.Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.PutSchema(ctx, k, syncTestSchema("sh-1", "appdb", now.Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}

	sources := []string{"primary", "replica-a", "replica-b"}
	for i, node := range sources {
		q := syncTestQueryStats("sh-1", "qs-"+node, node, now.Add(time.Duration(i)*time.Minute))
		if _, err := src.PutQueryStats(ctx, k, q); err != nil {
			t.Fatal(err)
		}
	}

	outs, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, fullScope())
	if err != nil {
		t.Fatalf("syncKeys: %v", err)
	}
	if outs[0].Query.Copied != 3 {
		t.Errorf("query stats copied = %d, want 3", outs[0].Query.Copied)
	}

	dstList, err := dst.List(ctx, k, history.QueryKind(""), history.TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	gotLabels := make([]string, 0, len(dstList))
	for _, s := range dstList {
		gotLabels = append(gotLabels, s.NodeLabel)
	}
	sort.Strings(gotLabels)
	want := []string{"primary", "replica-a", "replica-b"}
	if len(gotLabels) != len(want) {
		t.Fatalf("dst query stats labels = %v, want %v", gotLabels, want)
	}
	for i := range want {
		if gotLabels[i] != want[i] {
			t.Errorf("labels[%d] = %q, want %q", i, gotLabels[i], want[i])
		}
	}
}

// TestSyncKindOrderIncludesQuery pushes into a FilesystemStore destination,
// which enforces the orphan rule: any planner/activity/query put before the
// matching schema bundle exists will fail. If kindOrder ever regressed (e.g.
// someone reordered it, or dropped the query entry that was appended after
// schema/planner/activity already existed), this test would blow up with
// ErrOrphanSnapshot. It's the cheapest insurance against that class of
// refactor mistake — renamed from ...IsSchemaPlannerActivity now that a
// fourth kind is part of the contract it's pinning.
func TestSyncKindOrderIncludesQuery(t *testing.T) {
	ctx := context.Background()
	src := openSQLite(t)
	dst := openFS(t)
	k := syncKey("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)

	if _, err := src.PutSchema(ctx, k, syncTestSchema("sh-1", "appdb", now.Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	if _, err := src.PutPlanner(ctx, k, syncTestPlanner("sh-1", "pl-1", "appdb", now)); err != nil {
		t.Fatal(err)
	}
	if _, err := src.PutActivity(ctx, k, syncTestActivity("sh-1", "ac-1", "primary", now, false)); err != nil {
		t.Fatal(err)
	}
	if _, err := src.PutQueryStats(ctx, k, syncTestQueryStats("sh-1", "qs-1", "primary", now)); err != nil {
		t.Fatal(err)
	}

	outs, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, fullScope())
	if err != nil {
		t.Fatalf("syncKeys against FilesystemStore dst: %v", err)
	}
	if outs[0].Query.Copied != 1 {
		t.Errorf("query stats copied to FilesystemStore = %d, want 1", outs[0].Query.Copied)
	}
}

// TestSyncSkipsQueryStatsWhenDestinationDoesntSupportIt is the sync-layer half
// of HTTPStore's ErrKindUnsupported contract (the store-layer half lives in
// internal/history's TestHTTPStorePutQueryStatsUnsupported). A predict/
// Hindsight remote's manifest has no query-stats field yet, so its Put
// returns history.ErrKindUnsupported for a query-stats StoredSnapshot. What
// this test actually pins down is that syncKindList treats that sentinel as
// "stop copying this one kind" rather than letting it bubble up through
// syncKeys and fail `snapshot push` outright for a customer who has query
// stats captured locally — schema/planner/activity must still sync
// successfully to a remote that doesn't understand query stats yet.
//
// There is no real predictd to test against here, so this uses a minimal
// decorator around a real SQLite Store: every method delegates normally
// except Put, which returns history.ErrKindUnsupported for query-stats rows
// specifically — the same shape of failure HTTPStore produces, without
// standing up an HTTP server to get it.
func TestSyncSkipsQueryStatsWhenDestinationDoesntSupportIt(t *testing.T) {
	ctx := context.Background()
	src := openSQLite(t)
	dst := &queryUnsupportedStore{Store: openSQLite(t)}
	k := syncKey("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)

	if _, err := src.PutSchema(ctx, k, syncTestSchema("sh-1", "appdb", now.Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	if _, err := src.PutPlanner(ctx, k, syncTestPlanner("sh-1", "pl-1", "appdb", now)); err != nil {
		t.Fatal(err)
	}
	if _, err := src.PutActivity(ctx, k, syncTestActivity("sh-1", "ac-1", "primary", now, false)); err != nil {
		t.Fatal(err)
	}
	if _, err := src.PutQueryStats(ctx, k, syncTestQueryStats("sh-1", "qs-1", "primary", now)); err != nil {
		t.Fatal(err)
	}

	outs, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, fullScope())
	if err != nil {
		t.Fatalf("syncKeys must not fail just because the destination rejects one kind: %v", err)
	}
	o := outs[0]
	if o.Schema.Copied != 1 || o.Planner.Copied != 1 || o.Activity.Copied != 1 {
		t.Errorf("other kinds must still sync: schema=%d planner=%d activity=%d, want 1/1/1",
			o.Schema.Copied, o.Planner.Copied, o.Activity.Copied)
	}
	if o.Query.Copied != 0 {
		t.Errorf("query.Copied = %d, want 0 (the destination never accepted it)", o.Query.Copied)
	}
}

// queryUnsupportedStore wraps a real *history.Store and behaves identically
// to it, except Put on a query-stats StoredSnapshot always fails with
// history.ErrKindUnsupported — standing in for a predict/Hindsight remote in
// tests without needing a real HTTP server.
type queryUnsupportedStore struct {
	*history.Store
}

func (q *queryUnsupportedStore) Put(ctx context.Context, key history.SnapshotKey, snap history.StoredSnapshot) (history.PutOutcome, error) {
	if snap.AsQueryStats() != nil {
		return history.PutInserted, history.ErrKindUnsupported
	}
	return q.Store.Put(ctx, key, snap)
}

// TestSyncAllUsesListKeys: a push/pull with --all must iterate every key
// in the source rather than the resolved profile key. We drive runSync
// directly with all=true against a multi-key src and assert both keys
// surface in the output block.
func TestSyncAllUsesListKeys(t *testing.T) {
	ctx := context.Background()
	src := openSQLite(t)
	dst := openSQLite(t)
	now := time.Now().UTC().Truncate(time.Second)

	for _, k := range []history.SnapshotKey{syncKey("acme", "primary"), syncKey("zeta", "replica")} {
		if _, err := src.PutSchema(ctx, k, syncTestSchema("sh-"+string(k.ProjectID), "appdb", now)); err != nil {
			t.Fatal(err)
		}
	}

	var buf bytes.Buffer
	if err := runSync(ctx, src, dst, true, fullScope(), &buf); err != nil {
		t.Fatalf("runSync(all=true): %v", err)
	}
	out := buf.String()
	for _, want := range []string{"acme/primary", "zeta/replica"} {
		if !bytes.Contains(buf.Bytes(), []byte(want)) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

// TestRoundTripSQLiteToFsToSQLite is the acceptance test for v0.7's wire
// format. We seed a SQLite Store, push it to a FilesystemStore, then pull
// from that FilesystemStore into a *fresh* SQLite Store and confirm every
// summary on the second SQLite store matches the first by content_hash.
// If the bundle JSON shape drifts between encoder and decoder — a missing
// snake_case alias, a swapped omitempty — this round trip stops being
// symmetric and the test catches it.
func TestRoundTripSQLiteToFsToSQLite(t *testing.T) {
	ctx := context.Background()
	srcA := openSQLite(t)
	fsMid := openFS(t)
	dstB := openSQLite(t)
	k := syncKey("acme", "primary")
	now := time.Now().UTC().Truncate(time.Second)

	if _, err := srcA.PutSchema(ctx, k, syncTestSchema("sh-1", "appdb", now.Add(-2*time.Hour))); err != nil {
		t.Fatal(err)
	}
	if _, err := srcA.PutPlanner(ctx, k, syncTestPlanner("sh-1", "pl-1", "appdb", now.Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	for _, src := range []string{"primary", "replica-a"} {
		a := syncTestActivity("sh-1", "ac-"+src, src, now, src != "primary")
		if _, err := srcA.PutActivity(ctx, k, a); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := syncKeys(ctx, srcA, fsMid, []history.SnapshotKey{k}, fullScope()); err != nil {
		t.Fatalf("push A -> FS: %v", err)
	}
	if _, err := syncKeys(ctx, fsMid, dstB, []history.SnapshotKey{k}, fullScope()); err != nil {
		t.Fatalf("pull FS -> B: %v", err)
	}

	cmp := func(label string, a, b []history.SnapshotSummary) {
		if len(a) != len(b) {
			t.Errorf("%s: len A=%d B=%d", label, len(a), len(b))
			return
		}
		ah := map[string]bool{}
		for _, s := range a {
			ah[s.ContentHash] = true
		}
		for _, s := range b {
			if !ah[s.ContentHash] {
				t.Errorf("%s: B has content_hash %q missing from A", label, s.ContentHash)
			}
		}
	}
	for _, kind := range []history.SnapshotKind{
		history.SchemaKind(), history.PlannerKind(), history.ActivityKind(""),
	} {
		a, err := srcA.List(ctx, k, kind, history.TimeRange{})
		if err != nil {
			t.Fatal(err)
		}
		b, err := dstB.List(ctx, k, kind, history.TimeRange{})
		if err != nil {
			t.Fatal(err)
		}
		cmp(kind.String(), a, b)
	}
}

// TestPrintSyncOutcomesEmpty: with no keys to sync, the output must be a
// single human-readable line — not an empty buffer. CI scripts grep for
// this; silence would be misread as a hang.
func TestPrintSyncOutcomesEmpty(t *testing.T) {
	var buf bytes.Buffer
	printSyncOutcomes(&buf, nil)
	if !bytes.Contains(buf.Bytes(), []byte("No keys to sync")) {
		t.Errorf("got %q, want a 'No keys to sync' notice", buf.String())
	}
}

// seedTwoTakes lays down two distinct takes (older t0, newer t1) under key k,
// each with its own schema + planner and one activity row per node (primary +
// replica). Returns (t0, t1). Used by the latest/full/since selection tests.
func seedTwoTakes(t *testing.T, ctx context.Context, src *history.Store, k history.SnapshotKey) (time.Time, time.Time) {
	t.Helper()
	t1 := time.Now().UTC().Truncate(time.Second)
	t0 := t1.Add(-24 * time.Hour)

	for _, take := range []struct {
		ts          time.Time
		sh, pl, suf string
	}{
		{t0, "sh-0", "pl-0", "0"},
		{t1, "sh-1", "pl-1", "1"},
	} {
		if _, err := src.PutSchema(ctx, k, syncTestSchema(take.sh, "appdb", take.ts)); err != nil {
			t.Fatal(err)
		}
		if _, err := src.PutPlanner(ctx, k, syncTestPlanner(take.sh, take.pl, "appdb", take.ts)); err != nil {
			t.Fatal(err)
		}
		if _, err := src.PutActivity(ctx, k, syncTestActivity(take.sh, "ac-p"+take.suf, "primary", take.ts, false)); err != nil {
			t.Fatal(err)
		}
		if _, err := src.PutActivity(ctx, k, syncTestActivity(take.sh, "ac-r"+take.suf, "replica", take.ts, true)); err != nil {
			t.Fatal(err)
		}
	}
	return t0, t1
}

// Latest scope (the pull default) must copy exactly the newest take: one
// schema, one planner, and one activity row PER NODE — never the older take.
// The per-node count (2, not 1) guards that newestPerNode groups by node label
// rather than collapsing all activity to a single newest row.
func TestSyncLatestSelectsNewestTakePerNode(t *testing.T) {
	ctx := context.Background()
	src := openSQLite(t)
	dst := openSQLite(t)
	k := syncKey("acme", "primary")
	seedTwoTakes(t, ctx, src, k)

	outs, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, pullScope{latest: true})
	if err != nil {
		t.Fatalf("syncKeys latest: %v", err)
	}
	o := outs[0]
	if o.Schema.Copied != 1 {
		t.Errorf("schema copied = %d, want 1 (newest take only)", o.Schema.Copied)
	}
	if o.Planner.Copied != 1 {
		t.Errorf("planner copied = %d, want 1 (newest take only)", o.Planner.Copied)
	}
	if o.Activity.Copied != 2 {
		t.Errorf("activity copied = %d, want 2 (newest take, one row per node)", o.Activity.Copied)
	}
}

// Full scope backfills both takes: 2 schema, 2 planner, 4 activity (2 nodes x 2 takes).
func TestSyncFullScopeCopiesAllTakes(t *testing.T) {
	ctx := context.Background()
	src := openSQLite(t)
	dst := openSQLite(t)
	k := syncKey("acme", "primary")
	seedTwoTakes(t, ctx, src, k)

	outs, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, fullScope())
	if err != nil {
		t.Fatalf("syncKeys full: %v", err)
	}
	o := outs[0]
	if o.Schema.Copied != 2 || o.Planner.Copied != 2 || o.Activity.Copied != 4 {
		t.Errorf("full copied = schema %d / planner %d / activity %d, want 2/2/4",
			o.Schema.Copied, o.Planner.Copied, o.Activity.Copied)
	}
}

// --since with a window between the two takes drops the older take even in
// full mode (the range bounds the source list before selection).
func TestSyncSinceWindowExcludesOlderTake(t *testing.T) {
	ctx := context.Background()
	src := openSQLite(t)
	dst := openSQLite(t)
	k := syncKey("acme", "primary")
	_, t1 := seedTwoTakes(t, ctx, src, k)

	cutoff := t1.Add(-time.Hour) // after t0, before t1
	scope := pullScope{rng: history.TimeRange{From: &cutoff}}
	outs, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, scope)
	if err != nil {
		t.Fatalf("syncKeys since: %v", err)
	}
	o := outs[0]
	if o.Schema.Copied != 1 || o.Planner.Copied != 1 || o.Activity.Copied != 2 {
		t.Errorf("since copied = schema %d / planner %d / activity %d, want 1/1/2 (newest take only)",
			o.Schema.Copied, o.Planner.Copied, o.Activity.Copied)
	}
}

// Regression: a stable schema keeps its original (old) timestamp, so a take
// captured today can reference a schema from 30 days ago. Pulling --since 7d
// must still bring that old schema along, or the planner/activity land
// orphaned (their schema_ref_hash resolves to nothing locally). Both latest
// and full mode must resolve the referenced schema unwindowed.
func TestSyncSincePullsReferencedSchemaOlderThanWindow(t *testing.T) {
	for _, full := range []bool{false, true} {
		ctx := context.Background()
		src := openSQLite(t)
		dst := openSQLite(t)
		k := syncKey("acme", "primary")

		now := time.Now().UTC().Truncate(time.Second)
		old := now.Add(-30 * 24 * time.Hour)
		if _, err := src.PutSchema(ctx, k, syncTestSchema("sh-old", "appdb", old)); err != nil {
			t.Fatal(err)
		}
		// today's take binds to the unchanged 30-day-old schema.
		if _, err := src.PutPlanner(ctx, k, syncTestPlanner("sh-old", "pl-now", "appdb", now)); err != nil {
			t.Fatal(err)
		}
		if _, err := src.PutActivity(ctx, k, syncTestActivity("sh-old", "ac-p", "primary", now, false)); err != nil {
			t.Fatal(err)
		}
		if _, err := src.PutActivity(ctx, k, syncTestActivity("sh-old", "ac-r", "replica", now, true)); err != nil {
			t.Fatal(err)
		}

		cutoff := now.Add(-7 * 24 * time.Hour) // excludes the 30-day-old schema row
		scope := pullScope{latest: !full, rng: history.TimeRange{From: &cutoff}}
		outs, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, scope)
		if err != nil {
			t.Fatalf("full=%v syncKeys: %v", full, err)
		}
		o := outs[0]
		if o.Schema.Copied != 1 {
			t.Errorf("full=%v: schema copied = %d, want 1 (referenced schema pulled despite being out of window)", full, o.Schema.Copied)
		}
		if o.Planner.Copied != 1 || o.Activity.Copied != 2 {
			t.Errorf("full=%v: planner %d / activity %d, want 1/2", full, o.Planner.Copied, o.Activity.Copied)
		}
	}
}

// A schema-only stream (no planner/activity) still pulls its current schema in
// latest mode even when --since predates it: latest always includes the most
// recent known schema so local state is never left without one.
func TestSyncLatestSchemaOnlyStreamPullsCurrentSchema(t *testing.T) {
	ctx := context.Background()
	src := openSQLite(t)
	dst := openSQLite(t)
	k := syncKey("acme", "primary")

	old := time.Now().UTC().Truncate(time.Second).Add(-30 * 24 * time.Hour)
	if _, err := src.PutSchema(ctx, k, syncTestSchema("sh-old", "appdb", old)); err != nil {
		t.Fatal(err)
	}

	cutoff := old.Add(7 * 24 * time.Hour) // window starts after the schema
	scope := pullScope{latest: true, rng: history.TimeRange{From: &cutoff}}
	outs, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, scope)
	if err != nil {
		t.Fatalf("syncKeys: %v", err)
	}
	if o := outs[0]; o.Schema.Copied != 1 || o.Planner.Copied != 0 || o.Activity.Copied != 0 {
		t.Errorf("schema-only latest = schema %d / planner %d / activity %d, want 1/0/0",
			o.Schema.Copied, o.Planner.Copied, o.Activity.Copied)
	}
}

func TestParseSince(t *testing.T) {
	ref := time.Now()
	cases := []struct {
		in      string
		wantErr bool
		// approxAgo is the expected age of the result for relative inputs.
		approxAgo time.Duration
		// absolute is the exact expected instant for date inputs.
		absolute *time.Time
	}{
		{in: "7d", approxAgo: 7 * 24 * time.Hour},
		{in: "2w", approxAgo: 14 * 24 * time.Hour},
		{in: "24h", approxAgo: 24 * time.Hour},
		{in: "90m", approxAgo: 90 * time.Minute},
		{in: "1h30m", approxAgo: 90 * time.Minute},
		{in: "1.5d", approxAgo: 36 * time.Hour},
		{in: "-7d", wantErr: true},
		{in: "garbage", wantErr: true},
		{in: "", wantErr: true},
	}
	for _, c := range cases {
		got, err := parseSince(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseSince(%q): want error, got %v", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseSince(%q): unexpected error %v", c.in, err)
			continue
		}
		ago := ref.Sub(got)
		if d := ago - c.approxAgo; d < -2*time.Second || d > 2*time.Second {
			t.Errorf("parseSince(%q): age %v, want ~%v", c.in, ago, c.approxAgo)
		}
	}

	// absolute date parses to that calendar day at UTC midnight.
	got, err := parseSince("2026-01-02")
	if err != nil {
		t.Fatalf("parseSince(date): %v", err)
	}
	if got.Year() != 2026 || got.Month() != 1 || got.Day() != 2 {
		t.Errorf("parseSince(date) = %v, want 2026-01-02", got)
	}
}
