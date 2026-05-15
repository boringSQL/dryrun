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
// content-hash diff is silently dropping rows.
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

	outs, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k})
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
	if o.Schema.UpToDate+o.Planner.UpToDate+o.Activity.UpToDate != 0 {
		t.Errorf("expected zero up-to-date on empty dst, got schema=%d planner=%d activity=%d",
			o.Schema.UpToDate, o.Planner.UpToDate, o.Activity.UpToDate)
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

	outs, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k})
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

	outs, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k})
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

// TestSyncKindOrderIsSchemaPlannerActivity pushes into a FilesystemStore
// destination, which enforces the orphan rule: any planner/activity put
// before the matching schema bundle exists will fail. If kindOrder ever
// regressed (e.g. someone reordered it alphabetically), this test would
// blow up with ErrOrphanSnapshot. It's the cheapest insurance against
// that class of refactor mistake.
func TestSyncKindOrderIsSchemaPlannerActivity(t *testing.T) {
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

	if _, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}); err != nil {
		t.Fatalf("syncKeys against FilesystemStore dst: %v", err)
	}
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
	if err := runSync(ctx, src, dst, true, &buf); err != nil {
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

	if _, err := syncKeys(ctx, srcA, fsMid, []history.SnapshotKey{k}); err != nil {
		t.Fatalf("push A -> FS: %v", err)
	}
	if _, err := syncKeys(ctx, fsMid, dstB, []history.SnapshotKey{k}); err != nil {
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
