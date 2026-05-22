package main

import (
	"bytes"
	"context"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/datamask"
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

	outs, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, nil)
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

	outs, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, nil)
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

	outs, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, nil)
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

	if _, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, nil); err != nil {
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
	if err := runSync(ctx, src, dst, true, nil, &buf); err != nil {
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

	if _, err := syncKeys(ctx, srcA, fsMid, []history.SnapshotKey{k}, nil); err != nil {
		t.Fatalf("push A -> FS: %v", err)
	}
	if _, err := syncKeys(ctx, fsMid, dstB, []history.SnapshotKey{k}, nil); err != nil {
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

// plannerWithSensitiveColumn builds a planner snapshot carrying one sensitive
// column (users.email, with all three value-bearing stat fields populated)
// and one non-sensitive column (users.id). It is the fixture both masking
// sub-tests below run against.
func plannerWithSensitiveColumn(schemaRef, hash, db string, ts time.Time) *schema.PlannerStatsSnapshot {
	return &schema.PlannerStatsSnapshot{
		SchemaRefHash: schemaRef,
		ContentHash:   hash,
		Database:      db,
		Timestamp:     ts,
		Columns: []schema.ColumnStatsEntry{
			{
				Table:  schema.QualifiedName{Schema: "public", Name: "users"},
				Column: "email",
				Stats: schema.ColumnStats{
					MostCommonVals:  strPtr("{alice@example.com,bob@example.com}"),
					MostCommonFreqs: strPtr("{0.5,0.5}"),
					HistogramBounds: strPtr("{a@example.com,z@example.com}"),
				},
			},
			{
				Table:  schema.QualifiedName{Schema: "public", Name: "users"},
				Column: "id",
				Stats: schema.ColumnStats{
					MostCommonVals:  strPtr("{1,2,3}"),
					MostCommonFreqs: strPtr("{0.4,0.3,0.3}"),
					HistogramBounds: strPtr("{0,100}"),
				},
			},
		},
	}
}

// plannerColumn pulls table.column's stats out of a planner snapshot, failing
// the test if it is missing.
func plannerColumn(t *testing.T, p *schema.PlannerStatsSnapshot, table, column string) schema.ColumnStats {
	t.Helper()
	for _, c := range p.Columns {
		if c.Table.Name == table && c.Column == column {
			return c.Stats
		}
	}
	t.Fatalf("column %s.%s not found in planner snapshot", table, column)
	return schema.ColumnStats{}
}

// TestSyncMasksPlannerStats is the masking guard for the merged push/export
// path. A planner snapshot with a sensitive column is synced into a
// FilesystemStore twice, and the snapshot is read back through the FS store:
//
//   - with a resolver that returns a real policy listing users.email, the
//     bundle on disk must have email's three value-bearing stat fields NULLed
//     and users.id left completely intact;
//   - with a nil resolver (the pull case), every column must survive verbatim.
//
// This is the regression guard for the C7 wiring: if syncKind ever stops
// calling ApplyPlanner, the masked sub-test fails loudly. It also pins the
// invariant that masking is strictly opt-in — a nil resolver never masks.
func TestSyncMasksPlannerStats(t *testing.T) {
	seed := func(t *testing.T) (*history.Store, *history.FilesystemStore, history.SnapshotKey) {
		t.Helper()
		ctx := context.Background()
		src := openSQLite(t)
		dst := openFS(t)
		k := syncKey("acme", "testdb")
		now := time.Now().UTC().Truncate(time.Second)

		if _, err := src.PutSchema(ctx, k, syncTestSchema("sh-1", "appdb", now.Add(-time.Hour))); err != nil {
			t.Fatal(err)
		}
		if _, err := src.PutPlanner(ctx, k, plannerWithSensitiveColumn("sh-1", "pl-1", "appdb", now)); err != nil {
			t.Fatal(err)
		}
		return src, dst, k
	}

	// readBackPlanner pulls the (single) planner snapshot off dst after a
	// sync. We list first because masking now recomputes the content hash,
	// so the on-disk hash can differ from whatever the source carried; the
	// list lets the test observe the bytes that actually landed on disk.
	readBackPlanner := func(t *testing.T, dst *history.FilesystemStore, k history.SnapshotKey) *schema.PlannerStatsSnapshot {
		t.Helper()
		list, err := dst.List(context.Background(), k, history.PlannerKind(), history.TimeRange{})
		if err != nil {
			t.Fatalf("list planner: %v", err)
		}
		if len(list) != 1 {
			t.Fatalf("expected 1 planner bundle on dst, got %d", len(list))
		}
		stored, err := dst.Get(context.Background(), k, history.PlannerKind(), history.NewRefHash(list[0].ContentHash))
		if err != nil {
			t.Fatalf("read back planner: %v", err)
		}
		p := stored.AsPlanner()
		if p == nil {
			t.Fatal("FilesystemStore returned a non-planner snapshot")
		}
		return p
	}

	t.Run("policy masks the sensitive column only", func(t *testing.T) {
		ctx := context.Background()
		src, dst, k := seed(t)

		// loadTestPolicy lists users.email under the "testdb" block; the
		// resolver hands that same policy back for every key.
		pol := loadTestPolicy(t, "users.email")
		resolve := func(history.SnapshotKey) (*datamask.Policy, error) { return pol, nil }

		if _, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, resolve); err != nil {
			t.Fatalf("syncKeys: %v", err)
		}
		p := readBackPlanner(t, dst, k)

		email := plannerColumn(t, p, "users", "email")
		if email.MostCommonVals != nil || email.MostCommonFreqs != nil || email.HistogramBounds != nil {
			t.Errorf("users.email stats not masked: mcv=%v freqs=%v hist=%v",
				email.MostCommonVals, email.MostCommonFreqs, email.HistogramBounds)
		}

		id := plannerColumn(t, p, "users", "id")
		if id.MostCommonVals == nil || id.MostCommonFreqs == nil || id.HistogramBounds == nil {
			t.Errorf("users.id stats were masked but should be untouched: mcv=%v freqs=%v hist=%v",
				id.MostCommonVals, id.MostCommonFreqs, id.HistogramBounds)
		}
	})

	t.Run("nil resolver leaves every column untouched", func(t *testing.T) {
		ctx := context.Background()
		src, dst, k := seed(t)

		if _, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, nil); err != nil {
			t.Fatalf("syncKeys: %v", err)
		}
		p := readBackPlanner(t, dst, k)

		email := plannerColumn(t, p, "users", "email")
		if email.MostCommonVals == nil || email.MostCommonFreqs == nil || email.HistogramBounds == nil {
			t.Errorf("users.email stats masked under a nil resolver: mcv=%v freqs=%v hist=%v",
				email.MostCommonVals, email.MostCommonFreqs, email.HistogramBounds)
		}
	})
}

// TestSyncMaskingRecomputesPlannerHash proves the content-address invariant:
// when ApplyPlanner actually changes bytes, syncKind recomputes ContentHash
// so the masked bundle does not share an identity with its raw original.
// Without this recompute, the dedup gates conflate "pre-mask raw" and
// "post-mask masked" as the same object — see the next test for the
// user-visible leak that asymmetry would otherwise cause.
func TestSyncMaskingRecomputesPlannerHash(t *testing.T) {
	ctx := context.Background()
	src := openSQLite(t)
	dst := openFS(t)
	k := syncKey("acme", "testdb")
	now := time.Now().UTC().Truncate(time.Second)

	if _, err := src.PutSchema(ctx, k, syncTestSchema("sh-1", "appdb", now.Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	// hand-set hash is fine: nothing in the storage path verifies it, and
	// we use the hash purely as a label to assert "this is the stored one"
	// vs "this is the recomputed one" below.
	if _, err := src.PutPlanner(ctx, k, plannerWithSensitiveColumn("sh-1", "pl-raw", "appdb", now)); err != nil {
		t.Fatal(err)
	}

	pol := loadTestPolicy(t, "users.email")
	resolve := func(history.SnapshotKey) (*datamask.Policy, error) { return pol, nil }
	if _, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, resolve); err != nil {
		t.Fatalf("syncKeys: %v", err)
	}

	list, err := dst.List(ctx, k, history.PlannerKind(), history.TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 planner bundle on dst, got %d", len(list))
	}
	if list[0].ContentHash == "pl-raw" {
		t.Errorf("masked bundle kept the raw hash %q; recompute did not fire", list[0].ContentHash)
	}
}

// TestSyncMaskedPushAfterRawPushReplacesRaw is the headline regression guard
// for the back-catalog leak. The same planner is synced twice from the same
// source: first WITHOUT masking (raw bundle lands on dst with the source's
// stored hash), then again WITH masking. The naive front-gate dedup would
// see the stored hash already present on dst and skip the masked push as
// "up-to-date", silently leaving the raw planner in place. syncKind detects
// the planner+policy case, bypasses the front gate, applies masking, and the
// post-mask hash check decides what to do. End state: dst no longer holds
// the raw bundle, the email column is masked.
func TestSyncMaskedPushAfterRawPushReplacesRaw(t *testing.T) {
	ctx := context.Background()
	src := openSQLite(t)
	dst := openFS(t)
	k := syncKey("acme", "testdb")
	now := time.Now().UTC().Truncate(time.Second)

	if _, err := src.PutSchema(ctx, k, syncTestSchema("sh-1", "appdb", now.Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	if _, err := src.PutPlanner(ctx, k, plannerWithSensitiveColumn("sh-1", "pl-raw", "appdb", now)); err != nil {
		t.Fatal(err)
	}

	// step 1: raw push with a nil resolver. dst now holds the raw planner
	// under the stored hash "pl-raw" — exactly the back-catalog precondition
	// for the leak.
	if _, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, nil); err != nil {
		t.Fatalf("raw syncKeys: %v", err)
	}
	list, err := dst.List(ctx, k, history.PlannerKind(), history.TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ContentHash != "pl-raw" {
		t.Fatalf("after raw push, dst should hold one bundle with hash %q, got %+v", "pl-raw", list)
	}

	// step 2: masked push of the same source. With the front-gate bypass,
	// syncKind reads the planner, masks it, recomputes, and only then
	// decides whether dst already has the post-mask bundle.
	pol := loadTestPolicy(t, "users.email")
	resolve := func(history.SnapshotKey) (*datamask.Policy, error) { return pol, nil }
	if _, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, resolve); err != nil {
		t.Fatalf("masked syncKeys: %v", err)
	}

	list, err = dst.List(ctx, k, history.PlannerKind(), history.TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 planner bundle after masked re-push, got %d", len(list))
	}
	if list[0].ContentHash == "pl-raw" {
		t.Fatalf("masked push silently skipped: dst still holds the raw bundle")
	}

	stored, err := dst.Get(ctx, k, history.PlannerKind(), history.NewRefHash(list[0].ContentHash))
	if err != nil {
		t.Fatal(err)
	}
	p := stored.AsPlanner()
	if p == nil {
		t.Fatal("FilesystemStore returned a non-planner snapshot")
	}
	email := plannerColumn(t, p, "users", "email")
	if email.MostCommonVals != nil || email.MostCommonFreqs != nil || email.HistogramBounds != nil {
		t.Errorf("users.email still carries raw stats after masked re-push: mcv=%v freqs=%v hist=%v",
			email.MostCommonVals, email.MostCommonFreqs, email.HistogramBounds)
	}
}

// TestSyncNoMasksFlagDisablesMasking proves the push wiring end to end: when
// the global --no-masks flag is set, resolveMaskPolicyForKey (the resolver
// snapshotPushCmd actually hands to syncKeys) short-circuits to a nil policy,
// so a sensitive planner column survives the sync unmasked. This is the
// flag-level companion to TestSyncMasksPlannerStats, which only exercised
// hand-built/injected resolvers and never touched the real opt-out path.
func TestSyncNoMasksFlagDisablesMasking(t *testing.T) {
	ctx := context.Background()
	src := openSQLite(t)
	dst := openFS(t)
	k := syncKey("acme", "testdb")
	now := time.Now().UTC().Truncate(time.Second)

	if _, err := src.PutSchema(ctx, k, syncTestSchema("sh-1", "appdb", now.Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	if _, err := src.PutPlanner(ctx, k, plannerWithSensitiveColumn("sh-1", "pl-1", "appdb", now)); err != nil {
		t.Fatal(err)
	}

	// flip the global flag for the duration of the test, then restore it so
	// the rest of the package keeps the default (masking-on) behaviour.
	prev := flagNoMasks
	flagNoMasks = true
	t.Cleanup(func() { flagNoMasks = prev })

	// resolveMaskPolicyForKey is exactly what snapshotPushCmd passes; with
	// flagNoMasks set it must return a nil policy before any file discovery.
	if _, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, resolveMaskPolicyForKey); err != nil {
		t.Fatalf("syncKeys: %v", err)
	}

	stored, err := dst.Get(ctx, k, history.PlannerKind(), history.NewRefHash("pl-1"))
	if err != nil {
		t.Fatalf("read back planner: %v", err)
	}
	p := stored.AsPlanner()
	if p == nil {
		t.Fatal("FilesystemStore returned a non-planner snapshot")
	}
	email := plannerColumn(t, p, "users", "email")
	if email.MostCommonVals == nil || email.MostCommonFreqs == nil || email.HistogramBounds == nil {
		t.Errorf("users.email stats masked despite --no-masks: mcv=%v freqs=%v hist=%v",
			email.MostCommonVals, email.MostCommonFreqs, email.HistogramBounds)
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
