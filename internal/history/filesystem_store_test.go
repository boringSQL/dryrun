package history

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func testFsStore(t *testing.T) (*FilesystemStore, string) {
	t.Helper()
	root := t.TempDir()
	store, err := NewFilesystemStore(root)
	if err != nil {
		t.Fatalf("NewFilesystemStore: %v", err)
	}
	return store, root
}

func decodeBundleOnDisk(t *testing.T, path string) Bundle {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read bundle: %v", err)
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("zstd reader: %v", err)
	}
	defer dec.Close()
	plain, err := dec.DecodeAll(raw, nil)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	var b Bundle
	if err := json.Unmarshal(plain, &b); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return b
}

// TestFilesystemStoreRoundTripPerKind drives a schema + planner + activity +
// query-stats snapshot through Put -> Get -> List on FilesystemStore and
// confirms each surfaces under its own kind. This is the cross-store
// contract: anything the SQLite Store accepts must come back out of
// FilesystemStore identical. Query stats are the newest addition to this
// contract — Bundle grew a fourth field (Query, a map keyed by node source
// exactly like Activity), and FilesystemStore.Put/Get gained a putQueryStats
// method and a KindQuery switch case that lean on the same
// pickQueryStats/selectQueryStats helpers OCIStore also shares. This test is
// what proves that plumbing is actually connected end to end through a real
// on-disk bundle file, not just compiling.
func TestFilesystemStoreRoundTripPerKind(t *testing.T) {
	store, _ := testFsStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	s := testSnapshot("sh-1", "appdb")
	if _, err := store.Put(ctx, k, WrapSchema(s)); err != nil {
		t.Fatalf("put schema: %v", err)
	}
	p := plannerFixture("sh-1", "pl-1", "appdb")
	if _, err := store.Put(ctx, k, WrapPlanner(p)); err != nil {
		t.Fatalf("put planner: %v", err)
	}
	a := activityFixture("sh-1", "ac-1", "primary", false)
	if _, err := store.Put(ctx, k, WrapActivity(a)); err != nil {
		t.Fatalf("put activity: %v", err)
	}
	q := queryStatsFixture("sh-1", "qs-1", "primary")
	if _, err := store.Put(ctx, k, WrapQueryStats(q)); err != nil {
		t.Fatalf("put query stats: %v", err)
	}

	gotS, err := store.Get(ctx, k, SchemaKind(), NewRefHash("sh-1"))
	if err != nil || gotS.AsSchema().ContentHash != "sh-1" {
		t.Errorf("get schema: got %+v err=%v", gotS.AsSchema(), err)
	}
	gotP, err := store.Get(ctx, k, PlannerKind(), NewRefHash("pl-1"))
	if err != nil || gotP.AsPlanner().ContentHash != "pl-1" {
		t.Errorf("get planner: got %+v err=%v", gotP.AsPlanner(), err)
	}
	gotA, err := store.Get(ctx, k, ActivityKind("primary"), NewRefHash("ac-1"))
	if err != nil || gotA.AsActivity().ContentHash != "ac-1" {
		t.Errorf("get activity: got %+v err=%v", gotA.AsActivity(), err)
	}
	gotQ, err := store.Get(ctx, k, QueryKind("primary"), NewRefHash("qs-1"))
	if err != nil || gotQ.AsQueryStats() == nil || gotQ.AsQueryStats().ContentHash != "qs-1" {
		t.Errorf("get query stats: got %+v err=%v", gotQ.AsQueryStats(), err)
	}

	sl, _ := store.List(ctx, k, SchemaKind(), TimeRange{})
	pl, _ := store.List(ctx, k, PlannerKind(), TimeRange{})
	al, _ := store.List(ctx, k, ActivityKind(""), TimeRange{})
	ql, _ := store.List(ctx, k, QueryKind(""), TimeRange{})
	if len(sl) != 1 || len(pl) != 1 || len(al) != 1 || len(ql) != 1 {
		t.Errorf("list lengths: schema=%d planner=%d activity=%d query=%d, want 1/1/1/1",
			len(sl), len(pl), len(al), len(ql))
	}
}

// TestFilesystemStoreBundleJSONShape opens the raw bundle file and asserts
// the documented Rust-compatible layout: a top-level object with `schema`,
// `planner` (null when absent), and `activity` keyed by node_source. If
// these field names ever drift, cross-implementation sync silently breaks,
// so we pin them at the JSON level rather than via Go types.
func TestFilesystemStoreBundleJSONShape(t *testing.T) {
	store, root := testFsStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	s := testSnapshot("sh-1", "appdb")
	if _, err := store.Put(ctx, k, WrapSchema(s)); err != nil {
		t.Fatal(err)
	}
	a := activityFixture("sh-1", "ac-1", "replica-a", true)
	if _, err := store.Put(ctx, k, WrapActivity(a)); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(BundleDir(root, k), BundleFilename(s.Timestamp, s.ContentHash))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("bundle file not at expected path %s: %v", path, err)
	}
	dec, _ := zstd.NewReader(nil)
	defer dec.Close()
	plain, err := dec.DecodeAll(raw, nil)
	if err != nil {
		t.Fatal(err)
	}

	var generic map[string]json.RawMessage
	if err := json.Unmarshal(plain, &generic); err != nil {
		t.Fatalf("bundle is not a top-level object: %v", err)
	}
	for _, want := range []string{"schema", "planner", "activity"} {
		if _, ok := generic[want]; !ok {
			t.Errorf("bundle missing top-level key %q", want)
		}
	}
	if string(generic["planner"]) != "null" {
		t.Errorf("planner with no planner row: got %s, want null", string(generic["planner"]))
	}

	var act map[string]json.RawMessage
	if err := json.Unmarshal(generic["activity"], &act); err != nil {
		t.Fatalf("activity is not an object: %v", err)
	}
	if _, ok := act["replica-a"]; !ok {
		t.Errorf("activity map missing replica-a key: %v", act)
	}
}

// TestFilesystemStoreSchemaDedup: putting the byte-identical schema twice
// returns PutDeduped on the second call and the destination directory
// holds exactly one bundle file. This is the cross-store contract that
// keeps `push --all` from doubling history on every run.
func TestFilesystemStoreSchemaDedup(t *testing.T) {
	store, root := testFsStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	snap := testSnapshot("dup-hash", "appdb")

	if o, err := store.Put(ctx, k, WrapSchema(snap)); err != nil || o != PutInserted {
		t.Fatalf("first put: %v %v", o, err)
	}
	if o, err := store.Put(ctx, k, WrapSchema(snap)); err != nil || o != PutDeduped {
		t.Fatalf("second put: %v %v, want PutDeduped", o, err)
	}

	files, _ := os.ReadDir(BundleDir(root, k))
	bundles := 0
	for _, f := range files {
		if _, _, ok := ParseBundleFilename(f.Name()); ok {
			bundles++
		}
	}
	if bundles != 1 {
		t.Errorf("bundle count after dedup: got %d, want 1", bundles)
	}
}

// TestFilesystemStoreOrphan: putting a planner snapshot whose schema_ref_hash
// has no matching bundle returns ErrOrphanSnapshot. Same for activity. The
// invariant we're protecting is that planner/activity can't exist without
// a schema to bind to — otherwise sync from a partially-populated source
// would leave dangling stats files.
func TestFilesystemStoreOrphan(t *testing.T) {
	store, _ := testFsStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	p := plannerFixture("missing-schema-hash", "pl-1", "appdb")
	if _, err := store.Put(ctx, k, WrapPlanner(p)); err == nil {
		t.Errorf("put planner with no matching schema: want error, got nil")
	}

	a := activityFixture("also-missing", "ac-1", "primary", false)
	if _, err := store.Put(ctx, k, WrapActivity(a)); err == nil {
		t.Errorf("put activity with no matching schema: want error, got nil")
	}
}

// TestFilesystemStoreActivityByNode: two activity puts with different
// node_source values populate the bundle's activity map as a two-entry
// object, not as two separate bundles. This is the bundle-by-node fanout
// that lets HA clusters land their replica probes inside a single shared
// file.
func TestFilesystemStoreActivityByNode(t *testing.T) {
	store, root := testFsStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	s := testSnapshot("sh-1", "appdb")
	if _, err := store.Put(ctx, k, WrapSchema(s)); err != nil {
		t.Fatal(err)
	}
	for _, src := range []string{"primary", "replica-a"} {
		a := activityFixture("sh-1", "ac-"+src, src, src != "primary")
		if _, err := store.Put(ctx, k, WrapActivity(a)); err != nil {
			t.Fatalf("put %s: %v", src, err)
		}
	}

	path := filepath.Join(BundleDir(root, k), BundleFilename(s.Timestamp, s.ContentHash))
	b := decodeBundleOnDisk(t, path)
	if len(b.Activity) != 2 {
		t.Fatalf("bundle.activity entries: got %d, want 2", len(b.Activity))
	}
	if b.Activity["primary"].ContentHash != "ac-primary" {
		t.Errorf("primary entry: got %+v", b.Activity["primary"])
	}
	if b.Activity["replica-a"].ContentHash != "ac-replica-a" {
		t.Errorf("replica-a entry: got %+v", b.Activity["replica-a"])
	}

	// And only one bundle file lives on disk for the matching schema.
	files, _ := os.ReadDir(BundleDir(root, k))
	bundles := 0
	for _, f := range files {
		if _, _, ok := ParseBundleFilename(f.Name()); ok {
			bundles++
		}
	}
	if bundles != 1 {
		t.Errorf("bundle count: got %d, want 1 (activity amends the schema bundle)", bundles)
	}
}

// TestFilesystemStoreConcurrentPutIdempotency races 16 goroutines all
// putting the byte-identical schema. Exactly one bundle must land, and no
// .bundle-*.tmp files must remain behind — the unique-tmp+rename pattern
// is what keeps a CI runner with parallel exporters from leaving litter.
func TestFilesystemStoreConcurrentPutIdempotency(t *testing.T) {
	store, root := testFsStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	snap := testSnapshot("race-hash", "appdb")

	var wg sync.WaitGroup
	const N = 16
	wg.Add(N)
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			if _, err := store.Put(ctx, k, WrapSchema(snap)); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent put: %v", err)
	}

	files, err := os.ReadDir(BundleDir(root, k))
	if err != nil {
		t.Fatal(err)
	}
	var bundles, tmps int
	for _, f := range files {
		if _, _, ok := ParseBundleFilename(f.Name()); ok {
			bundles++
		}
		if strings.HasPrefix(f.Name(), ".bundle-") {
			tmps++
		}
	}
	if bundles != 1 {
		t.Errorf("bundle count after race: got %d, want 1", bundles)
	}
	if tmps != 0 {
		t.Errorf("leftover tmp files: got %d, want 0", tmps)
	}
}

// TestFilesystemStorePlannerActivityDedup: re-putting a planner / activity
// snapshot whose content_hash matches the existing slot returns PutDeduped
// without rewriting the bundle. We confirm via file mtime that the bundle
// is left alone, which matters for storage backends where every rewrite
// is an additional cost.
func TestFilesystemStorePlannerActivityDedup(t *testing.T) {
	store, root := testFsStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	s := testSnapshot("sh-1", "appdb")
	if _, err := store.Put(ctx, k, WrapSchema(s)); err != nil {
		t.Fatal(err)
	}
	p := plannerFixture("sh-1", "pl-1", "appdb")
	if _, err := store.Put(ctx, k, WrapPlanner(p)); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(BundleDir(root, k), BundleFilename(s.Timestamp, s.ContentHash))
	info1, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	// FS mtime granularity is OS-dependent; sleep just over a second so we
	// can detect a write if it happens.
	time.Sleep(1100 * time.Millisecond)

	if o, err := store.Put(ctx, k, WrapPlanner(p)); err != nil || o != PutDeduped {
		t.Fatalf("planner re-put: %v %v, want PutDeduped", o, err)
	}
	info2, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info1.ModTime().Equal(info2.ModTime()) {
		t.Errorf("planner dedup rewrote the bundle: %v -> %v", info1.ModTime(), info2.ModTime())
	}

	a := activityFixture("sh-1", "ac-1", "primary", false)
	if _, err := store.Put(ctx, k, WrapActivity(a)); err != nil {
		t.Fatal(err)
	}
	info3, _ := os.Stat(path)
	if o, err := store.Put(ctx, k, WrapActivity(a)); err != nil || o != PutDeduped {
		t.Fatalf("activity re-put: %v %v, want PutDeduped", o, err)
	}
	info4, _ := os.Stat(path)
	if !info3.ModTime().Equal(info4.ModTime()) {
		t.Errorf("activity dedup rewrote the bundle: %v -> %v", info3.ModTime(), info4.ModTime())
	}
}

// TestFilesystemStoreListKeys walks the on-disk tree and surfaces only
// (project, database) pairs that have at least one bundle. An empty
// directory must not appear in the result so a half-populated root from
// an aborted push is silently ignored.
func TestFilesystemStoreListKeys(t *testing.T) {
	store, root := testFsStore(t)
	ctx := context.Background()

	if _, err := store.Put(ctx, key("acme", "primary"), WrapSchema(testSnapshot("a", "appdb"))); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, key("zeta", "replica"), WrapSchema(testSnapshot("z", "appdb"))); err != nil {
		t.Fatal(err)
	}

	// stray empty (project, database) dir — must not appear
	if err := os.MkdirAll(filepath.Join(root, "ghost", "db"), 0o755); err != nil {
		t.Fatal(err)
	}

	keys, err := store.ListKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []SnapshotKey{key("acme", "primary"), key("zeta", "replica")}
	if len(keys) != len(want) {
		t.Fatalf("got %d keys (%+v), want 2 (%+v)", len(keys), keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Errorf("keys[%d]: got %+v, want %+v", i, keys[i], want[i])
		}
	}
}
