package history

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

// A lean decode must be the full decode minus exactly the payloads it skips:
// anything else missing would change what List reports.
func TestDecodeLeanBundleSkipsOnlyThePayloads(t *testing.T) {
	b := fullBundle()
	b.Planner.Indexes = []schema.IndexSizingEntry{{Table: schema.QualifiedName{Schema: "public", Name: "users"}, Index: "users_pkey"}}
	b.Planner.Columns = []schema.ColumnStatsEntry{{Table: schema.QualifiedName{Schema: "public", Name: "users"}, Column: "id"}}
	b.Planner.GUCs = []schema.GucSetting{{Name: "work_mem", Setting: "4MB"}}
	b.QueryMembers = map[string][]*schema.QueryStatsSnapshot{
		"replica": {queryStatsFixture("schema-hash-abc123", "query-hash-member", "replica"), nil},
	}
	b.Query["gone"] = nil
	raw, err := EncodeBundle(b)
	if err != nil {
		t.Fatal(err)
	}

	lean, err := decodeLeanBundle(raw)
	if err != nil {
		t.Fatal(err)
	}
	want, err := DecodeBundle(raw)
	if err != nil {
		t.Fatal(err)
	}
	want.Planner.Tables, want.Planner.Indexes, want.Planner.Columns = nil, nil, nil
	for _, q := range want.Query {
		if q != nil {
			q.Queries = nil
		}
	}
	for _, rows := range want.QueryMembers {
		for _, q := range rows {
			if q != nil {
				q.Queries = nil
			}
		}
	}
	if !reflect.DeepEqual(lean, want) {
		t.Errorf("lean decode:\n%+v\nwant the full decode without its payloads:\n%+v", lean, want)
	}
}

// fullListBackend lists by decoding every bundle in full, as before lean
// listing: the reference a lean store must read exactly like.
type fullListBackend struct{ *fsBackend }

func (f fullListBackend) list(_ context.Context, key SnapshotKey) ([]listedBundle, error) {
	dir := BundleDir(f.root, key)
	entries, err := readBundleEntries(dir)
	if err != nil {
		return nil, err
	}
	var out []listedBundle
	for _, e := range entries {
		b, err := readBundle(filepath.Join(dir, e.name))
		if err != nil {
			return nil, err
		}
		out = append(out, listedBundle{b: b})
	}
	return out, nil
}

// Every read through a lean listing, on disk and in a bucket, must return what
// a full decode returns: Get above all, whose planner and query rows come
// from a second decode.
func TestLeanListingReadsLikeAFullDecode(t *testing.T) {
	ctx := context.Background()
	root := seedParityDir(t)
	fsStore, err := NewFilesystemStore(root)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now().UTC().Truncate(time.Second)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)
	for i, row := range []struct {
		at, boot time.Time
	}{{t0, bootA}, {t0.Add(time.Hour), bootB}} {
		q := queryStatsFixture("sref", "pq"+string(rune('a'+i)), "pool")
		q.Node.Timestamp, q.Node.PostmasterStartTime, q.Node.Pool = row.at, &row.boot, true
		if _, err := fsStore.Put(ctx, key("acme", "pooled"), WrapQueryStats(q)); err != nil {
			t.Fatal(err)
		}
	}

	ref := &bundleStore{backend: fullListBackend{&fsBackend{root: root}}}
	stores := map[string]SnapshotStore{
		"fs":     reopenFs(t, root),
		"object": newObjectStore(bucketFromDir(t, root, ""), memURL(""), nil),
	}
	keys, err := ref.ListKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for name, s := range stores {
		t.Run(name, func(t *testing.T) {
			for _, k := range keys {
				assertSameReads(t, ref, s, k)
			}
		})
	}
}

func reopenFs(t *testing.T, root string) SnapshotStore {
	t.Helper()
	s, err := NewFilesystemStore(root)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// The point of lean listing: the cache holds no query rows or planner sizing,
// however many bundles a key has, while Get still hands them out.
func TestListingCacheHoldsNoPayloads(t *testing.T) {
	ctx := context.Background()
	s, _ := countingFsStore(t)
	k := key("acme", "primary")
	seedBundles(t, s, k, 3)
	fresh := &bundleStore{backend: s.backend}

	q, err := fresh.Get(ctx, k, QueryKind("primary"), NewRefLatest())
	if err != nil {
		t.Fatal(err)
	}
	if len(q.AsQueryStats().Queries) == 0 {
		t.Fatal("Get returned query stats without their rows")
	}
	p, err := fresh.Get(ctx, k, PlannerKind(), NewRefIndex(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.AsPlanner().Tables) == 0 {
		t.Fatal("Get returned planner stats without their sizing")
	}

	for _, b := range fresh.cache[k].bundles {
		if b.Planner != nil && b.Planner.Tables != nil {
			t.Errorf("bundle %s: cached planner keeps its sizing", b.Schema.ContentHash)
		}
		for label, q := range b.Query {
			if q.Queries != nil {
				t.Errorf("bundle %s: cached %s query stats keep their rows", b.Schema.ContentHash, label)
			}
		}
	}
}
