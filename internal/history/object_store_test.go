package history

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

// memObjects is the in-memory objectReader: the reference the per-cloud fakes
// are held to by testObjectReaderContract.
type memObjects struct {
	mu      sync.Mutex
	objects map[string][]byte
	listErr error
	getErr  map[string]error  // per-name get failure
	onGet   func(name string) // runs before a get reads the map
}

func newMemObjects(objects map[string][]byte) *memObjects {
	if objects == nil {
		objects = map[string][]byte{}
	}
	return &memObjects{objects: objects, getErr: map[string]error{}}
}

func (m *memObjects) list(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listErr != nil {
		return nil, m.listErr
	}
	var out []string
	for n := range m.objects {
		if strings.HasPrefix(n, prefix) {
			out = append(out, n)
		}
	}
	return out, nil
}

func (m *memObjects) get(ctx context.Context, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.onGet != nil {
		m.onGet(name)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.getErr[name]; err != nil {
		return nil, err
	}
	b, ok := m.objects[name]
	if !ok {
		return nil, fmt.Errorf("%s: %w", name, errObjectNotFound)
	}
	return b, nil
}

// testObjectReaderContract pins what objectBackend relies on from a reader.
// The gs:// and s3:// readers run it against their fake servers, with a page
// size small enough that the 1200 objects below span several pages.
func testObjectReaderContract(t *testing.T, newReader func(t *testing.T, objects map[string][]byte) objectReader) {
	t.Helper()
	ctx := context.Background()
	objects := map[string][]byte{
		"top.json.zst":                      []byte("top"),
		"team/acme/primary/a.json.zst":      []byte("a"),
		"team/acme/primary/b.json.zst":      []byte("b"),
		"team/acme/primary/sub/c.json.zst":  []byte("c"),
		"team/acme/primaryx/d.json.zst":     []byte("d"),
		"team/acme/primary/ünï code.zst":    []byte("u"),
		"team/acme/primary/a+b %25#?&=.zst": []byte("p"),
	}
	for i := 0; i < 1200; i++ {
		objects[fmt.Sprintf("bulk/acme/db/%05d.json.zst", i)] = []byte{byte(i)}
	}
	r := newReader(t, objects)

	listed := func(prefix string) []string {
		t.Helper()
		got, err := r.list(ctx, prefix)
		if err != nil {
			t.Fatalf("list %q: %v", prefix, err)
		}
		sort.Strings(got)
		return got
	}
	want := func(prefix string) []string {
		var out []string
		for n := range objects {
			if strings.HasPrefix(n, prefix) {
				out = append(out, n)
			}
		}
		sort.Strings(out)
		return out
	}

	for _, prefix := range []string{"", "team/", "team/acme/primary/", "bulk/acme/db/"} {
		if got := listed(prefix); !reflect.DeepEqual(got, want(prefix)) {
			t.Errorf("list %q: got %d names, want %d (every page, nested names included, siblings excluded)", prefix, len(got), len(want(prefix)))
		}
	}
	if got := listed("absent/"); len(got) != 0 {
		t.Errorf("list of an absent prefix = %v, want empty and no error", got)
	}

	for _, name := range []string{"team/acme/primary/a.json.zst", "team/acme/primary/ünï code.zst", "team/acme/primary/a+b %25#?&=.zst", "bulk/acme/db/00999.json.zst"} {
		got, err := r.get(ctx, name)
		if err != nil {
			t.Fatalf("get %q: %v", name, err)
		}
		if string(got) != string(objects[name]) {
			t.Errorf("get %q = %q, want %q", name, got, objects[name])
		}
	}
	if _, err := r.get(ctx, "team/acme/primary/missing.json.zst"); !errors.Is(err, errObjectNotFound) {
		t.Errorf("get of a missing object: err = %v, want errObjectNotFound", err)
	}

	// a paging loop that ignores ctx would hang a pull
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := r.list(cancelled, "bulk/"); err == nil {
		t.Error("list with a cancelled ctx: want an error")
	}
}

// what a reader's fake server is told to answer with
type readerFailure int

const (
	failListDenied    readerFailure = iota // 403 on list: no objectViewer / s3:ListBucket
	failBucketMissing                      // 404 on list: a typo in the bucket name
	failGetDenied                          // 403 on get; S3 without ListBucket says this for a missing key too
)

// Failures must surface: an empty list reads as "source has no snapshots" and
// exits 0, and a denied get mapped to not-found is swallowed as an expired object.
func testObjectReaderErrors(t *testing.T, newFailing func(t *testing.T, f readerFailure, objects map[string][]byte) objectReader) {
	t.Helper()
	ctx := context.Background()
	objects := map[string][]byte{"acme/primary/a.json.zst": []byte("a")}

	for _, f := range []readerFailure{failListDenied, failBucketMissing} {
		names, err := newFailing(t, f, objects).list(ctx, "acme/")
		if err == nil {
			t.Errorf("failure %d: list = %v, nil; want an error, not an empty bucket", f, names)
		}
	}
	_, err := newFailing(t, failGetDenied, objects).get(ctx, "acme/primary/a.json.zst")
	if err == nil || errors.Is(err, errObjectNotFound) {
		t.Errorf("denied get: err = %v, want an error that is not errObjectNotFound", err)
	}
}

func TestMemObjectsContract(t *testing.T) {
	testObjectReaderContract(t, func(_ *testing.T, objects map[string][]byte) objectReader {
		return newMemObjects(objects)
	})
	testObjectReaderErrors(t, func(_ *testing.T, f readerFailure, objects map[string][]byte) objectReader {
		m := newMemObjects(objects)
		switch f {
		case failListDenied:
			m.listErr = errors.New("403 forbidden")
		case failBucketMissing:
			m.listErr = errors.New("404 bucket not found")
		case failGetDenied:
			for n := range objects {
				m.getErr[n] = errors.New("403 forbidden")
			}
		}
		return m
	})
}

func memURL(prefix string) ObjectURL {
	return ObjectURL{Scheme: "gs", Bucket: "bucket", Prefix: prefix}
}

// bucketFromDir uploads a push --to-path dir under prefix, as `gcloud storage cp -r` does.
func bucketFromDir(t *testing.T, root, prefix string) *memObjects {
	t.Helper()
	m := newMemObjects(nil)
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		m.objects[prefix+filepath.ToSlash(rel)] = raw
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// seedParityDir builds a dir with what the production bucket holds: schema
// versions with stats, a pool label with members, schema twins (one hash under
// several timestamps), an equal-second pair, and files a reader must skip.
func seedParityDir(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	s, err := NewFilesystemStore(root)
	if err != nil {
		t.Fatal(err)
	}
	k := key("acme", "primary")
	seedBundles(t, s.(*bundleStore), k, 3)

	t0 := time.Now().UTC().Truncate(time.Second)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)
	seedPoolBundle(t, s, key("acme", "pooled"),
		poolBundleActivity("pa1", t0, bootA, true),
		poolBundleActivity("pb1", t0.Add(time.Hour), bootB, true),
		poolBundleActivity("pa2", t0.Add(2*time.Hour), bootA, true),
	)

	// twins: what a fresh history.db per run leaves, one object per capture
	fsb := &fsBackend{root: root}
	tk := key("acme", "twins")
	for i, ts := range []time.Time{t0.Add(-3 * time.Hour), t0.Add(-2 * time.Hour), t0.Add(-time.Hour)} {
		snap := testSnapshot("sh-twin", "appdb")
		snap.Timestamp = ts
		a := activityFixture("sh-twin", fmt.Sprintf("ac-twin-%d", i), "primary", false)
		a.Node.Timestamp = ts
		if err := fsb.save(ctx, tk, &Bundle{Schema: snap, Activity: map[string]*schema.ActivityStatsSnapshot{"primary": a}}); err != nil {
			t.Fatal(err)
		}
	}
	// equal second, two hashes: the hash tiebreak must order them the same way
	for _, h := range []string{"sh-eq-b", "sh-eq-a"} {
		snap := testSnapshot(h, "appdb")
		snap.Timestamp = t0
		if err := fsb.save(ctx, tk, &Bundle{Schema: snap, Activity: map[string]*schema.ActivityStatsSnapshot{}}); err != nil {
			t.Fatal(err)
		}
	}

	junk := map[string][]byte{
		"acme/twins/README.md":   []byte("not a bundle"),
		"acme/empty/.keep":       nil,
		"acme/twins/.bundle.tmp": []byte("half a write"),
	}
	// real bundles at the wrong depth: fs never walks into them, so neither may a bucket
	stray, err := EncodeBundle(&Bundle{Schema: testSnapshot("sh-stray", "appdb"), Activity: map[string]*schema.ActivityStatsSnapshot{}})
	if err != nil {
		t.Fatal(err)
	}
	name := BundleFilename(t0.Add(time.Hour), "sh-stray")
	// a folder named like a bundle is the one case the name parser can't reject
	dirLike := BundleFilename(t0, "dir")
	for _, p := range []string{
		"acme/twins/nested/" + name, "acme/" + name, name, "deep/acme/twins/x/" + name,
		"acme/twins/" + dirLike + "/" + name, "acme/ghost/" + dirLike + "/" + name,
	} {
		junk[p] = stray
	}
	for rel, raw := range junk {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// The object store must answer every read exactly as the filesystem store does
// over the same files: sync, log and prune all sit on top of these calls.
func TestObjectStoreReadsLikeFilesystem(t *testing.T) {
	ctx := context.Background()
	root := seedParityDir(t)
	fsStore, err := NewFilesystemStore(root)
	if err != nil {
		t.Fatal(err)
	}

	for _, prefix := range []string{"", "dryrun/us/"} {
		t.Run("prefix="+prefix, func(t *testing.T) {
			objStore := newObjectStore(bucketFromDir(t, root, prefix), memURL(prefix), nil)

			fsKeys, err := fsStore.ListKeys(ctx)
			if err != nil {
				t.Fatal(err)
			}
			objKeys, err := objStore.ListKeys(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(fsKeys, objKeys) {
				t.Fatalf("ListKeys: object %v, fs %v", objKeys, fsKeys)
			}
			if len(fsKeys) != 3 {
				t.Fatalf("ListKeys = %v, want primary, pooled, twins", fsKeys)
			}

			for _, k := range fsKeys {
				assertSameReads(t, fsStore, objStore, k)
			}
		})
	}
}

func assertSameReads(t *testing.T, want, got SnapshotStore, k SnapshotKey) {
	t.Helper()
	ctx := context.Background()

	wantKinds, err := want.ListKinds(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	gotKinds, err := got.ListKinds(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wantKinds, gotKinds) {
		t.Fatalf("%s ListKinds: object %v, fs %v", k, gotKinds, wantKinds)
	}

	for _, kind := range append(wantKinds, ActivityKind(""), QueryKind("")) {
		wantList, err := want.List(ctx, k, kind, TimeRange{})
		if err != nil {
			t.Fatal(err)
		}
		gotList, err := got.List(ctx, k, kind, TimeRange{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(wantList, gotList) {
			t.Fatalf("%s List %s:\nobject %+v\nfs     %+v", k, kind, gotList, wantList)
		}

		refs := []SnapshotRef{NewRefLatest()}
		for i := range wantList {
			refs = append(refs, NewRefHash(wantList[i].ContentHash), NewRefIndex(i))
		}
		for _, ref := range refs {
			w, werr := want.Get(ctx, k, kind, ref)
			g, gerr := got.Get(ctx, k, kind, ref)
			if (werr == nil) != (gerr == nil) || !reflect.DeepEqual(w, g) {
				t.Fatalf("%s Get %s %+v: object (%v, %v), fs (%v, %v)", k, kind, ref, g.ContentHash(), gerr, w.ContentHash(), werr)
			}
		}

		if kind.Tag == KindActivity || kind.Tag == KindQuery {
			for _, sum := range wantList {
				w, werr := want.ListPoolMembers(ctx, k, kind, sum.SchemaRefHash)
				g, gerr := got.ListPoolMembers(ctx, k, kind, sum.SchemaRefHash)
				if werr != nil || gerr != nil || !reflect.DeepEqual(w, g) {
					t.Fatalf("%s ListPoolMembers %s %s: object (%+v, %v), fs (%+v, %v)", k, kind, sum.SchemaRefHash, g, gerr, w, werr)
				}
			}
		}
	}
}

func TestObjectStoreRefusesWrites(t *testing.T) {
	ctx := context.Background()
	root := seedParityDir(t)
	m := bucketFromDir(t, root, "")
	s := newObjectStore(m, memURL(""), nil)
	before := len(m.objects)

	k := key("acme", "primary")
	for _, snap := range []StoredSnapshot{
		WrapSchema(testSnapshot("sh-new", "appdb")),
		WrapPlanner(plannerFixture("sh-a", "pl-new", "appdb")),
	} {
		if _, err := s.Put(ctx, k, snap); !errors.Is(err, ErrReadOnlyStore) {
			t.Fatalf("put %s: err = %v, want ErrReadOnlyStore", snap.Kind(), err)
		}
	}
	// an unchanged schema dedups before it would save: a no-op is not an error
	if out, err := s.Put(ctx, k, WrapSchema(testSnapshot("sh-a", "appdb"))); err != nil || out != PutDeduped {
		t.Fatalf("re-put of a stored schema: %v, %v; want deduped", out, err)
	}
	if len(m.objects) != before {
		t.Fatalf("objects %d -> %d after refused writes", before, len(m.objects))
	}
}

// stream overrides decide where a key lives, as on the OCI store
func TestObjectStoreFollowsStreamOverride(t *testing.T) {
	ctx := context.Background()
	root := seedParityDir(t)
	m := newMemObjects(nil)
	for n, raw := range bucketFromDir(t, root, "").objects {
		if rest, ok := strings.CutPrefix(n, "acme/primary/"); ok {
			m.objects["b/custom/stream/"+rest] = raw
		}
	}
	k := key("acme", "primary")
	s := newObjectStore(m, memURL("b/"), func(sk SnapshotKey) string {
		if sk == k {
			return "custom/stream"
		}
		return StreamSuffix(sk)
	})

	list, err := s.List(ctx, k, SchemaKind(), TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 {
		t.Fatalf("schemas under the override = %d, want 3", len(list))
	}
}

// a lifecycle rule can expire an object between list and get
func TestObjectStoreSkipsObjectExpiredAfterList(t *testing.T) {
	ctx := context.Background()
	root := seedParityDir(t)
	m := bucketFromDir(t, root, "")
	s := newObjectStore(m, memURL(""), nil)
	k := key("acme", "twins")

	var twins []string
	for n := range m.objects {
		if strings.HasPrefix(n, "acme/twins/") && strings.HasSuffix(n, "-sh-twin.json.zst") {
			twins = append(twins, n)
		}
	}
	sort.Strings(twins) // the ts stem sorts oldest first
	if len(twins) != 3 {
		t.Fatalf("fixture: twins = %v, want 3", twins)
	}
	newestTwin := twins[2]
	var once sync.Once
	m.onGet = func(string) {
		once.Do(func() {
			m.mu.Lock()
			delete(m.objects, newestTwin)
			m.mu.Unlock()
		})
	}

	list, err := s.List(ctx, k, SchemaKind(), TimeRange{})
	if err != nil {
		t.Fatalf("list with an expired object: %v", err)
	}
	if len(list) != 4 {
		t.Fatalf("schemas = %d, want the 4 that still exist", len(list))
	}

	// load (merge, dedup, pool members) falls back to an older twin of the hash
	m = bucketFromDir(t, root, "")
	once = sync.Once{}
	m.onGet = func(string) {
		once.Do(func() {
			m.mu.Lock()
			delete(m.objects, newestTwin)
			m.mu.Unlock()
		})
	}
	b, ok, err := (&objectBackend{reader: m, streamFor: StreamSuffix}).load(ctx, k, "sh-twin")
	if err != nil || !ok {
		t.Fatalf("load after expiry: ok %v, err %v", ok, err)
	}
	if got := b.Activity["primary"].ContentHash; got != "ac-twin-1" {
		t.Fatalf("loaded twin carries %s, want the older twin's ac-twin-1", got)
	}
}

func TestObjectStorePropagatesGetError(t *testing.T) {
	ctx := context.Background()
	root := seedParityDir(t)
	m := bucketFromDir(t, root, "")
	boom := errors.New("403 forbidden")
	for n := range m.objects {
		if strings.HasPrefix(n, "acme/primary/") {
			m.getErr[n] = boom
		}
	}
	s := newObjectStore(m, memURL(""), nil)
	if _, err := s.List(ctx, key("acme", "primary"), SchemaKind(), TimeRange{}); !errors.Is(err, boom) {
		t.Fatalf("list: err = %v, want the get error", err)
	}
}

func TestObjectStoreNamesTheObjectOnDecodeError(t *testing.T) {
	m := newMemObjects(map[string][]byte{
		"acme/primary/" + BundleFilename(time.Now(), "sh-x"): []byte("not zstd"),
	})
	s := newObjectStore(m, memURL(""), nil)
	_, err := s.List(context.Background(), key("acme", "primary"), SchemaKind(), TimeRange{})
	if err == nil || !strings.Contains(err.Error(), "gs://bucket/acme/primary/") {
		t.Fatalf("err = %v, want the object's URL in it", err)
	}
}

func TestObjectStorePropagatesListError(t *testing.T) {
	ctx := context.Background()
	m := newMemObjects(nil)
	m.listErr = errors.New("403 forbidden")
	s := newObjectStore(m, memURL(""), nil)
	if _, err := s.List(ctx, key("acme", "primary"), SchemaKind(), TimeRange{}); !errors.Is(err, m.listErr) {
		t.Fatalf("List: err = %v, want the list error", err)
	}
	if _, err := s.ListKeys(ctx); !errors.Is(err, m.listErr) {
		t.Fatalf("ListKeys: err = %v, want the list error", err)
	}
}

func TestParseObjectURL(t *testing.T) {
	for _, tc := range []struct {
		in, wantErr string
		want        ObjectURL
	}{
		{in: "gs://tf-production-dryrun-snapshots-us", want: ObjectURL{Scheme: "gs", Bucket: "tf-production-dryrun-snapshots-us"}},
		{in: "gs://b/", want: ObjectURL{Scheme: "gs", Bucket: "b"}},
		{in: "gs://b/team/us", want: ObjectURL{Scheme: "gs", Bucket: "b", Prefix: "team/us/"}},
		{in: "s3://b//team//us/", want: ObjectURL{Scheme: "s3", Bucket: "b", Prefix: "team/us/"}},
		{in: "gs://", wantErr: "no bucket"},
		{in: "gs:///prefix", wantErr: "no bucket"},
		{in: "gs://b:443/p", wantErr: "a bucket takes no port"},
		{in: "gs://b/p?x=1", wantErr: "only gs://bucket[/prefix]"},
		{in: "https://b/p", wantErr: `unsupported store URL scheme "https"`},
		{in: "./bundles", wantErr: `unsupported store URL scheme ""`},
	} {
		got, err := ParseObjectURL(tc.in)
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("%q: err = %v, want %q", tc.in, err, tc.wantErr)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%q = %+v, %v; want %+v", tc.in, got, err, tc.want)
		}
	}
	if s := (ObjectURL{Scheme: "gs", Bucket: "b", Prefix: "team/"}).String(); s != "gs://b/team" {
		t.Errorf("String = %q", s)
	}
}
