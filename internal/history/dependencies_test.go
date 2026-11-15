package history

import (
	"context"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

// Dependencies are derived metadata outside ContentHash. A local capture with
// an unchanged schema must still refresh the payload when the edge set changes,
// but it must not create another schema version.
func TestPutRefreshesDependenciesUnderSameContentHash(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	snap := testSnapshot("dep-hash", "acme")
	snap.DependenciesHash = "dep-1"
	snap.Dependencies = []schema.Dependency{{
		Deptype:            "n",
		DependentClass:     "pg_class",
		DependentType:      "table",
		DependentIdentity:  "public.users",
		ReferencedClass:    "pg_namespace",
		ReferencedType:     "schema",
		ReferencedIdentity: "public",
	}}

	if o, err := store.PutSchema(ctx, k, snap); err != nil || o != PutInserted {
		t.Fatalf("first put: got (%v, %v)", o, err)
	}

	// Same ContentHash and same dependency hash: the normal dedup applies.
	if o, err := store.PutSchema(ctx, k, snap); err != nil || o != PutDeduped {
		t.Fatalf("unchanged put: got (%v, %v), want PutDeduped", o, err)
	}

	// Same ContentHash, new dependency hash: refresh the payload.
	changed := *snap
	changed.DependenciesHash = "dep-2"
	changed.Dependencies = append(snap.Dependencies, schema.Dependency{
		Deptype:            "e",
		DependentClass:     "pg_proc",
		DependentType:      "function",
		DependentIdentity:  "public.f()",
		ReferencedClass:    "pg_extension",
		ReferencedType:     "extension",
		ReferencedIdentity: "citext",
	})
	if o, err := store.PutSchema(ctx, k, &changed); err != nil || o != PutInserted {
		t.Fatalf("changed put: got (%v, %v), want PutInserted", o, err)
	}

	latest, err := store.GetSchema(ctx, k, NewRefLatest())
	if err != nil {
		t.Fatal(err)
	}
	if latest.DependenciesHash != "dep-2" || len(latest.Dependencies) != 2 {
		t.Fatalf("latest dependencies not refreshed: hash=%q edges=%d",
			latest.DependenciesHash, len(latest.Dependencies))
	}

	list, err := store.ListSchema(ctx, k, TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Errorf("dependency refresh created a new schema version: %d rows", len(list))
	}
}

// A newer payload from a dryrun that predates dependency capture must refresh
// the payload while keeping the stored edges.
func TestPullOldPayloadKeepsDependencies(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	stored := testSnapshot("dep-pull-hash", "acme")
	stored.Timestamp = time.Now().UTC().Add(-time.Hour)
	stored.DependenciesHash = "dep-new"
	stored.Dependencies = []schema.Dependency{testDependency("public.users")}
	stored.GUCs = []schema.GucSetting{{Name: "work_mem", Setting: "4096"}}
	if o, err := store.PutSchema(ctx, k, stored); err != nil || o != PutInserted {
		t.Fatalf("seed put: got (%v, %v)", o, err)
	}

	olderSource := testSnapshot("dep-pull-hash", "acme")
	olderSource.Timestamp = time.Now().UTC()
	olderSource.DependenciesHash = ""
	olderSource.Dependencies = nil
	olderSource.GUCs = []schema.GucSetting{{Name: "work_mem", Setting: "65536"}}
	if o, err := store.Put(ctx, k, WrapSchema(olderSource)); err != nil || o != PutInserted {
		t.Fatalf("pull without dependencies: got (%v, %v), want PutInserted", o, err)
	}

	latest, err := store.GetSchema(ctx, k, NewRefLatest())
	if err != nil {
		t.Fatal(err)
	}
	if latest.DependenciesHash != "dep-new" || len(latest.Dependencies) != 1 {
		t.Fatalf("pull dropped dependencies: hash=%q edges=%d",
			latest.DependenciesHash, len(latest.Dependencies))
	}
	if latest.GUCs[0].Setting != "65536" {
		t.Fatalf("pull did not refresh payload: work_mem=%s", latest.GUCs[0].Setting)
	}
}

// An older payload with a different dependency hash must not replace newer
// edges.
func TestPullDoesNotDowngradeDependencies(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	base := time.Now().UTC().Truncate(time.Second)
	stored := testSnapshot("dep-pull-hash", "acme")
	stored.Timestamp = base
	stored.DependenciesHash = "dep-new"
	stored.Dependencies = []schema.Dependency{testDependency("public.users"), testDependency("public.events")}
	if o, err := store.PutSchema(ctx, k, stored); err != nil || o != PutInserted {
		t.Fatalf("seed put: got (%v, %v)", o, err)
	}

	incoming := testSnapshot("dep-pull-hash", "acme")
	incoming.Timestamp = base.Add(-time.Hour)
	incoming.DependenciesHash = "dep-old"
	incoming.Dependencies = []schema.Dependency{testDependency("public.users")}
	if o, err := store.Put(ctx, k, WrapSchema(incoming)); err != nil || o != PutDeduped {
		t.Fatalf("older pull: got (%v, %v), want PutDeduped", o, err)
	}

	latest, err := store.GetSchema(ctx, k, NewRefLatest())
	if err != nil {
		t.Fatal(err)
	}
	if latest.DependenciesHash != "dep-new" || len(latest.Dependencies) != 2 {
		t.Fatalf("older pull downgraded dependencies: hash=%q edges=%d",
			latest.DependenciesHash, len(latest.Dependencies))
	}
}

// A bundle store keeps one bundle per content hash; dependency-only updates
// must still replace that bundle.
func TestBundleStoreRefreshesDependencies(t *testing.T) {
	store, _ := testFsStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	base := time.Now().UTC().Truncate(time.Second)
	old := testSnapshot("bundle-dep-hash", "acme")
	old.Timestamp = base.Add(-time.Hour)
	if o, err := store.Put(ctx, k, WrapSchema(old)); err != nil || o != PutInserted {
		t.Fatalf("seed put: got (%v, %v)", o, err)
	}

	updated := testSnapshot("bundle-dep-hash", "acme")
	updated.Timestamp = base
	updated.DependenciesHash = "dep-new"
	updated.Dependencies = []schema.Dependency{testDependency("public.users")}
	if o, err := store.Put(ctx, k, WrapSchema(updated)); err != nil || o != PutInserted {
		t.Fatalf("dependency put: got (%v, %v), want PutInserted", o, err)
	}

	got, err := store.Get(ctx, k, SchemaKind(), NewRefLatest())
	if err != nil {
		t.Fatal(err)
	}
	if got.AsSchema().DependenciesHash != "dep-new" || len(got.AsSchema().Dependencies) != 1 {
		t.Fatalf("bundle did not take dependencies: hash=%q edges=%d",
			got.AsSchema().DependenciesHash, len(got.AsSchema().Dependencies))
	}
	list, err := store.List(ctx, k, SchemaKind(), TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].DependenciesHash != "dep-new" {
		t.Fatalf("bundle summary dependencies hash = %+v", list)
	}
}

func TestBundleStoreDoesNotDowngradeDependencies(t *testing.T) {
	store, _ := testFsStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	base := time.Now().UTC().Truncate(time.Second)
	newer := testSnapshot("bundle-dep-hash", "acme")
	newer.Timestamp = base
	newer.DependenciesHash = "dep-new"
	newer.Dependencies = []schema.Dependency{testDependency("public.users"), testDependency("public.events")}
	if o, err := store.Put(ctx, k, WrapSchema(newer)); err != nil || o != PutInserted {
		t.Fatalf("seed put: got (%v, %v)", o, err)
	}

	older := testSnapshot("bundle-dep-hash", "acme")
	older.Timestamp = base.Add(-time.Hour)
	older.DependenciesHash = "dep-old"
	older.Dependencies = []schema.Dependency{testDependency("public.users")}
	if o, err := store.Put(ctx, k, WrapSchema(older)); err != nil || o != PutDeduped {
		t.Fatalf("older put: got (%v, %v), want PutDeduped", o, err)
	}

	got, err := store.Get(ctx, k, SchemaKind(), NewRefLatest())
	if err != nil {
		t.Fatal(err)
	}
	if got.AsSchema().DependenciesHash != "dep-new" || len(got.AsSchema().Dependencies) != 2 {
		t.Fatalf("bundle downgraded dependencies: hash=%q edges=%d",
			got.AsSchema().DependenciesHash, len(got.AsSchema().Dependencies))
	}
}

func TestBundleStoreOldPayloadKeepsDependencies(t *testing.T) {
	store, _ := testFsStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	base := time.Now().UTC().Truncate(time.Second)
	stored := testSnapshot("bundle-dep-hash", "acme")
	stored.Timestamp = base.Add(-time.Hour)
	stored.DependenciesHash = "dep-new"
	stored.Dependencies = []schema.Dependency{testDependency("public.users")}
	stored.GUCs = []schema.GucSetting{{Name: "work_mem", Setting: "4096"}}
	if o, err := store.Put(ctx, k, WrapSchema(stored)); err != nil || o != PutInserted {
		t.Fatalf("seed put: got (%v, %v)", o, err)
	}

	incoming := testSnapshot("bundle-dep-hash", "acme")
	incoming.Timestamp = base
	incoming.GUCs = []schema.GucSetting{{Name: "work_mem", Setting: "65536"}}
	if o, err := store.Put(ctx, k, WrapSchema(incoming)); err != nil || o != PutInserted {
		t.Fatalf("old payload put: got (%v, %v), want PutInserted", o, err)
	}

	got, err := store.Get(ctx, k, SchemaKind(), NewRefLatest())
	if err != nil {
		t.Fatal(err)
	}
	if got.AsSchema().DependenciesHash != "dep-new" || len(got.AsSchema().Dependencies) != 1 {
		t.Fatalf("bundle dropped dependencies: hash=%q edges=%d",
			got.AsSchema().DependenciesHash, len(got.AsSchema().Dependencies))
	}
	if got.AsSchema().GUCs[0].Setting != "65536" {
		t.Fatalf("bundle did not refresh payload: work_mem=%s", got.AsSchema().GUCs[0].Setting)
	}
}

func TestSummaryCarriesDependenciesHash(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	snap := testSnapshot("summary-dep-hash", "acme")
	snap.DependenciesHash = "dep-summary"
	if _, err := store.PutSchema(ctx, k, snap); err != nil {
		t.Fatal(err)
	}
	list, err := store.ListSchema(ctx, k, TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].DependenciesHash != "dep-summary" {
		t.Fatalf("summary dependencies hash = %+v", list)
	}
}

func testDependency(identity string) schema.Dependency {
	return schema.Dependency{
		Deptype:            "n",
		DependentClass:     "pg_class",
		DependentType:      "table",
		DependentIdentity:  identity,
		ReferencedClass:    "pg_namespace",
		ReferencedType:     "schema",
		ReferencedIdentity: "public",
	}
}
