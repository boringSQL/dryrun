package main

import (
	"context"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

func syncDep(identity string) schema.Dependency {
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

// A dependency-only change under an existing content hash must cross the
// sync layer into a bundle destination.
func TestSyncPropagatesDependenciesToBundleStore(t *testing.T) {
	ctx := context.Background()
	src := openSQLite(t)
	dst := openFS(t)
	k := syncKey("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second)

	old := syncTestSchema("sh-deps", "appdb", base.Add(-time.Hour))
	if _, err := src.PutSchema(ctx, k, old); err != nil {
		t.Fatal(err)
	}
	if _, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, fullScope()); err != nil {
		t.Fatal(err)
	}

	updated := syncTestSchema("sh-deps", "appdb", base)
	updated.DependenciesHash = "dep-1"
	updated.Dependencies = []schema.Dependency{syncDep("public.users")}
	if _, err := src.PutSchema(ctx, k, updated); err != nil {
		t.Fatal(err)
	}
	if _, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, fullScope()); err != nil {
		t.Fatal(err)
	}

	got, err := dst.Get(ctx, k, history.SchemaKind(), history.NewRefLatest())
	if err != nil {
		t.Fatal(err)
	}
	if got.AsSchema().DependenciesHash != "dep-1" || len(got.AsSchema().Dependencies) != 1 {
		t.Fatalf("bundle after sync: hash=%q edges=%d",
			got.AsSchema().DependenciesHash, len(got.AsSchema().Dependencies))
	}
}

// The same dependency-only change must cross into a history.db destination.
func TestSyncPropagatesDependenciesToLocalStore(t *testing.T) {
	ctx := context.Background()
	src := openSQLite(t)
	dst := openSQLite(t)
	k := syncKey("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second)

	old := syncTestSchema("sh-deps", "appdb", base.Add(-time.Hour))
	if _, err := src.PutSchema(ctx, k, old); err != nil {
		t.Fatal(err)
	}
	if _, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, fullScope()); err != nil {
		t.Fatal(err)
	}

	updated := syncTestSchema("sh-deps", "appdb", base)
	updated.DependenciesHash = "dep-1"
	updated.Dependencies = []schema.Dependency{syncDep("public.users")}
	if _, err := src.PutSchema(ctx, k, updated); err != nil {
		t.Fatal(err)
	}
	if _, err := syncKeys(ctx, src, dst, []history.SnapshotKey{k}, fullScope()); err != nil {
		t.Fatal(err)
	}

	got, err := dst.GetSchema(ctx, k, history.NewRefLatest())
	if err != nil {
		t.Fatal(err)
	}
	if got.DependenciesHash != "dep-1" || len(got.Dependencies) != 1 {
		t.Fatalf("local store after sync: hash=%q edges=%d",
			got.DependenciesHash, len(got.Dependencies))
	}
}

func TestSchemaDepsRefresh(t *testing.T) {
	base := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	summary := func(ts time.Time, deps string) history.SnapshotSummary {
		return history.SnapshotSummary{Timestamp: ts, DependenciesHash: deps}
	}

	tests := []struct {
		name string
		src  history.SnapshotSummary
		dst  history.SnapshotSummary
		want bool
	}{
		{"no source deps", summary(base, ""), summary(base, "d1"), false},
		{"same deps", summary(base, "d1"), summary(base, "d1"), false},
		{"fill missing at same second", summary(base, "d1"), summary(base, ""), true},
		{"missing but source older", summary(base.Add(-time.Hour), "d1"), summary(base, ""), false},
		{"missing, same second different nanos", summary(base.Add(900*time.Millisecond), "d1"), summary(base.Add(100*time.Millisecond), ""), true},
		{"different and newer", summary(base.Add(time.Hour), "d2"), summary(base, "d1"), true},
		{"different and older", summary(base.Add(-time.Hour), "d2"), summary(base, "d1"), false},
		{"different and same second", summary(base.Add(900*time.Millisecond), "d2"), summary(base.Add(100*time.Millisecond), "d1"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := schemaDepsRefresh(tc.src, tc.dst); got != tc.want {
				t.Fatalf("schemaDepsRefresh(%+v, %+v) = %v, want %v", tc.src, tc.dst, got, tc.want)
			}
		})
	}
}
