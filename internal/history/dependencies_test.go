package history

import (
	"context"
	"testing"

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
