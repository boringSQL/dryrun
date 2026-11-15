package snapshot

import "testing"

func dependency(deptype, depClass, depIdentity, refClass, refIdentity string) Dependency {
	return Dependency{
		Deptype:            deptype,
		DependentClass:     depClass,
		DependentType:      "function",
		DependentIdentity:  depIdentity,
		ReferencedClass:    refClass,
		ReferencedType:     "type",
		ReferencedIdentity: refIdentity,
	}
}

func TestComputeDependenciesHashIsOrderIndependent(t *testing.T) {
	a := []Dependency{
		dependency("e", "pg_proc", "app.f(integer)", "pg_extension", "citext"),
		dependency("n", "pg_class", "app.t", "pg_namespace", "app"),
	}
	b := []Dependency{a[1], a[0]}

	if got, want := ComputeDependenciesHash(a), ComputeDependenciesHash(b); got != want {
		t.Fatalf("dependency order moved the hash: %s != %s", got, want)
	}
}

func TestComputeDependenciesHashChangesWithContent(t *testing.T) {
	a := []Dependency{dependency("e", "pg_proc", "app.f(integer)", "pg_extension", "citext")}
	b := []Dependency{dependency("n", "pg_proc", "app.f(integer)", "pg_extension", "citext")}
	if ComputeDependenciesHash(a) == ComputeDependenciesHash(b) {
		t.Fatal("deptype did not move the dependency hash")
	}
}

// Capturing pg_depend is derived metadata. It must not rehash existing
// structural snapshots, or every history row changes the first time the new
// capture runs.
func TestDependenciesDoNotMoveContentHash(t *testing.T) {
	snap := &SchemaSnapshot{
		FormatVersion: 3,
		PgVersion:     "PostgreSQL 17.0",
		Tables: []Table{{
			Schema: "public",
			Name:   "t",
			Columns: []Column{{
				Name:     "id",
				Ordinal:  1,
				TypeName: "integer",
			}},
		}},
	}

	before := DigestFor(snap)
	snap.Dependencies = []Dependency{
		dependency("n", "pg_class", "public.t", "pg_namespace", "public"),
	}
	snap.DependenciesHash = ComputeDependenciesHash(snap.Dependencies)
	if after := DigestFor(snap); after != before {
		t.Fatalf("dependencies moved ContentHash: %s -> %s", before, after)
	}
}
