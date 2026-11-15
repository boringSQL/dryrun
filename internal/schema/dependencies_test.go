package schema

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The dependency query is only as good as the identities it resolves to.
// This live test creates one object of each shape that matters for a future
// restore (schema, table, column type, identity sequence, view, function,
// trigger) and asserts the edges are present with stable, schema-qualified
// identities rather than OIDs.
func TestFetchDependenciesLive(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()

	name := fmt.Sprintf("dryrun_dep_%d", time.Now().UnixNano())
	qualified := fmt.Sprintf("%s.t", name)
	for _, stmt := range []string{
		fmt.Sprintf("CREATE SCHEMA %s", name),
		fmt.Sprintf("CREATE DOMAIN %s.posint AS integer CHECK (VALUE > 0)", name),
		fmt.Sprintf("CREATE TABLE %s (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v %s.posint DEFAULT 1)", qualified, name),
		fmt.Sprintf("CREATE VIEW %s.v AS SELECT id, v FROM %s.t", name, name),
		fmt.Sprintf("CREATE FUNCTION %s.f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$", name),
		fmt.Sprintf("CREATE TRIGGER t_trg BEFORE INSERT ON %s FOR EACH ROW EXECUTE FUNCTION %s.f()", qualified, name),
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed dependency fixture: %v", err)
		}
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA %s CASCADE", name))
	})

	snap, err := IntrospectSchema(ctx, pool)
	if err != nil {
		t.Fatalf("introspect: %v", err)
	}
	if snap.DependenciesHash == "" {
		t.Fatal("DependenciesHash is empty")
	}
	if len(snap.Dependencies) == 0 {
		t.Fatal("no dependencies captured")
	}

	type want struct {
		deptype, depClass, depIdentity, refClass, refIdentity string
	}
	for _, w := range []want{
		{"n", "pg_class", qualified, "pg_namespace", name},
		{"i", "pg_class", name + ".t_id_seq", "pg_class", qualified + ".id"},
		{"i", "pg_type", qualified, "pg_class", qualified},
		{"n", "pg_class", qualified + ".v", "pg_type", name + ".posint"},
		{"n", "pg_class", name + ".v", "pg_class", qualified + ".v"},
		{"n", "pg_proc", name + ".f()", "pg_namespace", name},
		{"n", "pg_trigger", "t_trg on " + qualified, "pg_proc", name + ".f()"},
	} {
		if !hasDependency(snap.Dependencies, w.deptype, w.depClass, w.depIdentity, w.refClass, w.refIdentity) {
			t.Errorf("missing dependency %+v", w)
		}
	}
}

// A temp table visible during capture must not leak into the snapshot. The
// temp namespace is pg_temp_<n>, not the literal pg_temp, so the schema filter
// has to use the same pg_temp_% rule as the table/column fetches.
func TestFetchDependenciesExcludesTemp(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()

	name := fmt.Sprintf("dryrun_dep_tmp_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx,
		fmt.Sprintf("CREATE SCHEMA %s; CREATE TYPE %s.tmp_type AS ENUM ('x')", name, name)); err != nil {
		t.Fatalf("seed temp fixture: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA %s CASCADE", name))
	})

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire temp connection: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS tmp_dep")
		conn.Release()
	})
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE tmp_dep (v %s.tmp_type)", name)); err != nil {
		t.Fatalf("create temp table: %v", err)
	}

	snap, err := IntrospectSchema(ctx, conn)
	if err != nil {
		t.Fatalf("introspect: %v", err)
	}

	// The fixture itself must be visible, or the negative assertions are vacuous.
	if !hasDependency(snap.Dependencies, "n", "pg_type", name+".tmp_type", "pg_namespace", name) {
		t.Fatalf("fixture dependency missing; dependency capture did not run")
	}
	for _, d := range snap.Dependencies {
		if strings.Contains(d.DependentIdentity, "tmp_dep") ||
			strings.Contains(d.ReferencedIdentity, "tmp_dep") ||
			strings.Contains(d.DependentIdentity, "pg_temp") ||
			strings.Contains(d.ReferencedIdentity, "pg_temp") {
			t.Errorf("temp dependency leaked: %+v", d)
		}
	}
}

func hasDependency(deps []Dependency, deptype, depClass, depIdentity, refClass, refIdentity string) bool {
	for _, d := range deps {
		if d.Deptype != deptype || d.DependentClass != depClass || d.ReferencedClass != refClass {
			continue
		}
		if strings.Contains(d.DependentIdentity, depIdentity) && strings.Contains(d.ReferencedIdentity, refIdentity) {
			return true
		}
	}
	return false
}
