package ddl

import (
	"testing"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

// what pg_identify_object reports as the type of each referenced class
var refTypes = map[string]string{
	"pg_proc":      "function",
	"pg_opclass":   "operator class",
	"pg_type":      "type",
	"pg_extension": "extension",
}

func dep(deptype, depClass, depIdentity, refClass, refIdentity string) snapshot.Dependency {
	return snapshot.Dependency{
		Deptype:            deptype,
		DependentClass:     depClass,
		DependentIdentity:  depIdentity,
		ReferencedClass:    refClass,
		ReferencedType:     refTypes[refClass],
		ReferencedIdentity: refIdentity,
	}
}

// A snapshot with captured edges names the objects its DDL needs instead of
// describing them by shape.
func TestOmittedNamesRequiredObjects(t *testing.T) {
	tab := tbl("t", col("id", "integer"), col("v", "integer"))
	tab.Columns[0].Default = sp("public.f_def()")
	tab.Constraints = []snapshot.Constraint{{
		Name: "t_v_check", Kind: snapshot.ConstraintCheck, Definition: sp("CHECK (public.f_chk(v))"),
	}}
	tab.Indexes = []snapshot.Index{{Name: "t_expr_idx", Definition: "CREATE INDEX t_expr_idx ON public.t USING btree (public.f_idx(v))", HasExpressions: true}}
	tab.Triggers = []snapshot.Trigger{{
		Name: "t_trg", Definition: "CREATE TRIGGER t_trg BEFORE INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION public.f_trg()",
	}}

	snap := &snapshot.SchemaSnapshot{DependenciesHash: "dep-1", Dependencies: []snapshot.Dependency{
		dep("n", "pg_class", "public.t.id", "pg_proc", "public.f_def()"),
		dep("n", "pg_constraint", "t_v_check on public.t", "pg_proc", "public.f_chk(integer)"),
		dep("n", "pg_class", "public.t_expr_idx", "pg_proc", "public.f_idx(integer)"),
		dep("n", "pg_trigger", "t_trg on public.t", "pg_proc", "public.f_trg()"),
		// classes with their own entries, and objects of other tables
		dep("n", "pg_class", "public.t_expr_idx", "pg_opclass", "public.my_ops"),
		dep("n", "pg_class", "public.t_expr_idx", "pg_type", "public.mood"),
		dep("n", "pg_constraint", "other_chk on public.other", "pg_proc", "public.elsewhere()"),
	}}

	got := omitted(snap, tab)
	for _, want := range []string{
		"function public.f_def() (used by column id)",
		"function public.f_chk(integer) (used by constraint t_v_check)",
		"function public.f_idx(integer) (used by index t_expr_idx)",
		"function public.f_trg() (called by trigger t_trg)",
		"operator class public.my_ops (used by index t_expr_idx)",
	} {
		if !containsAny(got, want) {
			t.Errorf("missing %q in %v", want, got)
		}
	}
	for _, gone := range []string{
		"the functions these triggers call",
		"functions and operator classes used by defaults, generated columns, indexes and policies",
	} {
		if containsAny(got, gone) {
			t.Errorf("guessed entry %q survived: %v", gone, got)
		}
	}
	for _, s := range got {
		if contains(s, "public.mood") || contains(s, "public.elsewhere()") {
			t.Errorf("unrelated object listed: %v", got)
		}
	}
}

// One function behind two dependents is one entry, named by the first.
func TestOmittedDedupesRequiredObjects(t *testing.T) {
	tab := tbl("t", col("id", "integer"))
	tab.Columns[0].Default = sp("public.f()")
	tab.Triggers = []snapshot.Trigger{{Name: "t_trg", Definition: "x"}}

	snap := &snapshot.SchemaSnapshot{DependenciesHash: "dep-1", Dependencies: []snapshot.Dependency{
		dep("n", "pg_trigger", "t_trg on public.t", "pg_proc", "public.f()"),
		dep("n", "pg_class", "public.t.id", "pg_proc", "public.f()"),
	}}

	got := requiredObjects(snap, tab)
	want := []string{"function public.f() (called by trigger t_trg)"}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("requiredObjects = %v, want %v", got, want)
	}
}

// Snapshots taken before dependency capture keep the shape-based wording.
func TestOmittedFallsBackWithoutDependencyCapture(t *testing.T) {
	tab := tbl("t", col("id", "integer"))
	tab.Columns[0].Default = sp("public.f_def()")
	tab.Triggers = []snapshot.Trigger{{Name: "t_trg", Definition: "x"}}

	got := omitted(&snapshot.SchemaSnapshot{}, tab)
	if !containsAny(got, "the functions these triggers call") {
		t.Errorf("missing trigger fallback in %v", got)
	}
	if !containsAny(got, "functions and operator classes used by defaults, generated columns, indexes and policies") {
		t.Errorf("missing expression fallback in %v", got)
	}
}

// pg_policy is not captured, so policies keep a shape-based entry.
func TestOmittedKeepsPolicyEntry(t *testing.T) {
	tab := tbl("t", col("id", "integer"))
	tab.Policies = []snapshot.RlsPolicy{{Name: "p", Command: "SELECT", UsingExpr: sp("public.f(id)")}}

	got := omitted(&snapshot.SchemaSnapshot{DependenciesHash: "dep-1"}, tab)
	if !containsAny(got, "the functions and operator classes these policies use") {
		t.Fatalf("missing policy entry in %v", got)
	}
}

// pg_identify_object quotes identifiers, so the table's raw names must be
// quoted the same way or a table named like this loses every hint.
func TestQuotedIdentifiersResolveCapturedObjects(t *testing.T) {
	tab := &snapshot.Table{
		Schema: "Odd.Schema", Name: "Order.Items",
		Columns:     []snapshot.Column{{Name: "Line ID", TypeName: "integer"}},
		Indexes:     []snapshot.Index{{Name: "Idx.With.Dot"}},
		Constraints: []snapshot.Constraint{{Name: "Check.With.Dot", Kind: snapshot.ConstraintCheck}},
		Triggers:    []snapshot.Trigger{{Name: "Before.Insert"}},
	}
	snap := &snapshot.SchemaSnapshot{DependenciesHash: "dep-1", Dependencies: []snapshot.Dependency{
		dep("n", "pg_class", `"Odd.Schema"."Order.Items"."Line ID"`, "pg_proc", "public.f_default()"),
		dep("n", "pg_class", `"Odd.Schema"."Idx.With.Dot"`, "pg_proc", "public.f_index()"),
		dep("n", "pg_constraint", `"Check.With.Dot" on "Odd.Schema"."Order.Items"`, "pg_proc", "public.f_check()"),
		dep("n", "pg_trigger", `"Before.Insert" on "Odd.Schema"."Order.Items"`, "pg_proc", "public.f_trigger()"),
	}}

	got := requiredObjects(snap, tab)
	for _, want := range []string{
		`function public.f_default() (used by column "Line ID")`,
		`function public.f_index() (used by index "Idx.With.Dot")`,
		`function public.f_check() (used by constraint "Check.With.Dot")`,
		`function public.f_trigger() (called by trigger "Before.Insert")`,
	} {
		if !containsAny(got, want) {
			t.Errorf("missing %q in %v", want, got)
		}
	}
}

// An extension member must be provided by CREATE EXTENSION, not recreated.
func TestExtensionOwnedObjectsNeedCreateExtension(t *testing.T) {
	tab := tbl("u", col("id", "uuid"), col("own", "integer"))
	tab.Columns[0].Default = sp("public.uuid_generate_v4()")
	tab.Columns[1].Default = sp("public.own_fn()")
	tab.Triggers = []snapshot.Trigger{{Name: "t_trg", Definition: "x"}}

	snap := &snapshot.SchemaSnapshot{DependenciesHash: "dep-1", Dependencies: []snapshot.Dependency{
		dep("n", "pg_class", "public.u.id", "pg_proc", "public.uuid_generate_v4()"),
		dep("e", "pg_proc", "public.uuid_generate_v4()", "pg_extension", `"uuid-ossp"`),
		dep("n", "pg_trigger", "t_trg on public.u", "pg_proc", "public.ext_trg()"),
		dep("e", "pg_proc", "public.ext_trg()", "pg_extension", `"uuid-ossp"`),
		dep("n", "pg_class", "public.u.own", "pg_proc", "public.own_fn()"),
	}}

	got := requiredObjects(snap, tab)
	want := []string{
		`CREATE EXTENSION "uuid-ossp" (provides public.ext_trg(), called by trigger t_trg)`,
		`CREATE EXTENSION "uuid-ossp" (provides public.uuid_generate_v4(), used by column id)`,
		`function public.own_fn() (used by column own)`,
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("entry %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// The rendered result carries the exact list, not just the helper.
func TestRenderTableOmittedUsesDependencies(t *testing.T) {
	tab := tbl("t", col("id", "integer"))
	tab.Triggers = []snapshot.Trigger{{
		Name: "t_trg", Definition: "CREATE TRIGGER t_trg BEFORE INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION public.f_trg()",
	}}
	snap := &snapshot.SchemaSnapshot{DependenciesHash: "dep-1", Dependencies: []snapshot.Dependency{
		dep("n", "pg_trigger", "t_trg on public.t", "pg_proc", "public.f_trg()"),
	}}

	r := render(t, snap, tab)
	if !containsAny(r.Omitted, "function public.f_trg() (called by trigger t_trg)") {
		t.Fatalf("omitted = %v", r.Omitted)
	}
}

func TestViewDependents(t *testing.T) {
	tab := tbl("t", col("id", "integer"), col("v", "integer"))
	snap := &snapshot.SchemaSnapshot{
		DependenciesHash: "dep-1",
		Views: []snapshot.View{
			{Schema: "public", Name: "v"},
			{Schema: "app", Name: "mv", IsMaterialized: true},
			{Schema: "public", Name: "count_v"},
			{Schema: "public", Name: "unrelated"},
		},
		Dependencies: []snapshot.Dependency{
			dep("n", "pg_class", "public.v", "pg_class", "public.t.id"),
			dep("n", "pg_class", "public.v", "pg_class", "public.t.v"),
			dep("n", "pg_class", "app.mv", "pg_class", "public.t.v"),
			dep("n", "pg_class", "public.count_v", "pg_class", "public.t"),
			dep("n", "pg_class", "public.unrelated", "pg_class", "public.other.id"),
		},
	}

	got := ViewDependents(snap, tab)
	if len(got) != 3 {
		t.Fatalf("ViewDependents = %+v", got)
	}
	if got[0].Name != "mv" || !got[0].Materialized || len(got[0].Columns) != 1 || got[0].Columns[0] != "v" {
		t.Errorf("materialized view = %+v", got[0])
	}
	if got[1].Name != "count_v" || len(got[1].Columns) != 0 {
		t.Errorf("table-level dependent = %+v", got[1])
	}
	if got[2].Name != "v" || len(got[2].Columns) != 2 || got[2].Columns[0] != "id" || got[2].Columns[1] != "v" {
		t.Errorf("view = %+v", got[2])
	}
}

func TestViewDependentsQuotedTable(t *testing.T) {
	tab := &snapshot.Table{Schema: "Odd.Schema", Name: "Order.Items",
		Columns: []snapshot.Column{{Name: "Line ID", TypeName: "integer"}}}
	snap := &snapshot.SchemaSnapshot{
		DependenciesHash: "dep-1",
		Views:            []snapshot.View{{Schema: "Odd.Schema", Name: "Items.V"}},
		Dependencies: []snapshot.Dependency{
			dep("n", "pg_class", `"Odd.Schema"."Items.V"`, "pg_class", `"Odd.Schema"."Order.Items"."Line ID"`),
		},
	}
	got := ViewDependents(snap, tab)
	if len(got) != 1 || got[0].Name != "Items.V" || len(got[0].Columns) != 1 || got[0].Columns[0] != "Line ID" {
		t.Fatalf("ViewDependents = %+v", got)
	}
}

func TestViewDependentsWithoutCapture(t *testing.T) {
	if got := ViewDependents(&snapshot.SchemaSnapshot{}, tbl("t")); got != nil {
		t.Fatalf("ViewDependents = %+v, want nil", got)
	}
}
