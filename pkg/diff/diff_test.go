package diff

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

// emptySnap builds a minimal-but-valid SchemaSnapshot carrying just the bits the
// differ keys on: a content hash (so ClassifyDrift can short-circuit on equality)
// and a timestamp (so the SnapshotDiff envelope has real endpoints). Every test
// starts from one of these and layers in only the objects it cares about, which
// keeps each case focused on a single change-vocabulary entry.
func emptySnap(hash string) *snapshot.SchemaSnapshot {
	return &snapshot.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0", Database: "test",
		Timestamp: time.Now().UTC(), ContentHash: hash,
	}
}

// strp is the usual take-the-address-of-a-string-literal helper; the snapshot
// types model nullable columns (Default, Comment, ...) as *string, so we need a
// way to hand them a non-nil pointer inline in a struct literal.
func strp(s string) *string { return &s }

// findChange scans a delta's change list for the first entry of the given typed
// vocabulary kind and returns it, failing the test if none is present. Tests use
// this instead of asserting on a slice index because the differ sorts changes
// deterministically by object, so the positional index of any one change is an
// implementation detail we don't want to couple the assertions to.
func findChange(t *testing.T, changes []Change, ct ChangeType) Change {
	t.Helper()
	for _, c := range changes {
		if c.Type == ct {
			return c
		}
	}
	t.Fatalf("expected a %s change, got %+v", ct, changes)
	return Change{}
}

// --- drift direction ---
//
// ClassifyDrift folds the typed delta back down to a single coarse direction
// (identical / ahead / behind / diverged). These four cases were ported over
// from the old internal/diff suite when the package was promoted to pkg/diff,
// and they pin the raw, mechanical meaning of "direction": added-only reads as
// ahead, removed-only as behind, a mix as diverged. No risk judgment lives here.

func TestDrift_Identical(t *testing.T) {
	report := ClassifyDrift(emptySnap("abc"), emptySnap("abc"))
	if report.Direction != DriftIdentical {
		t.Errorf("expected identical, got %s", report.Direction)
	}
}

func TestDrift_Ahead(t *testing.T) {
	saved := emptySnap("old")
	live := emptySnap("new")
	live.Tables = []snapshot.Table{{Schema: "public", Name: "users"}}
	report := ClassifyDrift(saved, live)
	if report.Direction != DriftAhead {
		t.Errorf("expected ahead, got %s", report.Direction)
	}
	if report.AddedCount != 1 {
		t.Errorf("expected 1 added, got %d", report.AddedCount)
	}
}

func TestDrift_Behind(t *testing.T) {
	saved := emptySnap("old")
	saved.Tables = []snapshot.Table{{Schema: "public", Name: "users"}}
	live := emptySnap("new")
	report := ClassifyDrift(saved, live)
	if report.Direction != DriftBehind {
		t.Errorf("expected behind, got %s", report.Direction)
	}
}

func TestDrift_Diverged(t *testing.T) {
	saved := emptySnap("old")
	saved.Tables = []snapshot.Table{{Schema: "public", Name: "old_table"}}
	live := emptySnap("new")
	live.Tables = []snapshot.Table{{Schema: "public", Name: "new_table"}}
	report := ClassifyDrift(saved, live)
	if report.Direction != DriftDiverged {
		t.Errorf("expected diverged, got %s", report.Direction)
	}
}

// --- typed change vocabulary ---
//
// The whole point of D1 is that the differ no longer emits free-text "details"
// strings; it emits a typed vocabulary the cloud can pattern-match on without
// parsing prose. The tests below each construct a from/to schema pair that
// differs in exactly one dimension and assert that the differ produced the right
// ChangeType *and* populated the matching typed detail struct with the right
// fields. If any of these regress, the cloud's risk model silently loses a signal.

// A table present only in `to` must surface as TableAdded (carrying a human-hint
// Note with the column count), and a table present only in `from` as TableDropped.
// This is the most basic structural delta and the foundation the fold builds on.
func TestDiffSchema_TableAddedDropped(t *testing.T) {
	from := emptySnap("a")
	from.Tables = []snapshot.Table{{Schema: "public", Name: "legacy"}}
	to := emptySnap("b")
	to.Tables = []snapshot.Table{{Schema: "public", Name: "audit_log", Columns: []snapshot.Column{{Name: "id"}, {Name: "at"}}}}

	delta, err := DiffSchema(from, to)
	if err != nil {
		t.Fatal(err)
	}
	added := findChange(t, delta.Changes, TableAdded)
	if added.Object.Name != "audit_log" || added.Note != "2 columns" {
		t.Errorf("table added: got %+v note=%q", added.Object, added.Note)
	}
	dropped := findChange(t, delta.Changes, TableDropped)
	if dropped.Object.Name != "legacy" {
		t.Errorf("table dropped: got %+v", dropped.Object)
	}
}

// --- OID identity + rename detection ---
//
// Keying tables purely on (schema, name) means renaming a table reads as a drop
// of the old name plus an add of the new one — and a drop+add is a destructive,
// data-losing event, the opposite of what a rename actually is. Keying on the
// OID (stable across a rename within one database lifetime) lets the differ, when
// the same OID resurfaces under a new name, emit a single TableRenamed carrying
// both names instead of the misleading drop+add pair.

// The canonical rename: same OID, new name. The differ must emit exactly one
// TableRenamed (with FromName/ToName populated, qualified) and crucially must
// NOT also emit a TableAdded or TableDropped — the whole point is that the data
// moved, it was not destroyed and recreated.
func TestDiffSchema_TableRenamedByOID(t *testing.T) {
	from := emptySnap("a")
	from.Tables = []snapshot.Table{{OID: 42, Schema: "public", Name: "old_name", Columns: []snapshot.Column{{Name: "id"}}}}
	to := emptySnap("b")
	to.Tables = []snapshot.Table{{OID: 42, Schema: "public", Name: "new_name", Columns: []snapshot.Column{{Name: "id"}}}}

	delta, _ := DiffSchema(from, to)
	r := findChange(t, delta.Changes, TableRenamed)
	if r.Rename == nil || r.Rename.FromName != "public.old_name" || r.Rename.ToName != "public.new_name" {
		t.Errorf("rename names: got %+v", r.Rename)
	}
	if r.Object.Name != "new_name" || r.Object.OID != 42 {
		t.Errorf("rename object should carry the new name + OID: got %+v", r.Object)
	}
	for _, c := range delta.Changes {
		if c.Type == TableAdded || c.Type == TableDropped {
			t.Errorf("rename must not also produce %s: %+v", c.Type, c)
		}
	}
}

// A rename and a body change can land in the same diff. The renamed table's
// per-attribute changes (here a new column) must attach under the *new* name and
// OID, not the old one, so the cloud folds them onto the surviving identity.
func TestDiffSchema_RenameCarriesBodyChanges(t *testing.T) {
	from := emptySnap("a")
	from.Tables = []snapshot.Table{{OID: 7, Schema: "public", Name: "before", Columns: []snapshot.Column{{Name: "id"}}}}
	to := emptySnap("b")
	to.Tables = []snapshot.Table{{OID: 7, Schema: "public", Name: "after", Columns: []snapshot.Column{{Name: "id"}, {Name: "extra"}}}}

	delta, _ := DiffSchema(from, to)
	findChange(t, delta.Changes, TableRenamed)
	add := findChange(t, delta.Changes, ColumnAdded)
	if add.Object.Name != "after" || add.Object.OID != 7 {
		t.Errorf("body change should attach under the renamed identity: got %+v", add.Object)
	}
}

// OID matching only fires on tables left unmatched by name. When the OID is
// absent (0) — older snapshots, or the planner/activity channels that carry no
// OID — there is nothing to pair on, so a name change must degrade gracefully to
// the old drop+add behavior rather than mis-detecting a rename.
func TestDiffSchema_NoOIDFallsBackToDropAdd(t *testing.T) {
	from := emptySnap("a")
	from.Tables = []snapshot.Table{{Schema: "public", Name: "old_name"}} // OID 0
	to := emptySnap("b")
	to.Tables = []snapshot.Table{{Schema: "public", Name: "new_name"}} // OID 0

	delta, _ := DiffSchema(from, to)
	findChange(t, delta.Changes, TableAdded)
	findChange(t, delta.Changes, TableDropped)
	for _, c := range delta.Changes {
		if c.Type == TableRenamed {
			t.Fatalf("no OID to key on — must not detect a rename: %+v", c)
		}
	}
}

// A genuine drop of one table plus an unrelated add of another, both carrying
// OIDs, must NOT be collapsed into a rename: the OIDs differ, so the identities
// are distinct and the destructive drop has to stay visible.
func TestDiffSchema_DistinctOIDsStayDropAdd(t *testing.T) {
	from := emptySnap("a")
	from.Tables = []snapshot.Table{{OID: 1, Schema: "public", Name: "gone"}}
	to := emptySnap("b")
	to.Tables = []snapshot.Table{{OID: 2, Schema: "public", Name: "fresh"}}

	delta, _ := DiffSchema(from, to)
	findChange(t, delta.Changes, TableAdded)
	findChange(t, delta.Changes, TableDropped)
	for _, c := range delta.Changes {
		if c.Type == TableRenamed {
			t.Fatalf("distinct OIDs are distinct identities, not a rename: %+v", c)
		}
	}
}

// Adding a column is not one event but three risk profiles, and the differ has
// to tell them apart up front because the cloud can't recover the distinction
// later: a nullable column with no default is metadata-only, a constant default
// is cheap on PG 11+, but a *volatile* default (now(), nextval(), ...) forces a
// full table rewrite. This table-driven case feeds one of each and asserts the
// classifier bucketed DefaultKind correctly and preserved the nullability flag.
func TestDiffSchema_ColumnAddedClassifiesDefault(t *testing.T) {
	base := func() *snapshot.Table {
		return &snapshot.Table{Schema: "public", Name: "t", Columns: []snapshot.Column{{Name: "id", TypeName: "int4"}}}
	}
	from := emptySnap("a")
	from.Tables = []snapshot.Table{*base()}

	cases := []struct {
		name    string
		def     *string
		norm    DefaultKind
		colNull bool
	}{
		{"nullable_no_default", nil, DefaultNone, true},
		{"constant_default", strp("0"), DefaultConstant, false},
		{"volatile_default", strp("now()"), DefaultVolatile, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			to := emptySnap("b")
			tbl := *base()
			tbl.Columns = append(tbl.Columns, snapshot.Column{Name: "c", TypeName: "int4", Nullable: tc.colNull, Default: tc.def})
			to.Tables = []snapshot.Table{tbl}

			delta, _ := DiffSchema(from, to)
			c := findChange(t, delta.Changes, ColumnAdded)
			if c.Column.DefaultKind != tc.norm {
				t.Errorf("default kind: want %s got %s", tc.norm, c.Column.DefaultKind)
			}
			if c.Column.Nullable == nil || *c.Column.Nullable != tc.colNull {
				t.Errorf("nullable: want %v got %v", tc.colNull, c.Column.Nullable)
			}
		})
	}
}

// A column type change carries a Widening bool because widening (int4->int8,
// varchar->text) is a comparatively cheap, lossless promotion while narrowing or
// an unrelated retype risks data loss and a rewrite. The heuristic is deliberately
// conservative: anything it doesn't recognize as a known-safe widening reads as
// non-widening so the cloud errs toward the riskier interpretation. These cases
// cover an integer promotion, the reverse narrowing, the varchar->text widening,
// its reverse, and a cross-family change that has no widening relationship at all.
func TestDiffSchema_ColumnTypeWidening(t *testing.T) {
	mk := func(typeName string) *snapshot.SchemaSnapshot {
		s := emptySnap(typeName)
		s.Tables = []snapshot.Table{{Schema: "public", Name: "t", Columns: []snapshot.Column{{Name: "c", TypeName: typeName}}}}
		return s
	}
	cases := []struct {
		from, to string
		widening bool
	}{
		{"int4", "int8", true},  // integer promotion: lossless widening
		{"int8", "int4", false}, // the reverse is a narrowing, not a widening
		{"varchar(50)", "text", true},  // bounded varchar to unbounded text widens
		{"text", "varchar(50)", false}, // clamping text into varchar(50) does not
		{"int4", "text", false},        // unrelated families: not a widening
	}
	for _, tc := range cases {
		delta, _ := DiffSchema(mk(tc.from), mk(tc.to))
		c := findChange(t, delta.Changes, ColumnTypeChanged)
		if c.Column.Widening == nil || *c.Column.Widening != tc.widening {
			t.Errorf("%s→%s: want widening=%v got %v", tc.from, tc.to, tc.widening, c.Column.Widening)
		}
	}
}

// Flipping a column's nullability is directional, and the two directions carry
// very different operational weight: SET NOT NULL has to validate every existing
// row (and fails outright if any are null), while DROP NOT NULL is a trivial
// catalog flip. So they get distinct ChangeTypes rather than a single "nullability
// changed". We diff the pair both ways to prove each direction maps to its own type.
func TestDiffSchema_NotNullToggle(t *testing.T) {
	from := emptySnap("a")
	from.Tables = []snapshot.Table{{Schema: "public", Name: "t", Columns: []snapshot.Column{{Name: "c", TypeName: "int4", Nullable: true}}}}
	to := emptySnap("b")
	to.Tables = []snapshot.Table{{Schema: "public", Name: "t", Columns: []snapshot.Column{{Name: "c", TypeName: "int4", Nullable: false}}}}
	delta, _ := DiffSchema(from, to)
	findChange(t, delta.Changes, ColumnSetNotNull) // nullable -> NOT NULL

	// Swapping the operands runs the same column the other way, which must now
	// register as dropping the NOT NULL rather than setting it.
	delta, _ = DiffSchema(to, from)
	findChange(t, delta.Changes, ColumnDropNotNull)
}

// Dropping an index is usually low-stakes, but dropping one that backs a primary
// key or unique constraint is a different animal entirely. The differ therefore
// carries the BacksConstraint flag straight off the snapshot's Index struct onto
// the IndexDropped detail so the cloud can gate severity on it. Here we drop a
// constraint-backing index and assert the flag survived into the typed change.
func TestDiffSchema_IndexDroppedBacksConstraint(t *testing.T) {
	from := emptySnap("a")
	from.Tables = []snapshot.Table{{Schema: "public", Name: "t", Indexes: []snapshot.Index{
		{Name: "t_pkey", IsUnique: true, BacksConstraint: true},
	}}}
	to := emptySnap("b")
	to.Tables = []snapshot.Table{{Schema: "public", Name: "t"}}
	delta, _ := DiffSchema(from, to)
	c := findChange(t, delta.Changes, IndexDropped)
	if !c.Index.BacksConstraint {
		t.Errorf("expected backs_constraint true, got %+v", c.Index)
	}
}

// Row-level security toggling is a security-visibility change, so the differ both
// emits RLSToggled and records which way it went via RLS.Enabled. Turning RLS on
// can suddenly hide rows from an app that wasn't written for it; turning it off
// can expose rows that were meant to be filtered. We enable it here and assert
// both that the change fired and that Enabled captured the new state.
func TestDiffSchema_RLSToggled(t *testing.T) {
	from := emptySnap("a")
	from.Tables = []snapshot.Table{{Schema: "public", Name: "t", RLSEnabled: false}}
	to := emptySnap("b")
	to.Tables = []snapshot.Table{{Schema: "public", Name: "t", RLSEnabled: true}}
	delta, _ := DiffSchema(from, to)
	c := findChange(t, delta.Changes, RLSToggled)
	if c.RLS == nil || !c.RLS.Enabled {
		t.Errorf("expected RLS enabled, got %+v", c.RLS)
	}
}

// A function gaining SECURITY DEFINER widens the privilege surface: it starts
// executing with the definer's rights instead of the caller's, which is exactly
// the pattern privilege-escalation bugs hide behind. The differ flags it and uses
// a *bool (SecurityDefiner) so the cloud can distinguish "added" from "removed".
// The function identity is keyed on name+args, so the same f(integer) on both
// sides is matched as a modification rather than a drop+add.
func TestDiffSchema_FuncSecurityDefiner(t *testing.T) {
	from := emptySnap("a")
	from.Functions = []snapshot.Function{{Schema: "public", Name: "f", IdentityArgs: "integer", SecurityDefiner: false}}
	to := emptySnap("b")
	to.Functions = []snapshot.Function{{Schema: "public", Name: "f", IdentityArgs: "integer", SecurityDefiner: true}}
	delta, _ := DiffSchema(from, to)
	c := findChange(t, delta.Changes, FuncSecurityDefiner)
	if c.Function == nil || c.Function.SecurityDefiner == nil || !*c.Function.SecurityDefiner {
		t.Errorf("expected SECURITY DEFINER added, got %+v", c.Function)
	}
}

// Determinism is load-bearing, not cosmetic: the differ walks Go maps internally
// (whose iteration order is randomized), and the cloud dedups change hindsights by
// hashing the serialized delta. If two diffs of the same inputs serialized to
// different byte streams, the cloud would treat re-analysis of an unchanged push
// as a brand-new finding and double-insert. We feed deliberately out-of-order
// tables, diff twice, assert the JSON is byte-identical, and confirm the changes
// came out sorted by object name (the stable key sortChanges imposes).
func TestDiffSchema_Deterministic(t *testing.T) {
	from := emptySnap("a")
	to := emptySnap("b")
	for _, n := range []string{"z", "a", "m", "b"} {
		to.Tables = append(to.Tables, snapshot.Table{Schema: "public", Name: n})
	}
	d1, _ := DiffSchema(from, to)
	d2, _ := DiffSchema(from, to)
	j1, _ := json.Marshal(d1)
	j2, _ := json.Marshal(d2)
	if !bytes.Equal(j1, j2) {
		t.Fatalf("DiffSchema not deterministic:\n%s\n%s", j1, j2)
	}
	// The four tables were appended z, a, m, b; a stable diff must hand them back
	// alphabetized regardless of the map walk that discovered them.
	var names []string
	for _, c := range d1.Changes {
		names = append(names, c.Object.Name)
	}
	if strings.Join(names, ",") != "a,b,m,z" {
		t.Errorf("expected sorted order a,b,m,z got %v", names)
	}
}

// The default (non-JSON) console output groups changes under the object they
// touch and renders a +/~/- tree: a modified table prints as a "~ table ..."
// header with its per-column changes indented beneath. This test adds one
// nullable column to an existing table and asserts both the grouped header and
// the child line (including the nullable annotation) show up, since that human
// rendering is the surface most users actually read.
func TestRenderConsole_SchemaTree(t *testing.T) {
	from := emptySnap("aaaaaaaaaaaaaaaa")
	from.Tables = []snapshot.Table{{Schema: "public", Name: "users", Columns: []snapshot.Column{{Name: "id", TypeName: "int4"}}}}
	to := emptySnap("bbbbbbbbbbbbbbbb")
	to.Tables = []snapshot.Table{{Schema: "public", Name: "users", Columns: []snapshot.Column{
		{Name: "id", TypeName: "int4"},
		{Name: "email", TypeName: "text", Nullable: true},
	}}}
	delta, _ := DiffSchema(from, to)
	env := &SnapshotDiff{Kind: "schema", FromHash: from.ContentHash, ToHash: to.ContentHash, Schema: delta}

	var buf bytes.Buffer
	RenderConsole(&buf, env)
	out := buf.String()
	if !strings.Contains(out, "~ table public.users") {
		t.Errorf("missing table header:\n%s", out)
	}
	if !strings.Contains(out, "+ column email added (nullable)") {
		t.Errorf("missing column line:\n%s", out)
	}
}

// Two empty schemas with different hashes still diff to nothing structural, and
// the console must say so plainly rather than printing an empty tree that leaves
// the user wondering whether the command actually ran. This guards that "no
// changes" affordance.
func TestRenderConsole_NoChanges(t *testing.T) {
	from := emptySnap("a")
	to := emptySnap("b")
	delta, _ := DiffSchema(from, to)
	var buf bytes.Buffer
	RenderConsole(&buf, &SnapshotDiff{Kind: "schema", Schema: delta})
	if !strings.Contains(buf.String(), "no changes") {
		t.Errorf("expected 'no changes', got %q", buf.String())
	}
}
