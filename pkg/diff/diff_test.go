package diff

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

// emptySnap builds a minimal-but-valid SchemaSnapshot carrying just the bits the
// differ keys on: a content hash (carried through onto the DriftReport) and a
// timestamp (so the SnapshotDiff envelope has real endpoints). Every test starts
// from one of these and layers in only the objects it cares about, which keeps each
// case focused on a single change-vocabulary entry.
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
		{"int4", "int8", true},         // integer promotion: lossless widening
		{"int8", "int4", false},        // the reverse is a narrowing, not a widening
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
// (whose iteration order is randomized), and consumers dedup changes by
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
	RenderConsoleMinPct(&buf, env, DefaultMinPct)
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
	RenderConsoleMinPct(&buf, &SnapshotDiff{Kind: "schema", Schema: delta}, DefaultMinPct)
	if !strings.Contains(buf.String(), "no changes") {
		t.Errorf("expected 'no changes', got %q", buf.String())
	}
}

// tableWithReloptions is the storage-param fixture: one table, whatever reloptions the
// case needs. pg reports them as "name=value" strings on pg_class.reloptions.
func tableWithReloptions(reloptions ...string) *snapshot.SchemaSnapshot {
	s := emptySnap("h")
	s.Tables = []snapshot.Table{{Schema: "public", Name: "events", Reloptions: reloptions}}
	return s
}

// One ALTER TABLE ... SET (...) sets several knobs, and it is one user action; it must
// produce one table-grain change carrying them all, not one change per knob.
func TestDiffSchema_StorageParamsSetTogether(t *testing.T) {
	from := tableWithReloptions()
	to := tableWithReloptions(
		"autovacuum_vacuum_threshold=5000",
		"autovacuum_vacuum_scale_factor=0.007",
	)

	delta, _ := DiffSchema(from, to)
	if len(delta.Changes) != 1 {
		t.Fatalf("want 1 change for one ALTER, got %d", len(delta.Changes))
	}
	c := findChange(t, delta.Changes, StorageParamsChanged)
	if len(c.StorageParam) != 2 {
		t.Fatalf("want 2 params, got %d", len(c.StorageParam))
	}
	// sorted by name, so the change bytes are stable across pg's set-order
	if c.StorageParam[0].Name != "autovacuum_vacuum_scale_factor" {
		t.Errorf("params not sorted by name: %v", c.StorageParam[0].Name)
	}
	if c.StorageParam[0].From != nil {
		t.Errorf("newly-set param should have nil From, got %q", *c.StorageParam[0].From)
	}
	if c.StorageParam[0].To == nil || *c.StorageParam[0].To != "0.007" {
		t.Errorf("To not carried: %+v", c.StorageParam[0])
	}
}

func TestDiffSchema_StorageParamRetuned(t *testing.T) {
	from := tableWithReloptions("fillfactor=90")
	to := tableWithReloptions("fillfactor=70")

	delta, _ := DiffSchema(from, to)
	c := findChange(t, delta.Changes, StorageParamsChanged)
	p := c.StorageParam[0]
	if p.From == nil || *p.From != "90" || p.To == nil || *p.To != "70" {
		t.Errorf("want 90 -> 70, got %+v", p)
	}
}

// ALTER TABLE ... RESET (...) drops the param: To is nil, From carries what it was.
func TestDiffSchema_StorageParamReset(t *testing.T) {
	from := tableWithReloptions("autovacuum_enabled=off")
	to := tableWithReloptions()

	delta, _ := DiffSchema(from, to)
	c := findChange(t, delta.Changes, StorageParamsChanged)
	p := c.StorageParam[0]
	if p.From == nil || *p.From != "off" {
		t.Errorf("want From=off, got %+v", p)
	}
	if p.To != nil {
		t.Errorf("reset param should have nil To, got %q", *p.To)
	}
}

// pg_class.reloptions is in set-order, not identity order. Reordering is not a change.
func TestDiffSchema_StorageParamOrderIsNotAChange(t *testing.T) {
	from := tableWithReloptions("fillfactor=70", "autovacuum_enabled=off")
	to := tableWithReloptions("autovacuum_enabled=off", "fillfactor=70")

	delta, _ := DiffSchema(from, to)
	if !delta.IsEmpty() {
		t.Errorf("reordered reloptions reported as a change: %+v", delta.Changes)
	}
}

// The regression this whole change exists to prevent: ClassifyDrift used to short-circuit
// on ContentHash equality, so two snapshots hashed under different format_versions came
// back "diverged" with an empty change list. Identity must come from the delta.
func TestClassifyDrift_IdenticalAcrossHashAlgorithms(t *testing.T) {
	saved := tableWithReloptions("fillfactor=70")
	saved.FormatVersion = 1
	saved.ContentHash = snapshot.ComputeStructuralHash(saved)

	live := tableWithReloptions("fillfactor=70")
	live.FormatVersion = 2
	live.ContentHash = snapshot.ComputeContentHashV2(live)

	if saved.ContentHash == live.ContentHash {
		t.Fatal("fixture is not exercising the mismatch; hashes are equal")
	}
	report := ClassifyDrift(saved, live)
	if report.Direction != DriftIdentical {
		t.Errorf("want identical, got %q with %d changes", report.Direction, len(report.Delta.Changes))
	}
}

// A real settings change still reports, and counts as modified rather than added/removed.
func TestClassifyDrift_StorageParamsCountAsModified(t *testing.T) {
	saved := tableWithReloptions()
	live := tableWithReloptions("autovacuum_enabled=off")

	report := ClassifyDrift(saved, live)
	if report.Direction != DriftDiverged {
		t.Errorf("want diverged, got %q", report.Direction)
	}
	if report.ModifiedCount != 1 || report.AddedCount != 0 || report.RemovedCount != 0 {
		t.Errorf("want 1 modified, got +%d -%d ~%d",
			report.AddedCount, report.RemovedCount, report.ModifiedCount)
	}
}

// The cloud feed renders ScoredChange.Note and nothing else for a table-level change
// (changeName has no pointer to read), so the param list has to ride the Note or the
// card shows only "storage_params_changed" with no diff.
func TestDiffSchema_StorageParamsCarryNote(t *testing.T) {
	from := tableWithReloptions("autovacuum_vacuum_scale_factor=0.02")
	to := tableWithReloptions(
		"autovacuum_vacuum_scale_factor=0.007",
		"autovacuum_vacuum_threshold=5000",
	)

	delta, _ := DiffSchema(from, to)
	c := findChange(t, delta.Changes, StorageParamsChanged)
	want := "autovacuum_vacuum_scale_factor 0.02 → 0.007, " +
		"autovacuum_vacuum_threshold 50 → 5000 (was postgres default)"
	if c.Note != want {
		t.Errorf("note:\n got %q\nwant %q", c.Note, want)
	}
}

// A param the table never set still had an effective value, inherited from the cluster.
// Naming it turns "set 0.007" into a real delta. The captured GUC wins over pg's stock
// default, because a cluster that tunes autovacuum in postgresql.conf would otherwise be
// shown a before-value it never had.
func TestDiffSchema_StorageParamBaselineFromClusterGUC(t *testing.T) {
	from := tableWithReloptions()
	from.GUCs = []snapshot.GucSetting{{Name: "autovacuum_vacuum_scale_factor", Setting: "0.1"}}
	to := tableWithReloptions("autovacuum_vacuum_scale_factor=0.007")
	to.GUCs = from.GUCs

	delta, _ := DiffSchema(from, to)
	c := findChange(t, delta.Changes, StorageParamsChanged)
	p := c.StorageParam[0]
	if p.Baseline != "0.1" || p.BaselineFrom != "cluster" {
		t.Fatalf("baseline: got %q from %q, want 0.1 from cluster", p.Baseline, p.BaselineFrom)
	}
	before, after, origin := StorageParamValues(p)
	if before != "0.1" || after != "0.007" || origin != "was cluster default" {
		t.Errorf("values: got %q → %q (%q)", before, after, origin)
	}
}

// No GUC captured (older docs) falls back to pg's stock default, and says so.
func TestDiffSchema_StorageParamBaselineFallsBackToPgDefault(t *testing.T) {
	delta, _ := DiffSchema(tableWithReloptions(), tableWithReloptions("fillfactor=70"))
	p := findChange(t, delta.Changes, StorageParamsChanged).StorageParam[0]
	if p.Baseline != "100" || p.BaselineFrom != "pg" {
		t.Fatalf("baseline: got %q from %q, want 100 from pg", p.Baseline, p.BaselineFrom)
	}
}

// A RESET hands the param back to the baseline; that is the after value, not the before.
func TestDiffSchema_StorageParamResetShowsBaselineAsAfter(t *testing.T) {
	delta, _ := DiffSchema(tableWithReloptions("autovacuum_enabled=off"), tableWithReloptions())
	p := findChange(t, delta.Changes, StorageParamsChanged).StorageParam[0]
	before, after, origin := StorageParamValues(p)
	if before != "off" || after != "on" || origin != "reset to postgres default" {
		t.Errorf("values: got %q → %q (%q)", before, after, origin)
	}
}

// A param with no GUC and no known default states only what it can: the new value.
func TestDiffSchema_StorageParamUnknownHasNoBaseline(t *testing.T) {
	delta, _ := DiffSchema(tableWithReloptions(), tableWithReloptions("parallel_workers=8"))
	p := findChange(t, delta.Changes, StorageParamsChanged).StorageParam[0]
	if p.Baseline != "" {
		t.Fatalf("invented a baseline for parallel_workers: %q", p.Baseline)
	}
	if before, after, origin := StorageParamValues(p); before != "∅" || after != "8" || origin != "" {
		t.Errorf("values: got %q → %q (%q)", before, after, origin)
	}
}

// A retune names both sides itself, so no baseline is resolved or shown.
func TestDiffSchema_StorageParamRetuneHasNoBaseline(t *testing.T) {
	delta, _ := DiffSchema(
		tableWithReloptions("autovacuum_vacuum_scale_factor=0.02"),
		tableWithReloptions("autovacuum_vacuum_scale_factor=0.007"))
	p := findChange(t, delta.Changes, StorageParamsChanged).StorageParam[0]
	if p.Baseline != "" || p.BaselineFrom != "" {
		t.Errorf("retune resolved a baseline it does not need: %+v", p)
	}
}

// PG17 can change what a generated column computes without touching anything
// else. That used to surface as a default change, because the capture read the
// expression out of pg_attrdef as default_expr; now it has its own change type
// and must not go silent.
func TestDiffSchema_GenerationExprChange(t *testing.T) {
	build := func(hash, expr string) *snapshot.SchemaSnapshot {
		e, kind := expr, "stored"
		snap := emptySnap(hash)
		snap.Tables = []snapshot.Table{{
			Schema: "public", Name: "orders",
			Columns: []snapshot.Column{{
				Name: "total", Ordinal: 1, TypeName: "integer",
				Generated: &kind, GenerationExpr: &e,
			}},
		}}
		return snap
	}

	delta, err := DiffSchema(build("h1", "(a * b)"), build("h2", "(a + b)"))
	if err != nil {
		t.Fatalf("DiffSchema: %v", err)
	}
	var found *Change
	for i := range delta.Changes {
		if delta.Changes[i].Type == ColumnGenerationChanged {
			found = &delta.Changes[i]
		}
	}
	if found == nil {
		t.Fatalf("generation expression change went unreported: %+v", delta.Changes)
	}
	if found.Column.FromExpr == nil || *found.Column.FromExpr != "(a * b)" {
		t.Errorf("want the old expression, got %v", found.Column.FromExpr)
	}
	if found.Column.ToExpr == nil || *found.Column.ToExpr != "(a + b)" {
		t.Errorf("want the new expression, got %v", found.Column.ToExpr)
	}

	same, err := DiffSchema(build("h1", "(a * b)"), build("h1", "(a * b)"))
	if err != nil {
		t.Fatalf("DiffSchema: %v", err)
	}
	if len(same.Changes) != 0 {
		t.Errorf("identical schemas reported %d changes", len(same.Changes))
	}
}

// --- triggers and RLS policies ---
//
// Both are captured on snapshot.Table and feed the content hash, so before these
// differs existed a version that only touched a trigger or a policy moved the hash
// yet diffed as "no schema change" (and `snapshot list --table` dropped it).

// trigSnap builds a one-table snapshot carrying the given triggers. The OID is
// fixed so a test can rename the table on one side and still have the differ pair
// the two tables by OID (that is how TableRenamed is detected).
func trigSnap(hash, schema, name string, trigs ...snapshot.Trigger) *snapshot.SchemaSnapshot {
	s := emptySnap(hash)
	s.Tables = []snapshot.Table{{OID: 42, Schema: schema, Name: name, Triggers: trigs}}
	return s
}

// polSnap is the policy counterpart of trigSnap.
func polSnap(hash, schema, name string, pols ...snapshot.RlsPolicy) *snapshot.SchemaSnapshot {
	s := emptySnap(hash)
	s.Tables = []snapshot.Table{{OID: 42, Schema: schema, Name: name, Policies: pols}}
	return s
}

// changeTypes flattens a delta to its sorted type list so table-driven cases can
// assert the exact set of changes, catching both missing and spurious ones.
func changeTypes(d *SchemaDelta) []string {
	var out []string
	for _, c := range d.Changes {
		out = append(out, string(c.Type))
	}
	slices.Sort(out)
	return out
}

// Trigger comparison is keyed by name inside the already-paired table and compares
// pg_get_triggerdef text with the `ON <relation>` blanked. The rename cases are the
// point of the blanking: pg_get_triggerdef always schema-qualifies the relation, so
// without it every table rename / SET SCHEMA would report each trigger as changed.
// A trigger rename has no OID to pair on, so it is deliberately a drop + add.
func TestDiffSchema_Triggers(t *testing.T) {
	const (
		before = `CREATE TRIGGER audit_t BEFORE UPDATE ON public.t FOR EACH ROW EXECUTE FUNCTION audit_fn()`
		after  = `CREATE TRIGGER audit_t AFTER UPDATE ON public.t FOR EACH ROW EXECUTE FUNCTION audit_fn()`
	)
	trig := func(name, def string) snapshot.Trigger { return snapshot.Trigger{Name: name, Definition: def} }
	cases := []struct {
		name string
		from *snapshot.SchemaSnapshot
		to   *snapshot.SchemaSnapshot
		want []string
	}{
		{"added", trigSnap("a", "public", "t"), trigSnap("b", "public", "t", trig("audit_t", before)),
			[]string{"trigger_added"}},
		{"dropped", trigSnap("a", "public", "t", trig("audit_t", before)), trigSnap("b", "public", "t"),
			[]string{"trigger_dropped"}},
		{"definition changed", trigSnap("a", "public", "t", trig("audit_t", before)),
			trigSnap("b", "public", "t", trig("audit_t", after)), []string{"trigger_changed"}},
		{"identical", trigSnap("a", "public", "t", trig("audit_t", before)),
			trigSnap("b", "public", "t", trig("audit_t", before)), nil},
		{"table renamed", trigSnap("a", "public", "t", trig("audit_t", before)),
			trigSnap("b", "public", "t2", trig("audit_t", strings.Replace(before, "public.t ", "public.t2 ", 1))),
			[]string{"table_renamed"}},
		{"set schema", trigSnap("a", "public", "t", trig("audit_t", before)),
			trigSnap("b", "archive", "t", trig("audit_t", strings.Replace(before, "public.t ", "archive.t ", 1))),
			[]string{"table_renamed"}},
		{"quoted relation renamed",
			trigSnap("a", "public", "Order Items", trig("audit_t", `CREATE TRIGGER audit_t AFTER INSERT ON public."Order Items" FOR EACH ROW EXECUTE FUNCTION audit_fn()`)),
			trigSnap("b", "public", "Line Items", trig("audit_t", `CREATE TRIGGER audit_t AFTER INSERT ON public."Line Items" FOR EACH ROW EXECUTE FUNCTION audit_fn()`)),
			[]string{"table_renamed"}},
		{"rename plus real change", trigSnap("a", "public", "t", trig("audit_t", before)),
			trigSnap("b", "public", "t2", trig("audit_t", strings.Replace(after, "public.t ", "public.t2 ", 1))),
			[]string{"table_renamed", "trigger_changed"}},
		{"trigger renamed", trigSnap("a", "public", "t", trig("audit_t", before)),
			trigSnap("b", "public", "t", trig("audit_t2", strings.Replace(before, "audit_t ", "audit_t2 ", 1))),
			[]string{"trigger_added", "trigger_dropped"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			delta, _ := DiffSchema(tc.from, tc.to)
			if got := changeTypes(delta); !slices.Equal(got, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, got)
			}
		})
	}

	// payload shape: added carries only the new text, changed carries both sides
	delta, _ := DiffSchema(trigSnap("a", "public", "t"), trigSnap("b", "public", "t", trig("audit_t", before)))
	if c := findChange(t, delta.Changes, TriggerAdded); c.Trigger == nil || c.Trigger.ToDefinition != before || c.Trigger.FromDefinition != "" {
		t.Errorf("added payload: %+v", c.Trigger)
	}
	delta, _ = DiffSchema(trigSnap("a", "public", "t", trig("audit_t", before)), trigSnap("b", "public", "t", trig("audit_t", after)))
	if c := findChange(t, delta.Changes, TriggerChanged); c.Trigger.FromDefinition != before || c.Trigger.ToDefinition != after {
		t.Errorf("changed payload: %+v", c.Trigger)
	}
}

// triggerBody blanks only the relation after the first unquoted ` ON `. Quoted
// identifiers can contain spaces, `""` escapes and the word ON, so the scan has to
// track double-quote state; anything it cannot parse comes back unchanged, which can
// at worst cause a spurious trigger_changed, never hide a real change.
func TestTriggerBody(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"plain",
			`CREATE TRIGGER a AFTER INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION f()`,
			`CREATE TRIGGER a AFTER INSERT ON $rel FOR EACH ROW EXECUTE FUNCTION f()`},
		{"quoted relation with space and escaped quote",
			`CREATE TRIGGER a AFTER INSERT ON "my schema"."a ""b"" c" FOR EACH ROW EXECUTE FUNCTION f()`,
			`CREATE TRIGGER a AFTER INSERT ON $rel FOR EACH ROW EXECUTE FUNCTION f()`},
		{"quoted trigger name containing ON",
			`CREATE TRIGGER "x ON y" AFTER INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION f()`,
			`CREATE TRIGGER "x ON y" AFTER INSERT ON $rel FOR EACH ROW EXECUTE FUNCTION f()`},
		{"update of quoted column named on",
			`CREATE TRIGGER a AFTER UPDATE OF "on" ON public.t FOR EACH ROW EXECUTE FUNCTION f()`,
			`CREATE TRIGGER a AFTER UPDATE OF "on" ON $rel FOR EACH ROW EXECUTE FUNCTION f()`},
		{"constraint trigger keeps FROM relation",
			`CREATE CONSTRAINT TRIGGER a AFTER INSERT ON public.t FROM public.u NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION f()`,
			`CREATE CONSTRAINT TRIGGER a AFTER INSERT ON $rel FROM public.u NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION f()`},
		{"no ON clause", `garbage`, `garbage`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := triggerBody(tc.in); got != tc.want {
				t.Errorf("\nwant %s\ngot  %s", tc.want, got)
			}
		})
	}
}

// Policies compare on command, mode, roles (as a set), USING and WITH CHECK. Roles
// come from an unordered array_agg, so order must not matter. Capture drops PUBLIC
// (OID 0), so nil roles mean PUBLIC; a lone "public" is normalised to nil now so a
// future capture fix that starts emitting it does not flag every `TO public` policy.
func TestDiffSchema_Policies(t *testing.T) {
	base := snapshot.RlsPolicy{Name: "p", Command: "ALL", Permissive: true, Roles: []string{"app"}, UsingExpr: strp("tenant_id = 1")}
	with := func(f func(*snapshot.RlsPolicy)) snapshot.RlsPolicy {
		p := base
		p.Roles = slices.Clone(base.Roles)
		f(&p)
		return p
	}
	cases := []struct {
		name     string
		from, to *snapshot.SchemaSnapshot
		want     []string
	}{
		{"added", polSnap("a", "public", "t"), polSnap("b", "public", "t", base), []string{"policy_added"}},
		{"dropped", polSnap("a", "public", "t", base), polSnap("b", "public", "t"), []string{"policy_dropped"}},
		{"identical", polSnap("a", "public", "t", base), polSnap("b", "public", "t", base), nil},
		{"using changed", polSnap("a", "public", "t", base),
			polSnap("b", "public", "t", with(func(p *snapshot.RlsPolicy) { p.UsingExpr = strp("true") })), []string{"policy_changed"}},
		{"with check set", polSnap("a", "public", "t", base),
			polSnap("b", "public", "t", with(func(p *snapshot.RlsPolicy) { p.WithCheckExpr = strp("true") })), []string{"policy_changed"}},
		{"permissive to restrictive", polSnap("a", "public", "t", base),
			polSnap("b", "public", "t", with(func(p *snapshot.RlsPolicy) { p.Permissive = false })), []string{"policy_changed"}},
		{"command changed", polSnap("a", "public", "t", base),
			polSnap("b", "public", "t", with(func(p *snapshot.RlsPolicy) { p.Command = "SELECT" })), []string{"policy_changed"}},
		{"roles reordered", polSnap("a", "public", "t", with(func(p *snapshot.RlsPolicy) { p.Roles = []string{"b", "a"} })),
			polSnap("b", "public", "t", with(func(p *snapshot.RlsPolicy) { p.Roles = []string{"a", "b"} })), nil},
		{"public to role", polSnap("a", "public", "t", with(func(p *snapshot.RlsPolicy) { p.Roles = nil })),
			polSnap("b", "public", "t", base), []string{"policy_changed"}},
		{"nil roles equals lone public", polSnap("a", "public", "t", with(func(p *snapshot.RlsPolicy) { p.Roles = nil })),
			polSnap("b", "public", "t", with(func(p *snapshot.RlsPolicy) { p.Roles = []string{"public"} })), nil},
		{"table renamed, policy identical", polSnap("a", "public", "t", base), polSnap("b", "public", "t2", base),
			[]string{"table_renamed"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			delta, _ := DiffSchema(tc.from, tc.to)
			if got := changeTypes(delta); !slices.Equal(got, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, got)
			}
		})
	}

	// the payload carries the normalised (sorted) roles
	delta, _ := DiffSchema(polSnap("a", "public", "t"),
		polSnap("b", "public", "t", with(func(p *snapshot.RlsPolicy) { p.Roles = []string{"b", "a"} })))
	if c := findChange(t, delta.Changes, PolicyAdded); c.Policy == nil || c.Policy.To == nil || !slices.Equal(c.Policy.To.Roles, []string{"a", "b"}) {
		t.Errorf("added payload: %+v", c.Policy)
	}
}

// Describe is what snapshot list, snapshot diff and drift print. Triggers show the
// definition minus the CREATE TRIGGER prefix so `snapshot list -p | grep audit_fn`
// finds them; policies spell out the expressions because a USING edit is the
// security-relevant part. A nil payload must not panic.
func TestDescribe_TriggerPolicy(t *testing.T) {
	def := `CREATE TRIGGER audit_t AFTER INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION audit_fn()`
	defB := `CREATE TRIGGER audit_t BEFORE INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION audit_fn()`
	pol := &snapshot.RlsPolicy{Name: "p", Command: "ALL", Permissive: true, UsingExpr: strp("tenant_id = 1")}
	pol2 := &snapshot.RlsPolicy{Name: "p", Command: "ALL", Permissive: false, Roles: []string{"admin", "app"},
		UsingExpr: strp("true"), WithCheckExpr: strp("true")}
	cases := []struct {
		name string
		c    Change
		want string
	}{
		{"trigger added", Change{Type: TriggerAdded, Trigger: &TriggerChange{Name: "audit_t", ToDefinition: def}},
			"trigger audit_t added (AFTER INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION audit_fn())"},
		{"trigger dropped", Change{Type: TriggerDropped, Trigger: &TriggerChange{Name: "audit_t", FromDefinition: def}},
			"trigger audit_t dropped (AFTER INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION audit_fn())"},
		{"trigger changed", Change{Type: TriggerChanged, Trigger: &TriggerChange{Name: "audit_t", FromDefinition: defB, ToDefinition: def}},
			"trigger audit_t: BEFORE INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION audit_fn() → AFTER INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION audit_fn()"},
		{"quoted trigger name with space", Change{Type: TriggerAdded, Trigger: &TriggerChange{Name: "a b",
			ToDefinition: `CREATE TRIGGER "a b" AFTER INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION f()`}},
			"trigger a b added (AFTER INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION f())"},
		{"constraint trigger", Change{Type: TriggerAdded, Trigger: &TriggerChange{Name: "c",
			ToDefinition: `CREATE CONSTRAINT TRIGGER c AFTER INSERT ON public.t FROM public.u NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION f()`}},
			"trigger c added (AFTER INSERT ON public.t FROM public.u NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION f())"},
		{"trigger nil payload", Change{Type: TriggerChanged}, "trigger changed"},
		{"policy added to public", Change{Type: PolicyAdded, Policy: &PolicyChange{Name: "p", To: pol}},
			"policy p added (ALL, permissive, to public, using (tenant_id = 1))"},
		{"policy dropped", Change{Type: PolicyDropped, Policy: &PolicyChange{Name: "p", From: pol2}},
			"policy p dropped (ALL, restrictive, to admin, app, using (true), with check (true))"},
		{"policy changed", Change{Type: PolicyChanged, Policy: &PolicyChange{Name: "p", From: pol, To: pol2}},
			"policy p: permissive → restrictive; to public → admin, app; using (tenant_id = 1) → (true); with check ∅ → (true)"},
		{"policy changed, rendering identical", Change{Type: PolicyChanged, Policy: &PolicyChange{Name: "p",
			From: &snapshot.RlsPolicy{Name: "p", Command: "ALL", Permissive: true},
			To:   &snapshot.RlsPolicy{Name: "p", Command: "ALL", Permissive: true}}},
			"policy p changed"},
		{"policy nil payload", Change{Type: PolicyDropped}, "policy dropped"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Describe(tc.c); got != tc.want {
				t.Errorf("\nwant %s\ngot  %s", tc.want, got)
			}
		})
	}
}

// Category drives Marker, drift direction and every added/removed/modified counter,
// so the six new types are pinned explicitly.
func TestCategory_TriggerPolicy(t *testing.T) {
	cases := map[ChangeType]string{
		TriggerAdded: "added", TriggerDropped: "removed", TriggerChanged: "modified",
		PolicyAdded: "added", PolicyDropped: "removed", PolicyChanged: "modified",
	}
	for ct, want := range cases {
		if got := ct.Category(); got != want {
			t.Errorf("%s: want %s, got %s", ct, want, got)
		}
	}
}

// Same determinism contract as TestDiffSchema_Deterministic, one level down: several
// triggers and policies added to one table come out of map iteration, so childName
// must order them or the serialized delta (which the cloud dedups on) would vary.
func TestDiffSchema_TriggerPolicyOrder(t *testing.T) {
	var trigs []snapshot.Trigger
	var pols []snapshot.RlsPolicy
	for _, n := range []string{"z", "a", "m"} {
		trigs = append(trigs, snapshot.Trigger{Name: n, Definition: "CREATE TRIGGER " + n + " AFTER INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION f()"})
		pols = append(pols, snapshot.RlsPolicy{Name: n, Command: "ALL", Permissive: true})
	}
	from := trigSnap("a", "public", "t")
	to := trigSnap("b", "public", "t", trigs...)
	to.Tables[0].Policies = pols

	var first []byte
	for range 5 {
		d, _ := DiffSchema(from, to)
		j, _ := json.Marshal(d)
		if first == nil {
			first = j
		} else if !bytes.Equal(first, j) {
			t.Fatalf("DiffSchema not deterministic:\n%s\n%s", first, j)
		}
	}
	d, _ := DiffSchema(from, to)
	var got []string
	for _, c := range d.Changes {
		got = append(got, string(c.Type)+":"+childName(c))
	}
	want := "policy_added:a,policy_added:m,policy_added:z,trigger_added:a,trigger_added:m,trigger_added:z"
	if strings.Join(got, ",") != want {
		t.Errorf("want %s\ngot  %s", want, strings.Join(got, ","))
	}
}

// Drift takes its direction from the typed delta, so before this change a saved vs
// live pair differing only in triggers or policies printed "No drift detected".
func TestDrift_TriggersPolicies(t *testing.T) {
	trig := snapshot.Trigger{Name: "a", Definition: "CREATE TRIGGER a AFTER INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION f()"}
	r := ClassifyDrift(trigSnap("old", "public", "t"), trigSnap("new", "public", "t", trig))
	if r.Direction != DriftAhead || r.AddedCount != 1 {
		t.Errorf("trigger add: want ahead/1, got %s/%d", r.Direction, r.AddedCount)
	}
	p := snapshot.RlsPolicy{Name: "p", Command: "ALL", Permissive: true}
	p2 := p
	p2.UsingExpr = strp("true")
	r = ClassifyDrift(polSnap("old", "public", "t", p), polSnap("new", "public", "t", p2))
	if r.Direction != DriftDiverged || r.ModifiedCount != 1 {
		t.Errorf("policy change: want diverged/1 modified, got %s/%d", r.Direction, r.ModifiedCount)
	}
}

// pg_get_triggerdef does not show whether a trigger fires, so the enabled state is
// its own field and its own change type. Disabling an audit trigger is the case
// that matters; replica/always modes (logical replication setups) are diffed the
// same way. A definition edit and a state change in one version are two changes.
func TestDiffSchema_TriggerEnabled(t *testing.T) {
	const def = `CREATE TRIGGER audit_t AFTER INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION audit_fn()`
	const defB = `CREATE TRIGGER audit_t BEFORE INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION audit_fn()`
	trig := func(d, enabled string) snapshot.Trigger {
		return snapshot.Trigger{Name: "audit_t", Definition: d, Enabled: enabled}
	}
	cases := []struct {
		name     string
		from, to snapshot.Trigger
		want     []string
	}{
		{"origin to disabled", trig(def, ""), trig(def, "disabled"), []string{"trigger_enabled_changed"}},
		{"disabled to always", trig(def, "disabled"), trig(def, "always"), []string{"trigger_enabled_changed"}},
		{"unchanged state", trig(def, "replica"), trig(def, "replica"), nil},
		{"definition and state", trig(defB, ""), trig(def, "disabled"), []string{"trigger_changed", "trigger_enabled_changed"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			delta, _ := DiffSchema(trigSnap("a", "public", "t", tc.from), trigSnap("b", "public", "t", tc.to))
			if got := changeTypes(delta); !slices.Equal(got, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, got)
			}
		})
	}

	delta, _ := DiffSchema(trigSnap("a", "public", "t", trig(def, "")), trigSnap("b", "public", "t", trig(def, "disabled")))
	c := findChange(t, delta.Changes, TriggerEnabledChanged)
	if c.Trigger == nil || c.Trigger.FromEnabled != "" || c.Trigger.ToEnabled != "disabled" {
		t.Errorf("payload: %+v", c.Trigger)
	}
	// an added trigger that is already disabled carries its state on the added change
	delta, _ = DiffSchema(trigSnap("a", "public", "t"), trigSnap("b", "public", "t", trig(def, "disabled")))
	if c := findChange(t, delta.Changes, TriggerAdded); c.Trigger.ToEnabled != "disabled" {
		t.Errorf("added payload: %+v", c.Trigger)
	}
}

// FORCE ROW LEVEL SECURITY decides whether the table owner bypasses its own
// policies, so turning it on or off is a separate change from enabling RLS.
func TestDiffSchema_RLSForceToggled(t *testing.T) {
	mk := func(hash string, enabled, forced bool) *snapshot.SchemaSnapshot {
		s := emptySnap(hash)
		s.Tables = []snapshot.Table{{Schema: "public", Name: "t", RLSEnabled: enabled, RLSForced: forced}}
		return s
	}
	delta, _ := DiffSchema(mk("a", true, false), mk("b", true, true))
	if got := changeTypes(delta); !slices.Equal(got, []string{"rls_force_toggled"}) {
		t.Fatalf("force on: got %v", got)
	}
	if c := findChange(t, delta.Changes, RLSForceToggled); c.RLS == nil || !c.RLS.Forced || !c.RLS.Enabled {
		t.Errorf("payload: %+v", c.RLS)
	}
	delta, _ = DiffSchema(mk("a", true, true), mk("b", false, false))
	if got := changeTypes(delta); !slices.Equal(got, []string{"rls_force_toggled", "rls_toggled"}) {
		t.Fatalf("both off: got %v", got)
	}
}

// Wording for the new state changes as snapshot list / diff / drift print them.
func TestDescribe_TriggerEnabledAndForce(t *testing.T) {
	def := `CREATE TRIGGER audit_t AFTER INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION audit_fn()`
	cases := []struct {
		name string
		c    Change
		want string
	}{
		{"disabled", Change{Type: TriggerEnabledChanged, Trigger: &TriggerChange{Name: "audit_t", ToEnabled: "disabled"}},
			"trigger audit_t: enabled → disabled"},
		{"re-enabled always", Change{Type: TriggerEnabledChanged, Trigger: &TriggerChange{Name: "audit_t", FromEnabled: "disabled", ToEnabled: "always"}},
			"trigger audit_t: disabled → always"},
		{"added disabled", Change{Type: TriggerAdded, Trigger: &TriggerChange{Name: "audit_t", ToDefinition: def, ToEnabled: "disabled"}},
			"trigger audit_t added (disabled; AFTER INSERT ON public.t FOR EACH ROW EXECUTE FUNCTION audit_fn())"},
		{"forced", Change{Type: RLSForceToggled, RLS: &RLSChange{Enabled: true, Forced: true}}, "RLS forced"},
		{"forced without enable", Change{Type: RLSForceToggled, RLS: &RLSChange{Forced: true}}, "RLS forced (no effect: RLS not enabled)"},
		{"unforced", Change{Type: RLSForceToggled, RLS: &RLSChange{Enabled: true}}, "RLS no longer forced"},
		{"enabled nil payload", Change{Type: TriggerEnabledChanged}, "trigger enabled changed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Describe(tc.c); got != tc.want {
				t.Errorf("\nwant %s\ngot  %s", tc.want, got)
			}
		})
	}
	if got := RLSForceToggled.Category(); got != "modified" {
		t.Errorf("rls_force_toggled category: %s", got)
	}
	if got := TriggerEnabledChanged.Category(); got != "modified" {
		t.Errorf("trigger_enabled_changed category: %s", got)
	}
}
