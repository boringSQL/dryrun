package datamask

import (
	"encoding/json"
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
)

func strptr(s string) *string { return &s }

// colEntry builds a ColumnStatsEntry whose MostCommonVals, MostCommonFreqs and
// HistogramBounds are all non-nil. Starting from fully-populated stats is what
// makes the assertions meaningful: a column that began life with nil stats
// would "pass" a masking check without ApplyPlanner having done anything.
func colEntry(schemaName, table, column string) schema.ColumnStatsEntry {
	return schema.ColumnStatsEntry{
		Table:  schema.QualifiedName{Schema: schemaName, Name: table},
		Column: column,
		Stats: schema.ColumnStats{
			MostCommonVals:  strptr("{sample,values}"),
			MostCommonFreqs: strptr("{0.6,0.4}"),
			HistogramBounds: strptr("{1,2,3}"),
		},
	}
}

// statsOf finds the ColumnStats for table.column inside a snapshot, failing
// the test loudly if the column is missing — a lookup typo in a test should
// not quietly masquerade as a masking result.
func statsOf(t *testing.T, snap *schema.PlannerStatsSnapshot, table, column string) schema.ColumnStats {
	t.Helper()
	for _, c := range snap.Columns {
		if c.Table.Name == table && c.Column == column {
			return c.Stats
		}
	}
	t.Fatalf("column %s.%s not present in snapshot", table, column)
	return schema.ColumnStats{}
}

// assertMasked asserts that the three planner-stats fields dryrun strips are
// all nil. These three together (MCV, freqs, histogram) are the payload that
// can leak literal column values into history.db / exported snapshots.
func assertMasked(t *testing.T, s schema.ColumnStats, label string) {
	t.Helper()
	if s.MostCommonVals != nil || s.MostCommonFreqs != nil || s.HistogramBounds != nil {
		t.Errorf("%s: expected MCV/freqs/histogram all nil, got %v / %v / %v",
			label, s.MostCommonVals, s.MostCommonFreqs, s.HistogramBounds)
	}
}

// assertPreserved asserts the opposite: a non-sensitive column must come out
// of ApplyPlanner byte-for-byte unchanged.
func assertPreserved(t *testing.T, s schema.ColumnStats, label string) {
	t.Helper()
	if s.MostCommonVals == nil || s.MostCommonFreqs == nil || s.HistogramBounds == nil {
		t.Errorf("%s: expected stats preserved, but something was nilled: %v / %v / %v",
			label, s.MostCommonVals, s.MostCommonFreqs, s.HistogramBounds)
	}
}

// TestApplyPlannerMasksOnlySensitiveColumns is the central behaviour check.
// Given a snapshot with one sensitive column (users.email) and one ordinary
// column (users.id), ApplyPlanner must clear all three stat fields on the
// former, leave the latter completely untouched, and report a matched-count
// of exactly 1. Both the "sensitive gets nilled" and "non-sensitive survives"
// cases from the plan are exercised here against a single snapshot, because
// the interesting property is precisely that the two are handled differently
// in one pass.
func TestApplyPlannerMasksOnlySensitiveColumns(t *testing.T) {
	// unqualified key: users.email is sensitive in any schema.
	p := &Policy{
		qualified:   map[string]struct{}{},
		unqualified: map[string]struct{}{"users.email": {}},
	}
	snap := &schema.PlannerStatsSnapshot{
		Database: "dev",
		Columns: []schema.ColumnStatsEntry{
			colEntry("public", "users", "email"),
			colEntry("public", "users", "id"),
		},
	}

	masked := ApplyPlanner(p, snap)
	if masked != 1 {
		t.Errorf("matched count: got %d, want 1", masked)
	}
	assertMasked(t, statsOf(t, snap, "users", "email"), "users.email")
	assertPreserved(t, statsOf(t, snap, "users", "id"), "users.id")
}

// TestApplyPlannerIdempotent confirms ApplyPlanner can be run twice with no
// further change to the payload. This matters because masking is applied at
// capture time and, defensively, again at export time — the same snapshot may
// pass through ApplyPlanner more than once and the second pass must be a
// no-op. We compare the JSON encoding of the snapshot before and after the
// second run; PlannerStatsSnapshot carries json tags, so this is a faithful
// structural diff of the persisted form.
func TestApplyPlannerIdempotent(t *testing.T) {
	p := &Policy{
		qualified:   map[string]struct{}{},
		unqualified: map[string]struct{}{"users.email": {}},
	}
	snap := &schema.PlannerStatsSnapshot{
		Columns: []schema.ColumnStatsEntry{
			colEntry("public", "users", "email"),
			colEntry("public", "users", "id"),
		},
	}

	ApplyPlanner(p, snap) // first pass: does the actual masking
	before, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal before: %v", err)
	}

	ApplyPlanner(p, snap) // second pass: must change nothing
	after, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal after: %v", err)
	}

	if string(before) != string(after) {
		t.Errorf("ApplyPlanner is not idempotent:\n before: %s\n after:  %s", before, after)
	}
}

// TestApplyPlannerMultiDatabase proves that two policies built for different
// database_ids mask independently and never bleed into each other. Both
// snapshots have the identical two-column shape (accounts.ssn + leads.email);
// the db_a policy must touch only accounts.ssn, the db_b policy only
// leads.email. This is the property the per-key snapshot-export path (C7)
// depends on when a single export spans multiple databases.
func TestApplyPlannerMultiDatabase(t *testing.T) {
	path := writeMasks(t, `version: 1
databases:
  db_a:
    columns:
      accounts.ssn: { expr: "x", tags: [pii] }
  db_b:
    columns:
      leads.email: { expr: "x", tags: [pii] }
`)

	polA, err := Load(path, "db_a", nil)
	if err != nil {
		t.Fatalf("Load(db_a): %v", err)
	}
	polB, err := Load(path, "db_b", nil)
	if err != nil {
		t.Fatalf("Load(db_b): %v", err)
	}

	newSnap := func() *schema.PlannerStatsSnapshot {
		return &schema.PlannerStatsSnapshot{
			Columns: []schema.ColumnStatsEntry{
				colEntry("public", "accounts", "ssn"),
				colEntry("public", "leads", "email"),
			},
		}
	}

	snapA := newSnap()
	if n := ApplyPlanner(polA, snapA); n != 1 {
		t.Errorf("db_a matched count: got %d, want 1", n)
	}
	assertMasked(t, statsOf(t, snapA, "accounts", "ssn"), "db_a accounts.ssn")
	assertPreserved(t, statsOf(t, snapA, "leads", "email"), "db_a leads.email (foreign block)")

	snapB := newSnap()
	if n := ApplyPlanner(polB, snapB); n != 1 {
		t.Errorf("db_b matched count: got %d, want 1", n)
	}
	assertMasked(t, statsOf(t, snapB, "leads", "email"), "db_b leads.email")
	assertPreserved(t, statsOf(t, snapB, "accounts", "ssn"), "db_b accounts.ssn (foreign block)")
}

// TestApplyPlannerNilInputs guards the two no-op entry conditions. A nil
// Policy means masking is disabled (no masks file resolved); a nil snapshot
// should never happen but must not panic. Both must return a zero count and
// touch nothing.
func TestApplyPlannerNilInputs(t *testing.T) {
	snap := &schema.PlannerStatsSnapshot{
		Columns: []schema.ColumnStatsEntry{colEntry("public", "users", "email")},
	}
	if n := ApplyPlanner(nil, snap); n != 0 {
		t.Errorf("nil Policy: got count %d, want 0", n)
	}
	assertPreserved(t, statsOf(t, snap, "users", "email"), "nil-policy passthrough")

	if n := ApplyPlanner(&Policy{}, nil); n != 0 {
		t.Errorf("nil snapshot: got count %d, want 0", n)
	}
}
