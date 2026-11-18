package datamask

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/boringsql/fixturize/masking"

	"github.com/boringsql/dryrun/internal/schema"
)

func strptr(s string) *string { return &s }

// writeMasks drops body into a temp data-masking-policy.yml and returns its
// path. Tests then feed it to masking.Load — round-tripping through the real
// loader catches regressions in the loader too.
func writeMasks(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data-masking-policy.yml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write masks file: %v", err)
	}
	return path
}

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

// extEntry builds a fully populated entry on public.<table>. The statistics
// object lives in another schema, so every test also proves that sensitivity
// follows the table's identity, not the statistics object's.
func extEntry(table string, cols ...string) schema.ExtendedStatsEntry {
	return schema.ExtendedStatsEntry{
		Table:        schema.QualifiedName{Schema: "public", Name: table},
		Statistics:   schema.QualifiedName{Schema: "stats", Name: table + "_stats"},
		Columns:      cols,
		NDistinct:    strptr(`[{"attributes":[1,2],"ndistinct":42}]`),
		Dependencies: strptr(`[{"attributes":[1],"dependency":2,"degree":0.5}]`),
		MCVValues:    strptr(`{{"sample-a","sample-b"}}`),
		MCVFreqs:     strptr(`{0.6}`),
		MCVBaseFreqs: strptr(`{0.3}`),
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
	path := writeMasks(t, `version: 1
databases:
  dev:
    columns:
      users.email: { expr: "x", tags: [pii] }
`)
	p, err := masking.Load(path, "dev", nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	snap := &schema.PlannerStatsSnapshot{
		Database: "dev",
		Columns: []schema.ColumnStatsEntry{
			colEntry("public", "users", "email"),
			colEntry("public", "users", "id"),
		},
	}

	MaskPlanner(p, snap)
	if got := snap.Masking.ColumnsMasked; got != 1 {
		t.Errorf("matched count: got %d, want 1", got)
	}
	assertMasked(t, statsOf(t, snap, "users", "email"), "users.email")
	assertPreserved(t, statsOf(t, snap, "users", "id"), "users.id")
}

// One sensitive column drops the whole multi-column MCV list; n_distinct and
// dependencies carry no literal values and stay. Each case starts from a stale
// content hash so the rehash is checked too: masked cases must be rehashed,
// untouched ones must not.
func TestMaskPlannerExtendedStats(t *testing.T) {
	path := writeMasks(t, `version: 1
databases:
  dev:
    columns:
      users.email: { expr: "x", tags: [pii] }
`)
	p, err := masking.Load(path, "dev", nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	cases := []struct {
		name   string
		entry  schema.ExtendedStatsEntry
		masked int
	}{
		{"sensitive column", extEntry("users", "email", "country"), 1},
		{"non-sensitive columns", extEntry("users", "id", "country"), 0},
		{"no resolvable columns", extEntry("users"), 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap := &schema.PlannerStatsSnapshot{
				ContentHash:   "stale",
				ExtendedStats: []schema.ExtendedStatsEntry{tc.entry},
			}
			MaskPlanner(p, snap)
			e := snap.ExtendedStats[0]
			if got := snap.Masking.ExtendedStatsMasked; got != tc.masked {
				t.Errorf("extended count = %d, want %d", got, tc.masked)
			}
			if hasMCV := e.MCVValues != nil || e.MCVFreqs != nil || e.MCVBaseFreqs != nil; hasMCV != (tc.masked == 0) {
				t.Errorf("MCV present = %t after masking %d object(s)", hasMCV, tc.masked)
			}
			if e.NDistinct == nil || e.Dependencies == nil {
				t.Error("n_distinct/dependencies must survive masking")
			}
			// rehashed exactly when something was masked
			if rehashed := snap.ContentHash == schema.ComputePlannerContentHash(snap); rehashed != (tc.masked > 0) {
				t.Errorf("rehashed = %t, want %t", rehashed, tc.masked > 0)
			}
		})
	}
}

// TestApplyPlannerIdempotent confirms MaskPlanner can be run twice with no
// further change to the payload. A snapshot may pass through masking more than
// once, and the second pass must be a no-op. We compare the JSON encoding of
// the snapshot before and after the second run; PlannerStatsSnapshot carries
// json tags, so this is a faithful structural diff of the persisted form.
func TestApplyPlannerIdempotent(t *testing.T) {
	path := writeMasks(t, `version: 1
databases:
  dev:
    columns:
      users.email: { expr: "x", tags: [pii] }
`)
	p, err := masking.Load(path, "dev", nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	snap := &schema.PlannerStatsSnapshot{
		Columns: []schema.ColumnStatsEntry{
			colEntry("public", "users", "email"),
			colEntry("public", "users", "id"),
		},
		ExtendedStats: []schema.ExtendedStatsEntry{
			extEntry("users", "email", "country"),
		},
	}

	MaskPlanner(p, snap) // first pass: does the actual masking
	before, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal before: %v", err)
	}

	MaskPlanner(p, snap) // second pass: must change nothing
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

	polA, err := masking.Load(path, "db_a", nil)
	if err != nil {
		t.Fatalf("Load(db_a): %v", err)
	}
	polB, err := masking.Load(path, "db_b", nil)
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
	MaskPlanner(polA, snapA)
	if got := snapA.Masking.ColumnsMasked; got != 1 {
		t.Errorf("db_a column count: got %d, want 1", got)
	}
	assertMasked(t, statsOf(t, snapA, "accounts", "ssn"), "db_a accounts.ssn")
	assertPreserved(t, statsOf(t, snapA, "leads", "email"), "db_a leads.email (foreign block)")

	snapB := newSnap()
	MaskPlanner(polB, snapB)
	if got := snapB.Masking.ColumnsMasked; got != 1 {
		t.Errorf("db_b column count: got %d, want 1", got)
	}
	assertMasked(t, statsOf(t, snapB, "leads", "email"), "db_b leads.email")
	assertPreserved(t, statsOf(t, snapB, "accounts", "ssn"), "db_b accounts.ssn (foreign block)")
}

// TestApplyPlannerNilInputs guards the two no-op entry conditions. A nil
// Policy means masking is disabled (no masks file resolved); a nil snapshot
// should never happen but must not panic. Neither may touch any stats.
func TestApplyPlannerNilInputs(t *testing.T) {
	snap := &schema.PlannerStatsSnapshot{
		Columns: []schema.ColumnStatsEntry{colEntry("public", "users", "email")},
	}
	MaskPlanner(nil, snap)
	if snap.Masking == nil || *snap.Masking != (schema.MaskingInfo{JSONBStripped: true}) {
		t.Errorf("nil Policy: unexpected masking info %+v", snap.Masking)
	}
	assertPreserved(t, statsOf(t, snap, "users", "email"), "nil-policy passthrough")

	path := writeMasks(t, `version: 1
databases:
  dev:
    columns: {}
`)
	p, err := masking.Load(path, "dev", nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	MaskPlanner(p, nil)
}
