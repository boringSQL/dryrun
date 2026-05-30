package audit

import (
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
)

func TestDuplicateIndexSkipsInvalid(t *testing.T) {
	snap := testSnap()
	snap.Tables = []schema.Table{{
		Schema: "public", Name: "orders",
		Indexes: []schema.Index{
			{Name: "idx_a", Columns: []string{"user_id"}, IndexType: "btree", IsValid: true, IsReady: true},
			{Name: "idx_b", Columns: []string{"user_id"}, IndexType: "btree", IsValid: false},
		},
	}}
	findings := checkDuplicateIndexes(snap)
	if len(findings) != 0 {
		t.Errorf("invalid index should not count as duplicate, got %d findings", len(findings))
	}
}

func TestRedundantSkipsUniqueIndex(t *testing.T) {
	snap := testSnap()
	snap.Tables = []schema.Table{{
		Schema: "public", Name: "orders",
		Indexes: []schema.Index{
			{Name: "idx_unique_email", Columns: []string{"email"}, IndexType: "btree", IsUnique: true, IsValid: true, IsReady: true},
			{Name: "idx_email_created", Columns: []string{"email", "created_at"}, IndexType: "btree", IsValid: true, IsReady: true},
		},
	}}
	findings := checkRedundantIndexes(snap)
	if len(findings) != 0 {
		t.Errorf("unique index should not be flagged as redundant, got %d", len(findings))
	}
}

func TestNonUniqueRedundantWithUnique(t *testing.T) {
	snap := testSnap()
	snap.Tables = []schema.Table{{
		Schema: "public", Name: "orders",
		Indexes: []schema.Index{
			{Name: "idx_email", Columns: []string{"email"}, IndexType: "btree", IsValid: true, IsReady: true},
			{Name: "idx_email_unique", Columns: []string{"email"}, IndexType: "btree", IsUnique: true, IsValid: true, IsReady: true},
		},
	}}
	findings := checkRedundantIndexes(snap)
	if len(findings) != 1 {
		t.Fatalf("expected 1 redundant finding, got %d", len(findings))
	}
	if findings[0].Message == "" {
		t.Error("expected non-empty message")
	}
}

func TestDuplicateIndexBothValid(t *testing.T) {
	snap := testSnap()
	snap.Tables = []schema.Table{{
		Schema: "public", Name: "orders",
		Indexes: []schema.Index{
			{Name: "idx_a", Columns: []string{"user_id"}, IndexType: "btree", IsValid: true, IsReady: true},
			{Name: "idx_b", Columns: []string{"user_id"}, IndexType: "btree", IsValid: true, IsReady: true},
		},
	}}
	findings := checkDuplicateIndexes(snap)
	if len(findings) != 1 {
		t.Errorf("expected 1 duplicate, got %d", len(findings))
	}
}

// Regression guard for the is_valid/is_ready capture bug. Historically the
// snapshot serializer never populated Index.IsValid, so the field was always
// the Go zero value (false) for every single index in the database. The
// duplicate-index rule opens with `if !a.IsValid || !b.IsValid { continue }`,
// which meant the guard was *unconditionally true* and the entire rule body
// was dead code that could never emit a finding on any real database. Now
// that the introspection query actually selects indisvalid/indisready and the
// struct literal assigns them, the rule comes back to life. This test pins
// down the exact boolean matrix so a future regression in how those flags are
// populated (or in the guard condition itself) trips a red test immediately.
func TestDuplicateIndexValidityMatrix(t *testing.T) {
	// Each case is a pair of otherwise-identical btree indexes on the same
	// single column. The ONLY thing that varies between cases is the
	// valid/ready flags on the second index, so any change in the finding
	// count is attributable purely to the guard at rules.go.
	cases := []struct {
		name         string
		bValid       bool
		bReady       bool
		wantFindings int
	}{
		// Happy path: both indexes are valid AND ready, so they are genuine
		// live duplicates and the rule should flag exactly one of them.
		{name: "both valid and ready emits one finding", bValid: true, bReady: true, wantFindings: 1},
		// A structurally invalid index (think: a CREATE INDEX CONCURRENTLY
		// that aborted partway and left an indisvalid=false carcass behind)
		// must never be recommended for a drop, because the "duplicate" we'd
		// be leaning on isn't actually a usable index at all.
		{name: "invalid second index is skipped", bValid: false, bReady: true, wantFindings: 0},
		// A valid-but-not-ready index is mid-build (an in-flight concurrent
		// index that has passed validation but isn't serving queries yet).
		// Dropping its twin now would be premature, so the rule stays quiet.
		{name: "not-ready second index is skipped", bValid: true, bReady: false, wantFindings: 0},
		// Belt-and-suspenders: an index that is neither valid nor ready is the
		// most broken state of all and obviously must not produce a finding.
		{name: "neither valid nor ready is skipped", bValid: false, bReady: false, wantFindings: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap := testSnap()
			snap.Tables = []schema.Table{{
				Schema: "public", Name: "orders",
				Indexes: []schema.Index{
					// The first index is always perfectly healthy so that the
					// outcome hinges solely on the second index's flags.
					{Name: "idx_a", Columns: []string{"user_id"}, IndexType: "btree", IsValid: true, IsReady: true},
					{Name: "idx_b", Columns: []string{"user_id"}, IndexType: "btree", IsValid: tc.bValid, IsReady: tc.bReady},
				},
			}}
			findings := checkDuplicateIndexes(snap)
			if len(findings) != tc.wantFindings {
				t.Errorf("valid=%v ready=%v: expected %d findings, got %d",
					tc.bValid, tc.bReady, tc.wantFindings, len(findings))
			}
		})
	}
}
