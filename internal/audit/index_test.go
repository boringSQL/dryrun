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
			{Name: "idx_a", Columns: []string{"user_id"}, IndexType: "btree", IsValid: true},
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
			{Name: "idx_unique_email", Columns: []string{"email"}, IndexType: "btree", IsUnique: true, IsValid: true},
			{Name: "idx_email_created", Columns: []string{"email", "created_at"}, IndexType: "btree", IsValid: true},
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
			{Name: "idx_email", Columns: []string{"email"}, IndexType: "btree", IsValid: true},
			{Name: "idx_email_unique", Columns: []string{"email"}, IndexType: "btree", IsUnique: true, IsValid: true},
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
			{Name: "idx_a", Columns: []string{"user_id"}, IndexType: "btree", IsValid: true},
			{Name: "idx_b", Columns: []string{"user_id"}, IndexType: "btree", IsValid: true},
		},
	}}
	findings := checkDuplicateIndexes(snap)
	if len(findings) != 1 {
		t.Errorf("expected 1 duplicate, got %d", len(findings))
	}
}
