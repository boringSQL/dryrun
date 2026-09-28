package schema

import "testing"

// Zero scans is only evidence when activity was measured, and dropping a
// constraint's index drops the constraint.
func TestUnusedIndexesOnlyReportsMeasuredNonConstraintIndexes(t *testing.T) {
	uniq := makeTestIndex("orders_code_key", false, true)
	uniq.BacksConstraint = true
	plain := makeTestIndex("idx_a", false, false)
	tbl := Table{Schema: "public", Name: "orders", Indexes: []Index{uniq, plain}}
	zero := func(index string) []NodeActivity {
		return []NodeActivity{{Node: NodeIdentity{Source: "primary"}, Indexes: []IndexActivityEntry{
			{Table: qual("public", "orders"), Index: index, Activity: IndexActivity{IdxScan: 0}},
		}}}
	}
	tests := map[string]*AnnotatedSchema{
		"constraint-backing index": annotated(tbl, nil, zero("orders_code_key")),
		"no activity captured":     {Schema: &SchemaSnapshot{Tables: []Table{tbl}}},
		"no entry for this index":  annotated(tbl, nil, zero("some_other_index")),
	}
	for name, a := range tests {
		if got := DetectUnusedIndexes(a); len(got) != 0 {
			t.Errorf("%s reported as unused: %+v", name, got)
		}
	}
	if got := DetectUnusedIndexes(annotated(tbl, nil, zero("idx_a"))); len(got) != 1 || got[0].IndexName != "idx_a" {
		t.Errorf("a measured zero-scan plain index should be reported, got %+v", got)
	}
}
