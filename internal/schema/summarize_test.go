package schema

import (
	"testing"
)

func qual(s, n string) QualifiedName { return QualifiedName{Schema: s, Name: n} }

// Helper: build an AnnotatedSchema with one table whose indexes have planner
// sizing and (optionally) per-node activity so DetectUnusedIndexes/DetectBloatedIndexes
// can be exercised against the v0.6 view shape.
func annotated(t Table, sizing []IndexSizingEntry, nodes []NodeActivity) *AnnotatedSchema {
	return &AnnotatedSchema{
		Schema:  &SchemaSnapshot{Tables: []Table{t}},
		Planner: &PlannerStatsSnapshot{Indexes: sizing},
		Merged:  &MergedActivity{Nodes: nodes},
	}
}

func makeTestIndex(name string, isPrimary, isUnique bool) Index {
	return Index{
		Name: name, Columns: []string{"col"}, IndexType: "btree",
		IsUnique: isUnique, IsPrimary: isPrimary,
		Definition: "CREATE INDEX " + name + " ON t (col)",
	}
}

// An index with zero scans across the only node is flagged as unused; the
// reported size comes from planner sizing, the only authoritative source.
func TestSingleNodeUnusedIndex(t *testing.T) {
	a := annotated(
		Table{Schema: "public", Name: "orders", Indexes: []Index{makeTestIndex("idx_unused", false, false)}},
		[]IndexSizingEntry{{Table: qual("public", "orders"), Index: "idx_unused", Sizing: IndexSizing{Size: 8192}}},
		[]NodeActivity{{Node: NodeIdentity{Source: "primary"}, Indexes: []IndexActivityEntry{
			{Table: qual("public", "orders"), Index: "idx_unused", Activity: IndexActivity{IdxScan: 0}},
		}}},
	)
	result := DetectUnusedIndexes(a)
	if len(result) != 1 || result[0].IndexName != "idx_unused" {
		t.Errorf("expected 1 unused index, got %d", len(result))
	}
}

// Any non-zero scan on any node disqualifies the index from the unused list.
func TestSingleNodeUsedIndexNotReported(t *testing.T) {
	a := annotated(
		Table{Schema: "public", Name: "orders", Indexes: []Index{makeTestIndex("idx_used", false, false)}},
		nil,
		[]NodeActivity{{Node: NodeIdentity{Source: "primary"}, Indexes: []IndexActivityEntry{
			{Table: qual("public", "orders"), Index: "idx_used", Activity: IndexActivity{IdxScan: 42}},
		}}},
	)
	if got := DetectUnusedIndexes(a); len(got) != 0 {
		t.Errorf("expected 0, got %d", len(got))
	}
}

// Primary keys never get flagged as unused even with zero scans — dropping
// them would break referential integrity.
func TestSingleNodePrimaryKeySkipped(t *testing.T) {
	a := annotated(
		Table{Schema: "public", Name: "orders", Indexes: []Index{makeTestIndex("orders_pkey", true, true)}},
		nil,
		[]NodeActivity{{Node: NodeIdentity{Source: "primary"}, Indexes: []IndexActivityEntry{
			{Table: qual("public", "orders"), Index: "orders_pkey", Activity: IndexActivity{IdxScan: 0}},
		}}},
	)
	if got := DetectUnusedIndexes(a); len(got) != 0 {
		t.Errorf("primary key should be skipped, got %d", len(got))
	}
}

// All nodes must report zero scans before an index is considered unused —
// this is the multi-node correctness guard against dropping a replica-hot index.
func TestMultiNodeUnusedAcrossAllNodes(t *testing.T) {
	a := annotated(
		Table{Schema: "public", Name: "orders", Indexes: []Index{makeTestIndex("idx_unused", false, false)}},
		[]IndexSizingEntry{{Table: qual("public", "orders"), Index: "idx_unused", Sizing: IndexSizing{Size: 16384}}},
		[]NodeActivity{
			{Node: NodeIdentity{Source: "primary"}, Indexes: []IndexActivityEntry{
				{Table: qual("public", "orders"), Index: "idx_unused", Activity: IndexActivity{IdxScan: 0}},
			}},
			{Node: NodeIdentity{Source: "replica"}, Indexes: []IndexActivityEntry{
				{Table: qual("public", "orders"), Index: "idx_unused", Activity: IndexActivity{IdxScan: 0}},
			}},
		},
	)
	result := DetectUnusedIndexes(a)
	if len(result) != 1 {
		t.Fatalf("expected 1, got %d", len(result))
	}
	if result[0].TotalSizeBytes != 16384 {
		t.Errorf("expected size 16384, got %d", result[0].TotalSizeBytes)
	}
}

// If even one node uses the index, the aggregated TotalIndexScans is non-zero
// and the index is not reported — preventing a misclassification of replica-only-hot indexes.
func TestMultiNodeUsedOnOneNotReported(t *testing.T) {
	a := annotated(
		Table{Schema: "public", Name: "orders", Indexes: []Index{makeTestIndex("idx_partial", false, false)}},
		nil,
		[]NodeActivity{
			{Node: NodeIdentity{Source: "primary"}, Indexes: []IndexActivityEntry{
				{Table: qual("public", "orders"), Index: "idx_partial", Activity: IndexActivity{IdxScan: 0}},
			}},
			{Node: NodeIdentity{Source: "replica"}, Indexes: []IndexActivityEntry{
				{Table: qual("public", "orders"), Index: "idx_partial", Activity: IndexActivity{IdxScan: 5}},
			}},
		},
	)
	if got := DetectUnusedIndexes(a); len(got) != 0 {
		t.Errorf("expected 0 (used on replica), got %d", len(got))
	}
}

// Empty input yields empty output; covers the early-return safety guard.
func TestEmptyInputs(t *testing.T) {
	if got := DetectUnusedIndexes(nil); len(got) != 0 {
		t.Errorf("expected 0, got %d", len(got))
	}
}

func bloatedAnnotated(idxName string, relpages int64, reltuples float64) *AnnotatedSchema {
	t := Table{
		Schema: "public", Name: "orders",
		Columns: []Column{{Name: "id", TypeName: "integer"}},
		Indexes: []Index{{Name: idxName, Columns: []string{"id"}, IndexType: "btree"}},
	}
	return &AnnotatedSchema{
		Schema: &SchemaSnapshot{Tables: []Table{t}},
		Planner: &PlannerStatsSnapshot{Indexes: []IndexSizingEntry{{
			Table: qual("public", "orders"), Index: idxName,
			Sizing: IndexSizing{Relpages: relpages, Reltuples: reltuples, Size: relpages * 8192},
		}}},
	}
}

// 100k tuples on an integer key → expected ~163 pages. 1000 actual pages means
// ratio > 2.0 → flagged. Confirms DetectBloatedIndexes wires through to EstimateIndexBloat.
func TestDetectBloatedIndexes_FlagsAboveThreshold(t *testing.T) {
	result := DetectBloatedIndexes(bloatedAnnotated("idx_orders_id", 1000, 100000), 2.0)
	if len(result) != 1 {
		t.Fatalf("expected 1 bloated index, got %d", len(result))
	}
	if result[0].IndexName != "idx_orders_id" {
		t.Errorf("expected idx_orders_id, got %s", result[0].IndexName)
	}
	if result[0].BloatRatio <= 2.0 {
		t.Errorf("expected bloat ratio > 2.0, got %.2f", result[0].BloatRatio)
	}
}

// 200 pages vs ~163 expected gives a ratio just over 1.0 — below the 2.0 threshold,
// so nothing should surface.
func TestDetectBloatedIndexes_BelowThreshold(t *testing.T) {
	if got := DetectBloatedIndexes(bloatedAnnotated("idx_orders_id", 200, 100000), 2.0); len(got) != 0 {
		t.Errorf("expected 0, got %d", len(got))
	}
}

// Non-btree index types skip bloat estimation entirely (no analytical model);
// even a clearly over-allocated GIN index won't surface.
func TestDetectBloatedIndexes_NonBtreeSkipped(t *testing.T) {
	t1 := Table{
		Schema: "public", Name: "docs",
		Columns: []Column{{Name: "body", TypeName: "tsvector"}},
		Indexes: []Index{{Name: "idx_docs_body", Columns: []string{"body"}, IndexType: "gin"}},
	}
	a := &AnnotatedSchema{
		Schema: &SchemaSnapshot{Tables: []Table{t1}},
		Planner: &PlannerStatsSnapshot{Indexes: []IndexSizingEntry{{
			Table: qual("public", "docs"), Index: "idx_docs_body",
			Sizing: IndexSizing{Relpages: 5000, Reltuples: 100000, Size: 5000 * 8192},
		}}},
	}
	if got := DetectBloatedIndexes(a, 2.0); len(got) != 0 {
		t.Errorf("expected 0 for gin index, got %d", len(got))
	}
}
