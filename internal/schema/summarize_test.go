package schema

import (
	"testing"
	"time"
)

func makeTestIndex(name string, isPrimary, isUnique bool, stats *IndexStats) Index {
	return Index{
		Name: name, Columns: []string{"col"}, IndexType: "btree",
		IsUnique: isUnique, IsPrimary: isPrimary,
		Definition: "CREATE INDEX " + name + " ON t (col)",
		Stats:      stats,
	}
}

func makeTestTable(name string, indexes []Index) Table {
	return Table{Schema: "public", Name: name, Indexes: indexes}
}

func makeTestNodeStats(source string, indexStats []NodeIndexStats) NodeStats {
	return NodeStats{
		Source:     source,
		Timestamp:  time.Now().UTC(),
		IndexStats: indexStats,
	}
}

func idxStats(scan, size int64) IndexStats {
	return IndexStats{IdxScan: scan, Size: size}
}

func TestSingleNodeUnusedIndex(t *testing.T) {
	tables := []Table{makeTestTable("orders", []Index{
		makeTestIndex("idx_unused", false, false, &IndexStats{IdxScan: 0, Size: 8192}),
	})}
	result := DetectUnusedIndexes(nil, tables)
	if len(result) != 1 || result[0].IndexName != "idx_unused" {
		t.Errorf("expected 1 unused index, got %d", len(result))
	}
}

func TestSingleNodeUsedIndexNotReported(t *testing.T) {
	tables := []Table{makeTestTable("orders", []Index{
		makeTestIndex("idx_used", false, false, &IndexStats{IdxScan: 42, Size: 8192}),
	})}
	result := DetectUnusedIndexes(nil, tables)
	if len(result) != 0 {
		t.Errorf("expected 0, got %d", len(result))
	}
}

func TestSingleNodePrimaryKeySkipped(t *testing.T) {
	tables := []Table{makeTestTable("orders", []Index{
		makeTestIndex("orders_pkey", true, true, &IndexStats{IdxScan: 0, Size: 8192}),
	})}
	result := DetectUnusedIndexes(nil, tables)
	if len(result) != 0 {
		t.Errorf("primary key should be skipped, got %d", len(result))
	}
}

func TestMultiNodeUnusedAcrossAllNodes(t *testing.T) {
	tables := []Table{makeTestTable("orders", []Index{
		makeTestIndex("idx_unused", false, false, nil),
	})}
	nodeStats := []NodeStats{
		makeTestNodeStats("node1", []NodeIndexStats{{
			Schema: "public", Table: "orders", IndexName: "idx_unused",
			Stats: idxStats(0, 8192),
		}}),
		makeTestNodeStats("node2", []NodeIndexStats{{
			Schema: "public", Table: "orders", IndexName: "idx_unused",
			Stats: idxStats(0, 16384),
		}}),
	}
	result := DetectUnusedIndexes(nodeStats, tables)
	if len(result) != 1 {
		t.Fatalf("expected 1, got %d", len(result))
	}
	if result[0].TotalSizeBytes != 16384 {
		t.Errorf("expected max size 16384, got %d", result[0].TotalSizeBytes)
	}
}

func TestMultiNodeUsedOnOneNotReported(t *testing.T) {
	tables := []Table{makeTestTable("orders", []Index{
		makeTestIndex("idx_partial", false, false, nil),
	})}
	nodeStats := []NodeStats{
		makeTestNodeStats("node1", []NodeIndexStats{{
			Schema: "public", Table: "orders", IndexName: "idx_partial",
			Stats: idxStats(0, 8192),
		}}),
		makeTestNodeStats("node2", []NodeIndexStats{{
			Schema: "public", Table: "orders", IndexName: "idx_partial",
			Stats: idxStats(5, 8192),
		}}),
	}
	result := DetectUnusedIndexes(nodeStats, tables)
	if len(result) != 0 {
		t.Errorf("expected 0 (used on node2), got %d", len(result))
	}
}

func TestSortedBySizeDesc(t *testing.T) {
	tables := []Table{makeTestTable("orders", []Index{
		makeTestIndex("idx_small", false, false, nil),
		makeTestIndex("idx_big", false, false, nil),
	})}
	nodeStats := []NodeStats{
		makeTestNodeStats("node1", []NodeIndexStats{
			{Schema: "public", Table: "orders", IndexName: "idx_small", Stats: idxStats(0, 1024)},
			{Schema: "public", Table: "orders", IndexName: "idx_big", Stats: idxStats(0, 999999)},
		}),
	}
	result := DetectUnusedIndexes(nodeStats, tables)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].IndexName != "idx_big" {
		t.Errorf("expected idx_big first, got %s", result[0].IndexName)
	}
}

func TestEmptyInputs(t *testing.T) {
	result := DetectUnusedIndexes(nil, nil)
	if len(result) != 0 {
		t.Errorf("expected 0, got %d", len(result))
	}
}

func makeBloatedTable(name string, idxName string, relpages int64, reltuples float64) Table {
	return Table{
		Schema: "public", Name: name,
		Columns: []Column{{Name: "id", TypeName: "integer"}},
		Indexes: []Index{{
			Name: idxName, Columns: []string{"id"}, IndexType: "btree",
			Stats: &IndexStats{Relpages: relpages, Reltuples: reltuples, Size: relpages * pageSize},
		}},
	}
}

func TestDetectBloatedIndexes_SingleNode(t *testing.T) {
	// 100k tuples, integer key → expected ~163 pages. Give it 1000 pages → bloated
	tables := []Table{makeBloatedTable("orders", "idx_orders_id", 1000, 100000)}
	result := DetectBloatedIndexes(nil, tables, 2.0)
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

func TestDetectBloatedIndexes_SingleNode_BelowThreshold(t *testing.T) {
	// 100k tuples, ~163 expected pages, give it 200 pages → ratio ~1.2, below 2.0
	tables := []Table{makeBloatedTable("orders", "idx_orders_id", 200, 100000)}
	result := DetectBloatedIndexes(nil, tables, 2.0)
	if len(result) != 0 {
		t.Errorf("expected 0, got %d", len(result))
	}
}

func TestDetectBloatedIndexes_NonBtreeSkipped(t *testing.T) {
	tables := []Table{{
		Schema: "public", Name: "docs",
		Columns: []Column{{Name: "body", TypeName: "tsvector"}},
		Indexes: []Index{{
			Name: "idx_docs_body", Columns: []string{"body"}, IndexType: "gin",
			Stats: &IndexStats{Relpages: 5000, Reltuples: 100000, Size: 5000 * pageSize},
		}},
	}}
	result := DetectBloatedIndexes(nil, tables, 2.0)
	if len(result) != 0 {
		t.Errorf("expected 0 for gin index, got %d", len(result))
	}
}

func TestDetectBloatedIndexes_MultiNode(t *testing.T) {
	tables := []Table{{
		Schema: "public", Name: "orders",
		Columns: []Column{{Name: "id", TypeName: "integer"}},
		Indexes: []Index{{
			Name: "idx_orders_id", Columns: []string{"id"}, IndexType: "btree",
		}},
	}}
	nodeStats := []NodeStats{
		makeTestNodeStats("node1", []NodeIndexStats{{
			Schema: "public", Table: "orders", IndexName: "idx_orders_id",
			Stats: IndexStats{Relpages: 1000, Reltuples: 100000, Size: 1000 * pageSize},
		}}),
		makeTestNodeStats("node2", []NodeIndexStats{{
			Schema: "public", Table: "orders", IndexName: "idx_orders_id",
			Stats: IndexStats{Relpages: 2000, Reltuples: 100000, Size: 2000 * pageSize},
		}}),
	}
	result := DetectBloatedIndexes(nodeStats, tables, 2.0)
	if len(result) != 1 {
		t.Fatalf("expected 1, got %d", len(result))
	}
	// Should pick the higher bloat (node2 with 2000 pages)
	if result[0].ActualPages != 2000 {
		t.Errorf("expected actual pages from worst node (2000), got %d", result[0].ActualPages)
	}
}

func TestDetectBloatedIndexes_SortedByBloatDesc(t *testing.T) {
	tables := []Table{
		makeBloatedTable("orders", "idx_low_bloat", 500, 100000),
		makeBloatedTable("users", "idx_high_bloat", 2000, 100000),
	}
	result := DetectBloatedIndexes(nil, tables, 1.5)
	if len(result) < 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].BloatRatio < result[1].BloatRatio {
		t.Errorf("expected sorted by bloat desc: %.2f < %.2f", result[0].BloatRatio, result[1].BloatRatio)
	}
}
