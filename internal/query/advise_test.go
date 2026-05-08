package query

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

func testSnapshot() *schema.SchemaSnapshot {
	return &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0", Database: "test",
		Timestamp: time.Now().UTC(), ContentHash: "test",
	}
}

func bloatedTable() schema.Table {
	// Build a table with one column and one bloated btree index
	expected := int64(math.Ceil(100000.0 / (float64(8192) * 0.9 / float64(8+4))))
	return schema.Table{
		Schema: "public", Name: "orders",
		Columns: []schema.Column{{Name: "user_id", TypeName: "integer"}},
		Indexes: []schema.Index{{
			Name: "idx_orders_user_id", Columns: []string{"user_id"}, IndexType: "btree",
			Stats: &schema.IndexStats{Relpages: expected * 10, Reltuples: 100000},
		}},
	}
}

func healthyTable() schema.Table {
	expected := int64(math.Ceil(100000.0 / (float64(8192) * 0.9 / float64(8+4))))
	return schema.Table{
		Schema: "public", Name: "orders",
		Columns: []schema.Column{{Name: "user_id", TypeName: "integer"}},
		Indexes: []schema.Index{{
			Name: "idx_orders_user_id", Columns: []string{"user_id"}, IndexType: "btree",
			Stats: &schema.IndexStats{Relpages: expected, Reltuples: 100000},
		}},
	}
}

func TestAdviseSeqScan_BloatedIndex(t *testing.T) {
	snap := testSnapshot()
	snap.Tables = []schema.Table{bloatedTable()}

	filter := "(user_id = 42)"
	node := &PlanNode{
		NodeType:     "Seq Scan",
		RelationName: strp("orders"),
		Schema:       strp("public"),
		PlanRows:     50000,
		Filter:       &filter,
	}

	advice := Advise(node, snap, nil)

	var found bool
	for _, a := range advice {
		if strings.Contains(a.Issue, "bloated") {
			found = true
			if a.DDL == nil || !strings.Contains(*a.DDL, "REINDEX") {
				t.Error("expected REINDEX DDL for bloated index")
			}
			if a.Severity != "warning" {
				t.Errorf("expected warning severity, got %s", a.Severity)
			}
		}
	}
	if !found {
		t.Error("expected advice about bloated index on seq scan")
	}
}

func TestAdviseSeqScan_HealthyIndex(t *testing.T) {
	snap := testSnapshot()
	snap.Tables = []schema.Table{healthyTable()}

	filter := "(user_id = 42)"
	node := &PlanNode{
		NodeType:     "Seq Scan",
		RelationName: strp("orders"),
		Schema:       strp("public"),
		PlanRows:     50000,
		Filter:       &filter,
	}

	advice := Advise(node, snap, nil)

	for _, a := range advice {
		if strings.Contains(a.Issue, "bloated") {
			t.Error("should not report bloat for healthy index")
		}
		// Should suggest ANALYZE instead
		if strings.Contains(a.Issue, "despite existing index") {
			if a.DDL == nil || !strings.Contains(*a.DDL, "ANALYZE") {
				t.Error("expected ANALYZE DDL for healthy existing index")
			}
		}
	}
}

func TestAdviseIndexScanBloat(t *testing.T) {
	snap := testSnapshot()
	snap.Tables = []schema.Table{bloatedTable()}

	node := &PlanNode{
		NodeType:     "Index Scan",
		RelationName: strp("orders"),
		Schema:       strp("public"),
		IndexName:    strp("idx_orders_user_id"),
		PlanRows:     1000,
	}

	advice := Advise(node, snap, nil)

	var found bool
	for _, a := range advice {
		if strings.Contains(a.Issue, "bloated") && strings.Contains(a.Issue, "idx_orders_user_id") {
			found = true
			if a.Severity != "info" {
				t.Errorf("expected info severity, got %s", a.Severity)
			}
			if a.DDL == nil || !strings.Contains(*a.DDL, "REINDEX") {
				t.Error("expected REINDEX DDL")
			}
		}
	}
	if !found {
		t.Error("expected bloat advice for index scan")
	}
}

func TestAdviseIndexScanBloat_NoBloat(t *testing.T) {
	snap := testSnapshot()
	snap.Tables = []schema.Table{healthyTable()}

	node := &PlanNode{
		NodeType:     "Index Scan",
		RelationName: strp("orders"),
		Schema:       strp("public"),
		IndexName:    strp("idx_orders_user_id"),
		PlanRows:     1000,
	}

	advice := Advise(node, snap, nil)

	for _, a := range advice {
		if strings.Contains(a.Issue, "bloated") {
			t.Error("should not report bloat for healthy index")
		}
	}
}

func TestAdviseIndexOnlyScanBloat(t *testing.T) {
	snap := testSnapshot()
	snap.Tables = []schema.Table{bloatedTable()}

	node := &PlanNode{
		NodeType:     "Index Only Scan",
		RelationName: strp("orders"),
		Schema:       strp("public"),
		IndexName:    strp("idx_orders_user_id"),
		PlanRows:     1000,
	}

	advice := Advise(node, snap, nil)

	var found bool
	for _, a := range advice {
		if strings.Contains(a.Issue, "bloated") {
			found = true
		}
	}
	if !found {
		t.Error("expected bloat advice for index only scan")
	}
}

func TestPerNodeBreakdown(t *testing.T) {
	snap := testSnapshot()
	snap.NodeStats = []schema.NodeStats{
		{
			Source:    "node1",
			Timestamp: time.Now().UTC(),
			TableStats: []schema.NodeTableStats{{
				Schema: "public", Table: "orders",
				Stats: schema.TableStats{SeqScan: 100, IdxScan: 500},
			}},
		},
		{
			Source:    "node2",
			Timestamp: time.Now().UTC(),
			TableStats: []schema.NodeTableStats{{
				Schema: "public", Table: "orders",
				Stats: schema.TableStats{SeqScan: 200, IdxScan: 300},
			}},
		},
	}

	result := perNodeBreakdown(snap, "public.orders")
	if !strings.Contains(result, "node1") || !strings.Contains(result, "node2") {
		t.Errorf("expected both nodes in breakdown, got: %s", result)
	}
	if !strings.Contains(result, "seq_scan=100") || !strings.Contains(result, "seq_scan=200") {
		t.Errorf("expected seq_scan values in breakdown, got: %s", result)
	}
}

func TestPerNodeBreakdown_NoNodes(t *testing.T) {
	snap := testSnapshot()
	result := perNodeBreakdown(snap, "public.orders")
	if result != "" {
		t.Errorf("expected empty string, got: %s", result)
	}
}

func TestPerNodeBreakdown_InvalidQualified(t *testing.T) {
	snap := testSnapshot()
	result := perNodeBreakdown(snap, "no_dot")
	if result != "" {
		t.Errorf("expected empty string for invalid qualified name, got: %s", result)
	}
}
