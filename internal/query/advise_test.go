package query

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

// Builds an AnnotatedSchema with one table whose index is either healthy
// (relpages ≈ expected) or bloated (relpages ≈ 10x expected) for advise.go
// to chew on. The constants mirror EstimateIndexBloat's assumptions.
func annotatedFixture(name, idx string, bloated bool) *schema.AnnotatedSchema {
	expected := int64(math.Ceil(100000.0 / (float64(8192) * 0.9 / float64(8+4))))
	relpages := expected
	if bloated {
		relpages = expected * 10
	}
	t := schema.Table{
		Schema: "public", Name: name,
		Columns: []schema.Column{{Name: "user_id", TypeName: "integer"}},
		Indexes: []schema.Index{{
			Name: idx, Columns: []string{"user_id"}, IndexType: "btree",
		}},
	}
	return &schema.AnnotatedSchema{
		Schema: &schema.SchemaSnapshot{
			PgVersion: "PostgreSQL 17.0", Database: "test",
			Timestamp: time.Now().UTC(), ContentHash: "test",
			Tables: []schema.Table{t},
		},
		Planner: &schema.PlannerStatsSnapshot{Indexes: []schema.IndexSizingEntry{{
			Table: t.Qual(), Index: idx,
			Sizing: schema.IndexSizing{Relpages: relpages, Reltuples: 100000, Size: relpages * 8192},
		}}},
	}
}

// A seq scan over a table whose available index is bloated should advise
// REINDEX rather than ANALYZE — bloat distorts the planner's cost model.
func TestAdviseSeqScan_BloatedIndex(t *testing.T) {
	a := annotatedFixture("orders", "idx_orders_user_id", true)
	filter := "(user_id = 42)"
	node := &PlanNode{NodeType: "Seq Scan", RelationName: strp("orders"), Schema: strp("public"), PlanRows: 50000, Filter: &filter}

	advice := Advise(node, a)
	var found bool
	for _, ad := range advice {
		if strings.Contains(ad.Issue, "bloated") {
			found = true
			if ad.DDL == nil || !strings.Contains(*ad.DDL, "REINDEX") {
				t.Error("expected REINDEX DDL for bloated index")
			}
			if ad.Severity != "warning" {
				t.Errorf("expected warning severity, got %s", ad.Severity)
			}
		}
	}
	if !found {
		t.Error("expected advice about bloated index on seq scan")
	}
}

// A seq scan over a table with a healthy matching index suggests ANALYZE —
// the planner just needs fresher stats, the index itself is fine.
func TestAdviseSeqScan_HealthyIndex(t *testing.T) {
	a := annotatedFixture("orders", "idx_orders_user_id", false)
	filter := "(user_id = 42)"
	node := &PlanNode{NodeType: "Seq Scan", RelationName: strp("orders"), Schema: strp("public"), PlanRows: 50000, Filter: &filter}

	advice := Advise(node, a)
	for _, ad := range advice {
		if strings.Contains(ad.Issue, "bloated") {
			t.Error("should not report bloat for healthy index")
		}
		if strings.Contains(ad.Issue, "despite existing index") {
			if ad.DDL == nil || !strings.Contains(*ad.DDL, "ANALYZE") {
				t.Error("expected ANALYZE DDL for healthy existing index")
			}
		}
	}
}

// Even when the plan picks the index, bloat still inflates cost estimates;
// an Index Scan node on a bloated index should emit an info-level REINDEX hint.
func TestAdviseIndexScanBloat(t *testing.T) {
	a := annotatedFixture("orders", "idx_orders_user_id", true)
	node := &PlanNode{
		NodeType: "Index Scan", RelationName: strp("orders"), Schema: strp("public"),
		IndexName: strp("idx_orders_user_id"), PlanRows: 1000,
	}
	advice := Advise(node, a)
	var found bool
	for _, ad := range advice {
		if strings.Contains(ad.Issue, "bloated") && strings.Contains(ad.Issue, "idx_orders_user_id") {
			found = true
			if ad.Severity != "info" {
				t.Errorf("expected info severity, got %s", ad.Severity)
			}
			if ad.DDL == nil || !strings.Contains(*ad.DDL, "REINDEX") {
				t.Error("expected REINDEX DDL")
			}
		}
	}
	if !found {
		t.Error("expected bloat advice for index scan")
	}
}

// A healthy index used for the actual scan must not trigger a bloat advice;
// false positives erode operator trust.
func TestAdviseIndexScanBloat_NoBloat(t *testing.T) {
	a := annotatedFixture("orders", "idx_orders_user_id", false)
	node := &PlanNode{
		NodeType: "Index Scan", RelationName: strp("orders"), Schema: strp("public"),
		IndexName: strp("idx_orders_user_id"), PlanRows: 1000,
	}
	for _, ad := range Advise(node, a) {
		if strings.Contains(ad.Issue, "bloated") {
			t.Error("should not report bloat for healthy index")
		}
	}
}

// Index Only Scan nodes flow through the same bloat detection path as
// Index Scan — verifying both branches keeps regressions out.
func TestAdviseIndexOnlyScanBloat(t *testing.T) {
	a := annotatedFixture("orders", "idx_orders_user_id", true)
	node := &PlanNode{
		NodeType: "Index Only Scan", RelationName: strp("orders"), Schema: strp("public"),
		IndexName: strp("idx_orders_user_id"), PlanRows: 1000,
	}
	var found bool
	for _, ad := range Advise(node, a) {
		if strings.Contains(ad.Issue, "bloated") {
			found = true
		}
	}
	if !found {
		t.Error("expected bloat advice for index only scan")
	}
}

// perNodeBreakdown formats per-node activity counts as a stacked report;
// in a two-node setup each node line must appear with its seq_scan value.
func TestPerNodeBreakdown(t *testing.T) {
	q := schema.QualifiedName{Schema: "public", Name: "orders"}
	a := &schema.AnnotatedSchema{
		Merged: &schema.MergedActivity{Nodes: []schema.NodeActivity{
			{Node: schema.NodeIdentity{Source: "node1"}, Tables: []schema.TableActivityEntry{{Table: q, Activity: schema.TableActivity{SeqScan: 100, IdxScan: 500}}}},
			{Node: schema.NodeIdentity{Source: "node2"}, Tables: []schema.TableActivityEntry{{Table: q, Activity: schema.TableActivity{SeqScan: 200, IdxScan: 300}}}},
		}},
	}
	result := perNodeBreakdown(a, "public.orders")
	if !strings.Contains(result, "node1") || !strings.Contains(result, "node2") {
		t.Errorf("expected both nodes in breakdown, got: %s", result)
	}
	if !strings.Contains(result, "seq_scan=100") || !strings.Contains(result, "seq_scan=200") {
		t.Errorf("expected seq_scan values in breakdown, got: %s", result)
	}
}

// With no Merged activity, perNodeBreakdown returns an empty string so the
// caller can skip the "Per-node breakdown" section gracefully.
func TestPerNodeBreakdown_NoNodes(t *testing.T) {
	a := &schema.AnnotatedSchema{}
	if got := perNodeBreakdown(a, "public.orders"); got != "" {
		t.Errorf("expected empty string, got: %s", got)
	}
}

// Names that aren't schema.table parse to empty — protects against caller bugs
// when an unqualified relation name leaks through.
func TestPerNodeBreakdown_InvalidQualified(t *testing.T) {
	a := &schema.AnnotatedSchema{Merged: &schema.MergedActivity{}}
	if got := perNodeBreakdown(a, "no_dot"); got != "" {
		t.Errorf("expected empty string for invalid qualified name, got: %s", got)
	}
}
