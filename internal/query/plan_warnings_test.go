package query

import (
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
)

func strPtr(s string) *string { return &s }
func int64Ptr(n int64) *int64 { return &n }

func partitionSnap() *schema.SchemaSnapshot {
	return &schema.SchemaSnapshot{
		Tables: []schema.Table{
			{
				Schema: "public",
				Name:   "events",
				PartitionInfo: &schema.PartitionInfo{
					Strategy: schema.PartitionRange,
					Key:      "created_at",
					Children: []schema.PartitionChild{
						{Schema: "public", Name: "events_2025_01"},
						{Schema: "public", Name: "events_2025_02"},
						{Schema: "public", Name: "events_2025_03"},
						{Schema: "public", Name: "events_2025_04"},
					},
				},
			},
		},
	}
}

func TestPartitionPruningNoPruning(t *testing.T) {
	snap := partitionSnap()
	plan := &PlanNode{
		NodeType: "Append",
		Children: []PlanNode{
			{NodeType: "Seq Scan", RelationName: strPtr("events_2025_01")},
			{NodeType: "Seq Scan", RelationName: strPtr("events_2025_02")},
			{NodeType: "Seq Scan", RelationName: strPtr("events_2025_03")},
			{NodeType: "Seq Scan", RelationName: strPtr("events_2025_04")},
		},
	}

	warnings := DetectPlanWarnings(plan, snap)
	found := false
	for _, w := range warnings {
		if w.Severity == "warning" && strings.Contains(w.Message, "no partition pruning") {
			found = true
		}
	}
	if !found {
		t.Error("expected 'no partition pruning' warning when all partitions scanned")
	}
}

func TestPartitionPruningGoodPruning(t *testing.T) {
	snap := partitionSnap()
	// 1 of 4 scanned, 3 pruned
	plan := &PlanNode{
		NodeType:        "Append",
		SubplansRemoved: int64Ptr(3),
		Children: []PlanNode{
			{NodeType: "Seq Scan", RelationName: strPtr("events_2025_01")},
		},
	}

	warnings := DetectPlanWarnings(plan, snap)
	for _, w := range warnings {
		if strings.Contains(w.Message, "partition pruning") || strings.Contains(w.Message, "partial pruning") {
			t.Errorf("unexpected partition warning when pruning is effective: %s", w.Message)
		}
	}
}

func TestPartitionPruningPartial(t *testing.T) {
	snap := partitionSnap()
	// 3 of 4 scanned, 1 pruned - still scanning > 50%
	plan := &PlanNode{
		NodeType:        "Append",
		SubplansRemoved: int64Ptr(1),
		Children: []PlanNode{
			{NodeType: "Seq Scan", RelationName: strPtr("events_2025_01")},
			{NodeType: "Seq Scan", RelationName: strPtr("events_2025_02")},
			{NodeType: "Seq Scan", RelationName: strPtr("events_2025_03")},
		},
	}

	warnings := DetectPlanWarnings(plan, snap)
	found := false
	for _, w := range warnings {
		if w.Severity == "info" && strings.Contains(w.Message, "partial pruning") {
			found = true
		}
	}
	if !found {
		t.Error("expected 'partial pruning' info when >50% of partitions still scanned")
	}
}

func TestPartitionPruningNonPartitionedAppend(t *testing.T) {
	snap := partitionSnap()
	// Append over non-partition tables (e.g. UNION ALL)
	plan := &PlanNode{
		NodeType: "Append",
		Children: []PlanNode{
			{NodeType: "Seq Scan", RelationName: strPtr("some_other_table")},
			{NodeType: "Seq Scan", RelationName: strPtr("another_table")},
		},
	}

	warnings := DetectPlanWarnings(plan, snap)
	for _, w := range warnings {
		if strings.Contains(w.Message, "partition") {
			t.Errorf("unexpected partition warning for non-partitioned Append: %s", w.Message)
		}
	}
}

func TestPartitionPruningMergeAppend(t *testing.T) {
	snap := partitionSnap()
	plan := &PlanNode{
		NodeType: "Merge Append",
		Children: []PlanNode{
			{NodeType: "Index Scan", RelationName: strPtr("events_2025_01")},
			{NodeType: "Index Scan", RelationName: strPtr("events_2025_02")},
			{NodeType: "Index Scan", RelationName: strPtr("events_2025_03")},
			{NodeType: "Index Scan", RelationName: strPtr("events_2025_04")},
		},
	}

	warnings := DetectPlanWarnings(plan, snap)
	found := false
	for _, w := range warnings {
		if strings.Contains(w.Message, "no partition pruning") {
			found = true
		}
	}
	if !found {
		t.Error("expected partition pruning warning for Merge Append scanning all partitions")
	}
}
