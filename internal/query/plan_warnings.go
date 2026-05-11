package query

import (
	"fmt"

	"github.com/boringsql/dryrun/internal/jit"
	"github.com/boringsql/dryrun/internal/schema"
)

const seqScanRowThreshold = 5_000.0

func detectPlanWarnings(plan *PlanNode, snap *schema.SchemaSnapshot) []PlanWarning {
	var warnings []PlanWarning
	walkPlanWarnings(plan, snap, &warnings)
	return warnings
}

func walkPlanWarnings(node *PlanNode, snap *schema.SchemaSnapshot, warnings *[]PlanWarning) {
	detectSeqScanLargeTable(node, snap, warnings)
	detectNestedLoopSeqScan(node, warnings)
	detectSortWithoutIndex(node, warnings)
	detectHighRowsRemoved(node, warnings)
	detectPartitionPruningIssues(node, snap, warnings)
	detectCTEMaterialized(node, snap, warnings)

	for i := range node.Children {
		walkPlanWarnings(&node.Children[i], snap, warnings)
	}
}

func detectSeqScanLargeTable(node *PlanNode, snap *schema.SchemaSnapshot, warnings *[]PlanWarning) {
	if node.NodeType != "Seq Scan" || node.RelationName == nil {
		return
	}
	tableName := *node.RelationName

	// fallback row count from AnnotatedSchema.SizingFor moved to caller; trust the plan estimate
	_ = snap
	rowCount := node.PlanRows

	if rowCount >= seqScanRowThreshold {
		*warnings = append(*warnings, PlanWarning{
			Severity: "warning",
			Message:  fmt.Sprintf("sequential scan on '%s' (~%d rows) - consider adding an index", tableName, int64(rowCount)),
			NodeType: "Seq Scan",
			Detail:   node.Filter,
		})
	}
}

func detectNestedLoopSeqScan(node *PlanNode, warnings *[]PlanWarning) {
	if node.NodeType != "Nested Loop" || len(node.Children) < 2 {
		return
	}
	inner := &node.Children[1]
	if inner.NodeType == "Seq Scan" && inner.PlanRows > 100 {
		tableName := "unknown"
		if inner.RelationName != nil {
			tableName = *inner.RelationName
		}
		*warnings = append(*warnings, PlanWarning{
			Severity: "warning",
			Message:  fmt.Sprintf("nested loop with sequential scan on inner side '%s' (~%d rows) - this executes once per outer row", tableName, int64(inner.PlanRows)),
			NodeType: "Nested Loop",
		})
	}
}

func detectSortWithoutIndex(node *PlanNode, warnings *[]PlanWarning) {
	if node.NodeType != "Sort" || node.PlanRows <= 10_000 {
		return
	}
	sortKeys := ""
	if len(node.SortKey) > 0 {
		for i, k := range node.SortKey {
			if i > 0 {
				sortKeys += ", "
			}
			sortKeys += k
		}
	}
	*warnings = append(*warnings, PlanWarning{
		Severity: "info",
		Message:  fmt.Sprintf("sort on ~%d rows (keys: %s) - consider an index to avoid the sort", int64(node.PlanRows), sortKeys),
		NodeType: "Sort",
	})
}

func detectPartitionPruningIssues(node *PlanNode, snap *schema.SchemaSnapshot, warnings *[]PlanWarning) {
	if snap == nil {
		return
	}
	if node.NodeType != "Append" && node.NodeType != "Merge Append" {
		return
	}

	var (
		parent  *schema.Table
		scanned int
	)

	for i := range node.Children {
		child := &node.Children[i]
		if child.RelationName == nil {
			continue
		}
		p, _ := findPartitionParent(*child.RelationName, snap)
		if p == nil {
			continue
		}
		if parent == nil {
			parent = p
		}
		scanned++
	}

	if parent == nil {
		return
	}

	total := len(parent.PartitionInfo.Children)
	var pruned int64
	if node.SubplansRemoved != nil {
		pruned = *node.SubplansRemoved
	}

	qualified := parent.Schema + "." + parent.Name
	key := parent.PartitionInfo.Key

	if pruned == 0 {
		e := jit.NoPartitionPruning(qualified, key, scanned, total)
		*warnings = append(*warnings, PlanWarning{
			Severity: "warning",
			Message:  e.String(),
			NodeType: node.NodeType,
		})
	} else if scanned > total/2 {
		*warnings = append(*warnings, PlanWarning{
			Severity: "info",
			Message:  fmt.Sprintf("partial pruning on '%s': %d partitions pruned, %d still scanned", qualified, pruned, scanned),
			NodeType: node.NodeType,
		})
	}
}

func detectCTEMaterialized(node *PlanNode, snap *schema.SchemaSnapshot, warnings *[]PlanWarning) {
	if node.NodeType != "CTE Scan" || node.CTEName == nil {
		return
	}
	cteName := *node.CTEName
	rows := int64(node.PlanRows)
	if rows < 1000 {
		return
	}

	e := jit.CTEMaterialized(cteName, rows)

	// if CTE scans partitioned table (Append with many children below), upgrade message
	for i := range node.Children {
		child := &node.Children[i]
		if (child.NodeType == "Append" || child.NodeType == "Merge Append") && snap != nil {
			for j := range child.Children {
				if child.Children[j].RelationName != nil {
					if p, _ := findPartitionParent(*child.Children[j].RelationName, snap); p != nil {
						qualified := p.Schema + "." + p.Name
						e = jit.CTEOverPartitionedTable(cteName, qualified)
						break
					}
				}
			}
		}
	}

	*warnings = append(*warnings, PlanWarning{
		Severity: "warning",
		Message:  e.String(),
		NodeType: "CTE Scan",
	})
}

func findPartitionParent(childTableName string, snap *schema.SchemaSnapshot) (*schema.Table, string) {
	for i := range snap.Tables {
		t := &snap.Tables[i]
		if t.PartitionInfo == nil {
			continue
		}
		for _, child := range t.PartitionInfo.Children {
			if child.Name == childTableName {
				return t, t.PartitionInfo.Key
			}
		}
	}
	return nil, ""
}

func detectHighRowsRemoved(node *PlanNode, warnings *[]PlanWarning) {
	if node.RowsRemovedByFilter == nil || node.ActualRows == nil {
		return
	}
	removed := *node.RowsRemovedByFilter
	actual := *node.ActualRows
	if removed > 0 && actual > 0 && removed/(removed+actual) > 0.9 {
		*warnings = append(*warnings, PlanWarning{
			Severity: "warning",
			Message:  fmt.Sprintf("'%s' filter removed %.0f rows, kept %.0f - index on the filter column would help", node.NodeType, removed, actual),
			NodeType: node.NodeType,
			Detail:   node.Filter,
		})
	}
}
