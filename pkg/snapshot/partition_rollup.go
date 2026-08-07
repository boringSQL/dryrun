package snapshot

import "sort"

type indexActivityKey struct {
	Table QualifiedName
	Index string
}

// RollUpPartitionActivity synthesizes activity entries for partitioned parent
// tables and indexes by summing their physical children.
// Returns new slices; inputs are not mutated in place.
func RollUpPartitionActivity(schema *SchemaSnapshot, tables []TableActivityEntry, indexes []IndexActivityEntry) ([]TableActivityEntry, []IndexActivityEntry) {
	if schema == nil {
		return tables, indexes
	}

	tables = append([]TableActivityEntry(nil), tables...)
	indexes = append([]IndexActivityEntry(nil), indexes...)

	tableAt := make(map[QualifiedName]int, len(tables))
	for i, te := range tables {
		tableAt[te.Table] = i
	}
	indexAt := make(map[indexActivityKey]int, len(indexes))
	for i, ie := range indexes {
		indexAt[indexActivityKey{ie.Table, ie.Index}] = i
	}

	tableByQual := make(map[QualifiedName]*Table, len(schema.Tables))
	for i := range schema.Tables {
		tableByQual[schema.Tables[i].Qual()] = &schema.Tables[i]
	}

	// Bottom-up: intermediate parents must be rolled up before the top-level
	// parent sums them in.
	for _, t := range partitionRollupOrder(schema, tableByQual) {
		parentQual := t.Qual()

		var sum TableActivity
		for _, child := range t.PartitionInfo.Children {
			childQual := QualifiedName{Schema: child.Schema, Name: child.Name}
			if i, ok := tableAt[childQual]; ok {
				addTableActivity(&sum, tables[i].Activity)
			}
		}
		if i, ok := tableAt[parentQual]; ok {
			addTableActivity(&tables[i].Activity, sum)
		} else {
			tables = append(tables, TableActivityEntry{Table: parentQual, Activity: sum})
			tableAt[parentQual] = len(tables) - 1
		}

		for _, idx := range t.Indexes {
			if len(idx.Children) == 0 {
				continue
			}
			var isum IndexActivity
			for _, child := range idx.Children {
				childKey := indexActivityKey{QualifiedName{Schema: child.Schema, Name: child.Table}, child.Index}
				if i, ok := indexAt[childKey]; ok {
					addIndexActivity(&isum, indexes[i].Activity)
				}
			}
			key := indexActivityKey{parentQual, idx.Name}
			if i, ok := indexAt[key]; ok {
				addIndexActivity(&indexes[i].Activity, isum)
			} else {
				indexes = append(indexes, IndexActivityEntry{Table: parentQual, Index: idx.Name, Activity: isum})
				indexAt[key] = len(indexes) - 1
			}
		}
	}

	return tables, indexes
}

func partitionRollupOrder(schema *SchemaSnapshot, byQual map[QualifiedName]*Table) []*Table {
	var candidates []*Table
	for i := range schema.Tables {
		t := &schema.Tables[i]
		if t.PartitionInfo != nil && len(t.PartitionInfo.Children) > 0 {
			candidates = append(candidates, t)
		}
	}

	depthMemo := make(map[QualifiedName]int, len(candidates))
	for _, t := range candidates {
		partitionSubtreeDepth(t.Qual(), byQual, depthMemo, make(map[QualifiedName]bool))
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return depthMemo[candidates[i].Qual()] < depthMemo[candidates[j].Qual()]
	})
	return candidates
}

func partitionSubtreeDepth(q QualifiedName, byQual map[QualifiedName]*Table, memo map[QualifiedName]int, visiting map[QualifiedName]bool) int {
	if d, ok := memo[q]; ok {
		return d
	}
	t, ok := byQual[q]
	if !ok || t.PartitionInfo == nil || len(t.PartitionInfo.Children) == 0 {
		return 0
	}
	if visiting[q] {
		return 0
	}
	visiting[q] = true
	depth := 0
	for _, c := range t.PartitionInfo.Children {
		cq := QualifiedName{Schema: c.Schema, Name: c.Name}
		if d := partitionSubtreeDepth(cq, byQual, memo, visiting) + 1; d > depth {
			depth = d
		}
	}
	delete(visiting, q)
	memo[q] = depth
	return depth
}

// vacuum/analyze counts are not summed: a parent tracks its own, and folding
// in children's would misattribute them
func addTableActivity(dst *TableActivity, src TableActivity) {
	dst.SeqScan += src.SeqScan
	dst.SeqTupRead += src.SeqTupRead
	dst.IdxScan += src.IdxScan
	dst.IdxTupFetch += src.IdxTupFetch
	dst.NTupIns += src.NTupIns
	dst.NTupUpd += src.NTupUpd
	dst.NTupDel += src.NTupDel
	dst.NTupHotUpd += src.NTupHotUpd
	dst.NLiveTup += src.NLiveTup
	dst.NDeadTup += src.NDeadTup
	dst.NModSinceAnalyze += src.NModSinceAnalyze
}

func addIndexActivity(dst *IndexActivity, src IndexActivity) {
	dst.IdxScan += src.IdxScan
	dst.IdxTupRead += src.IdxTupRead
	dst.IdxTupFetch += src.IdxTupFetch
}

// RollUpActivitySnapshot returns a copy of a with partition-child activity
// rolled up (see RollUpPartitionActivity); nil inputs return a unchanged.
func RollUpActivitySnapshot(a *ActivityStatsSnapshot, schema *SchemaSnapshot) *ActivityStatsSnapshot {
	if a == nil || schema == nil {
		return a
	}
	tables, indexes := RollUpPartitionActivity(schema, a.Tables, a.Indexes)
	cp := *a
	cp.Tables = tables
	cp.Indexes = indexes
	return &cp
}
