package snapshot

// RollUpPartitionSizing returns a copy of planner with partitioned parents'
// sizing summed from their physical children.
func RollUpPartitionSizing(schema *SchemaSnapshot, planner *PlannerStatsSnapshot) *PlannerStatsSnapshot {
	if schema == nil || planner == nil {
		return planner
	}

	tableByQual := make(map[QualifiedName]*Table, len(schema.Tables))
	for i := range schema.Tables {
		tableByQual[schema.Tables[i].Qual()] = &schema.Tables[i]
	}

	order := partitionRollupOrder(schema, tableByQual)
	if len(order) == 0 {
		return planner
	}

	tables := append([]TableSizingEntry(nil), planner.Tables...)
	indexes := append([]IndexSizingEntry(nil), planner.Indexes...)

	tableAt := make(map[QualifiedName]int, len(tables))
	for i, te := range tables {
		tableAt[te.Table] = i
	}
	indexAt := make(map[indexActivityKey]int, len(indexes))
	for i, ie := range indexes {
		indexAt[indexActivityKey{ie.Table, ie.Index}] = i
	}

	for _, t := range order {
		parentQual := t.Qual()

		var sum TableSizing
		for _, child := range t.PartitionInfo.Children {
			childQual := QualifiedName{Schema: child.Schema, Name: child.Name}
			if i, ok := tableAt[childQual]; ok {
				addTableSizing(&sum, tables[i].Sizing)
			}
		}
		if i, ok := tableAt[parentQual]; ok {
			addTableSizing(&tables[i].Sizing, sum)
		} else {
			tables = append(tables, TableSizingEntry{Table: parentQual, Sizing: sum})
			tableAt[parentQual] = len(tables) - 1
		}

		for _, idx := range t.Indexes {
			if len(idx.Children) == 0 {
				continue
			}
			var isum IndexSizing
			for _, child := range idx.Children {
				childKey := indexActivityKey{QualifiedName{Schema: child.Schema, Name: child.Table}, child.Index}
				if i, ok := indexAt[childKey]; ok {
					addIndexSizing(&isum, indexes[i].Sizing)
				}
			}
			key := indexActivityKey{parentQual, idx.Name}
			if i, ok := indexAt[key]; ok {
				addIndexSizing(&indexes[i].Sizing, isum)
			} else {
				indexes = append(indexes, IndexSizingEntry{Table: parentQual, Index: idx.Name, Sizing: isum})
				indexAt[key] = len(indexes) - 1
			}
		}
	}

	out := *planner
	out.Tables = tables
	out.Indexes = indexes
	return &out
}

// Reltuples: -1 means "never measured", so replace rather than sum.
func addTableSizing(dst *TableSizing, src TableSizing) {
	if dst.Reltuples < 0 {
		dst.Reltuples = src.Reltuples
	} else {
		dst.Reltuples += src.Reltuples
	}
	dst.Relpages += src.Relpages
	dst.TableSize += src.TableSize
	dst.TotalRelationSize += src.TotalRelationSize
	dst.IndexesSize += src.IndexesSize
	dst.ToastSize += src.ToastSize
}

func addIndexSizing(dst *IndexSizing, src IndexSizing) {
	if dst.Reltuples < 0 {
		dst.Reltuples = src.Reltuples
	} else {
		dst.Reltuples += src.Reltuples
	}
	dst.Relpages += src.Relpages
	dst.Size += src.Size
}
