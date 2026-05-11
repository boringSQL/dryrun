package schema

// Not persisted: in-memory join of one SchemaSnapshot, one PlannerStatsSnapshot
// and N ActivityStatsSnapshot rows sharing the same schema_ref_hash
type AnnotatedSchema struct {
	Schema  *SchemaSnapshot
	Planner *PlannerStatsSnapshot
	Merged  *MergedActivity
}

// Activity across nodes for a single SchemaSnapshot; one entry per node
type MergedActivity struct {
	Nodes []NodeActivity
}

type NodeActivity struct {
	Node    NodeIdentity
	Tables  []TableActivityEntry
	Indexes []IndexActivityEntry
}

func (a *AnnotatedSchema) SizingFor(q QualifiedName) *TableSizing {
	if a == nil || a.Planner == nil {
		return nil
	}
	for i := range a.Planner.Tables {
		if a.Planner.Tables[i].Table == q {
			return &a.Planner.Tables[i].Sizing
		}
	}
	return nil
}

func (a *AnnotatedSchema) IndexSizingFor(table QualifiedName, index string) *IndexSizing {
	if a == nil || a.Planner == nil {
		return nil
	}
	for i := range a.Planner.Indexes {
		if a.Planner.Indexes[i].Table == table && a.Planner.Indexes[i].Index == index {
			return &a.Planner.Indexes[i].Sizing
		}
	}
	return nil
}

func (a *AnnotatedSchema) ActivityForNode(source string, q QualifiedName) *TableActivity {
	if a == nil || a.Merged == nil {
		return nil
	}
	for i := range a.Merged.Nodes {
		if a.Merged.Nodes[i].Node.Source != source {
			continue
		}
		for j := range a.Merged.Nodes[i].Tables {
			if a.Merged.Nodes[i].Tables[j].Table == q {
				return &a.Merged.Nodes[i].Tables[j].Activity
			}
		}
	}
	return nil
}

func (a *AnnotatedSchema) IndexActivityForNode(source string, table QualifiedName, index string) *IndexActivity {
	if a == nil || a.Merged == nil {
		return nil
	}
	for i := range a.Merged.Nodes {
		if a.Merged.Nodes[i].Node.Source != source {
			continue
		}
		for j := range a.Merged.Nodes[i].Indexes {
			e := &a.Merged.Nodes[i].Indexes[j]
			if e.Table == table && e.Index == index {
				return &e.Activity
			}
		}
	}
	return nil
}

func (a *AnnotatedSchema) Nodes() []NodeIdentity {
	if a == nil || a.Merged == nil {
		return nil
	}
	out := make([]NodeIdentity, len(a.Merged.Nodes))
	for i := range a.Merged.Nodes {
		out[i] = a.Merged.Nodes[i].Node
	}
	return out
}

// Preserves prior planner/merged only when schema_ref still matches the new DDL
func RebuildAfterRefresh(prev *AnnotatedSchema, refreshed *SchemaSnapshot) *AnnotatedSchema {
	out := &AnnotatedSchema{Schema: refreshed}
	if prev == nil || refreshed == nil {
		return out
	}
	if prev.Planner != nil && prev.Planner.SchemaRefHash == refreshed.ContentHash {
		out.Planner = prev.Planner
	}
	if prev.Merged != nil && prev.Schema != nil && prev.Schema.ContentHash == refreshed.ContentHash {
		out.Merged = prev.Merged
	}
	return out
}
