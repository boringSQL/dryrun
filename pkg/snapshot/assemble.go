package snapshot

// AssembleAnnotated builds the in-memory join the MCP tools read: schema +
// optional planner + one NodeActivity per activity snapshot. Mirrors
// history.GetAnnotated but purely in memory, so a hosted server assembles it
// from the three snapshot blobs rather than a local history.db.
func AssembleAnnotated(schema *SchemaSnapshot, planner *PlannerStatsSnapshot, acts []ActivityStatsSnapshot) *AnnotatedSchema {
	out := &AnnotatedSchema{Schema: schema, Planner: planner}
	if len(acts) > 0 {
		nodes := make([]NodeActivity, len(acts))
		for i := range acts {
			nodes[i] = NodeActivity{
				Node:    acts[i].Node,
				Tables:  acts[i].Tables,
				Indexes: acts[i].Indexes,
			}
		}
		out.Merged = &MergedActivity{Nodes: nodes}
	}
	return out
}
