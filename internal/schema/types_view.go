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
