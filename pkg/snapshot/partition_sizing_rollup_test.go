package snapshot

import "testing"

func sizingFor(t *testing.T, p *PlannerStatsSnapshot, schema, name string) *TableSizingEntry {
	t.Helper()
	for i := range p.Tables {
		if p.Tables[i].Table.Schema == schema && p.Tables[i].Table.Name == name {
			return &p.Tables[i]
		}
	}
	t.Fatalf("no sizing entry for %s.%s in %+v", schema, name, p.Tables)
	return nil
}

// A partitioned parent's own relallvisible is always 0 while its relpages is
// summed from the children, so the roll-up must sum it too or every parent
// looks like it has no all-visible pages.
func TestRollUpPartitionSizing_SumsRelAllVisible(t *testing.T) {
	schema := &SchemaSnapshot{Tables: []Table{
		{
			Schema: "public", Name: "events",
			PartitionInfo: &PartitionInfo{
				Strategy: PartitionRange, Key: "d",
				Children: []PartitionChild{{Schema: "public", Name: "events_2026"}},
			},
		},
		{Schema: "public", Name: "events_2026"},
	}}
	parentOwn := int64(0)
	childVisible := int64(40)
	planner := &PlannerStatsSnapshot{Tables: []TableSizingEntry{
		{Table: QualifiedName{Schema: "public", Name: "events"}, Sizing: TableSizing{RelAllVisible: &parentOwn}},
		{
			Table: QualifiedName{Schema: "public", Name: "events_2026"},
			Sizing: TableSizing{
				Relpages: 50, Reltuples: 5_000, RelAllVisible: &childVisible,
			},
		},
	}}

	out := RollUpPartitionSizing(schema, planner)
	parent := sizingFor(t, out, "public", "events")
	if parent.Sizing.Relpages != 50 {
		t.Errorf("parent relpages = %d, want 50", parent.Sizing.Relpages)
	}
	if parent.Sizing.RelAllVisible == nil || *parent.Sizing.RelAllVisible != 40 {
		t.Errorf("parent relallvisible = %v, want 40", parent.Sizing.RelAllVisible)
	}
	if *planner.Tables[0].Sizing.RelAllVisible != 0 {
		t.Error("roll-up mutated the input snapshot's relallvisible")
	}
}

// A parent with no planner row is created from the children. A child captured
// before relallvisible existed must leave the parent unknown, not zero.
func TestRollUpPartitionSizing_RelAllVisibleStaysNilWhenChildrenLackIt(t *testing.T) {
	schema := &SchemaSnapshot{Tables: []Table{
		{
			Schema: "public", Name: "logs",
			PartitionInfo: &PartitionInfo{
				Strategy: PartitionRange, Key: "d",
				Children: []PartitionChild{{Schema: "public", Name: "logs_2026"}},
			},
		},
		{Schema: "public", Name: "logs_2026"},
	}}
	planner := &PlannerStatsSnapshot{Tables: []TableSizingEntry{
		{Table: QualifiedName{Schema: "public", Name: "logs_2026"}, Sizing: TableSizing{Relpages: 20, Reltuples: 100}},
	}}

	out := RollUpPartitionSizing(schema, planner)
	parent := sizingFor(t, out, "public", "logs")
	if parent.Sizing.Relpages != 20 {
		t.Errorf("parent relpages = %d, want 20", parent.Sizing.Relpages)
	}
	if parent.Sizing.RelAllVisible != nil {
		t.Errorf("parent relallvisible = %v, want nil", *parent.Sizing.RelAllVisible)
	}
}
