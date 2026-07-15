package snapshot

import "testing"

func TestAssembleAnnotated_AttachesPlannerAndNodes(t *testing.T) {
	sch := &SchemaSnapshot{Database: "app"}
	pl := &PlannerStatsSnapshot{}
	acts := []ActivityStatsSnapshot{
		{Node: NodeIdentity{Source: "node-a"}},
		{Node: NodeIdentity{Source: "node-b"}},
	}

	a := AssembleAnnotated(sch, pl, acts)

	if a.Schema != sch {
		t.Fatal("schema not carried through")
	}
	if a.Planner != pl {
		t.Fatal("planner not attached")
	}
	if a.Merged == nil || len(a.Merged.Nodes) != 2 {
		t.Fatalf("merged activity = %+v, want 2 nodes", a.Merged)
	}
	if a.Merged.Nodes[0].Node.Source != "node-a" || a.Merged.Nodes[1].Node.Source != "node-b" {
		t.Fatalf("node order/identity lost: %+v", a.Merged.Nodes)
	}
}

func TestAssembleAnnotated_NoActivityLeavesMergedNil(t *testing.T) {
	a := AssembleAnnotated(&SchemaSnapshot{}, nil, nil)
	if a.Merged != nil {
		t.Fatal("empty activity must leave Merged nil, not an empty MergedActivity")
	}
	if a.Planner != nil {
		t.Fatal("nil planner must stay nil")
	}
}
