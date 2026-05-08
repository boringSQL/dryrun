package query

import (
	"encoding/json"
	"testing"
)

func TestParsePlanJSON(t *testing.T) {
	raw := json.RawMessage(`{
		"Node Type": "Seq Scan",
		"Relation Name": "users",
		"Schema": "public",
		"Startup Cost": 0.0,
		"Total Cost": 35.5,
		"Plan Rows": 2550,
		"Plan Width": 36,
		"Filter": "(age > 30)"
	}`)

	node, err := ParsePlanJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	if node.NodeType != "Seq Scan" {
		t.Errorf("got %q, want Seq Scan", node.NodeType)
	}
	if node.RelationName == nil || *node.RelationName != "users" {
		t.Error("expected relation_name = users")
	}
	if node.TotalCost != 35.5 {
		t.Errorf("got cost %f, want 35.5", node.TotalCost)
	}
	if node.Filter == nil || *node.Filter != "(age > 30)" {
		t.Error("expected filter")
	}
}

func TestParsePlanJSONWithChildren(t *testing.T) {
	raw := json.RawMessage(`{
		"Node Type": "Nested Loop",
		"Startup Cost": 0.0,
		"Total Cost": 100.0,
		"Plan Rows": 10,
		"Plan Width": 8,
		"Plans": [
			{"Node Type": "Index Scan", "Startup Cost": 0.0, "Total Cost": 10.0, "Plan Rows": 1, "Plan Width": 4},
			{"Node Type": "Seq Scan", "Relation Name": "orders", "Startup Cost": 0.0, "Total Cost": 50.0, "Plan Rows": 500, "Plan Width": 8}
		]
	}`)

	node, err := ParsePlanJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(node.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(node.Children))
	}
	if node.Children[0].NodeType != "Index Scan" {
		t.Errorf("child 0: got %q, want Index Scan", node.Children[0].NodeType)
	}
	if node.Children[1].NodeType != "Seq Scan" {
		t.Errorf("child 1: got %q, want Seq Scan", node.Children[1].NodeType)
	}
}

func TestParsePlanJSONSubplansRemoved(t *testing.T) {
	raw := json.RawMessage(`{
		"Node Type": "Append",
		"Startup Cost": 0.0,
		"Total Cost": 200.0,
		"Plan Rows": 5000,
		"Plan Width": 16,
		"Subplans Removed": 5,
		"Plans": [
			{"Node Type": "Seq Scan", "Relation Name": "events_2025_01", "Startup Cost": 0.0, "Total Cost": 50.0, "Plan Rows": 1000, "Plan Width": 16},
			{"Node Type": "Seq Scan", "Relation Name": "events_2025_02", "Startup Cost": 0.0, "Total Cost": 50.0, "Plan Rows": 1000, "Plan Width": 16}
		]
	}`)

	node, err := ParsePlanJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	if node.NodeType != "Append" {
		t.Errorf("got %q, want Append", node.NodeType)
	}
	if node.SubplansRemoved == nil {
		t.Fatal("expected SubplansRemoved to be set")
	}
	if *node.SubplansRemoved != 5 {
		t.Errorf("got SubplansRemoved=%d, want 5", *node.SubplansRemoved)
	}
	if len(node.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(node.Children))
	}
}

func TestParsePlanJSONSubplansRemovedAbsent(t *testing.T) {
	raw := json.RawMessage(`{
		"Node Type": "Seq Scan",
		"Startup Cost": 0.0,
		"Total Cost": 10.0,
		"Plan Rows": 100,
		"Plan Width": 8
	}`)

	node, err := ParsePlanJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	if node.SubplansRemoved != nil {
		t.Errorf("expected SubplansRemoved to be nil, got %d", *node.SubplansRemoved)
	}
}
