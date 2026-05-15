package query

import (
	"encoding/json"
	"fmt"
)

type PlanNode struct {
	NodeType            string     `json:"node_type"`
	RelationName        *string    `json:"relation_name,omitempty"`
	Schema              *string    `json:"schema,omitempty"`
	Alias               *string    `json:"alias,omitempty"`
	StartupCost         float64    `json:"startup_cost"`
	TotalCost           float64    `json:"total_cost"`
	PlanRows            float64    `json:"plan_rows"`
	PlanWidth           int64      `json:"plan_width"`
	ActualRows          *float64   `json:"actual_rows,omitempty"`
	ActualLoops         *float64   `json:"actual_loops,omitempty"`
	ActualStartupTime   *float64   `json:"actual_startup_time,omitempty"`
	ActualTotalTime     *float64   `json:"actual_total_time,omitempty"`
	SharedHitBlocks     *int64     `json:"shared_hit_blocks,omitempty"`
	SharedReadBlocks    *int64     `json:"shared_read_blocks,omitempty"`
	IndexName           *string    `json:"index_name,omitempty"`
	IndexCond           *string    `json:"index_cond,omitempty"`
	Filter              *string    `json:"filter,omitempty"`
	RowsRemovedByFilter *float64   `json:"rows_removed_by_filter,omitempty"`
	SortKey             []string   `json:"sort_key,omitempty"`
	SortMethod          *string    `json:"sort_method,omitempty"`
	HashCond            *string    `json:"hash_cond,omitempty"`
	JoinType            *string    `json:"join_type,omitempty"`
	SubplansRemoved     *int64     `json:"subplans_removed,omitempty"`
	CTEName             *string    `json:"cte_name,omitempty"`
	ParentRelationship  *string    `json:"parent_relationship,omitempty"`
	Children            []PlanNode `json:"children,omitempty"`
}

func ParsePlanJSON(data json.RawMessage) (*PlanNode, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, fmt.Errorf("plan node is not an object: %w", err)
	}

	node := &PlanNode{
		NodeType:            getStr(obj, "Node Type"),
		RelationName:        getOptStr(obj, "Relation Name"),
		Schema:              getOptStr(obj, "Schema"),
		Alias:               getOptStr(obj, "Alias"),
		StartupCost:         getFloat(obj, "Startup Cost"),
		TotalCost:           getFloat(obj, "Total Cost"),
		PlanRows:            getFloat(obj, "Plan Rows"),
		PlanWidth:           getInt(obj, "Plan Width"),
		ActualRows:          getOptFloat(obj, "Actual Rows"),
		ActualLoops:         getOptFloat(obj, "Actual Loops"),
		ActualStartupTime:   getOptFloat(obj, "Actual Startup Time"),
		ActualTotalTime:     getOptFloat(obj, "Actual Total Time"),
		SharedHitBlocks:     getOptInt(obj, "Shared Hit Blocks"),
		SharedReadBlocks:    getOptInt(obj, "Shared Read Blocks"),
		IndexName:           getOptStr(obj, "Index Name"),
		IndexCond:           getOptStr(obj, "Index Cond"),
		Filter:              getOptStr(obj, "Filter"),
		RowsRemovedByFilter: getOptFloat(obj, "Rows Removed by Filter"),
		SortMethod:          getOptStr(obj, "Sort Method"),
		HashCond:            getOptStr(obj, "Hash Cond"),
		JoinType:            getOptStr(obj, "Join Type"),
		SubplansRemoved:     getOptInt(obj, "Subplans Removed"),
		CTEName:             getOptStr(obj, "CTE Name"),
		ParentRelationship:  getOptStr(obj, "Parent Relationship"),
	}

	if raw, ok := obj["Sort Key"]; ok {
		var keys []string
		_ = json.Unmarshal(raw, &keys)
		node.SortKey = keys
	}

	if raw, ok := obj["Plans"]; ok {
		var plans []json.RawMessage
		if err := json.Unmarshal(raw, &plans); err == nil {
			for _, p := range plans {
				child, err := ParsePlanJSON(p)
				if err != nil {
					return nil, err
				}
				node.Children = append(node.Children, *child)
			}
		}
	}

	return node, nil
}

func getStr(obj map[string]json.RawMessage, key string) string {
	raw, ok := obj[key]
	if !ok {
		return ""
	}
	var s string
	_ = json.Unmarshal(raw, &s)
	return s
}

func getOptStr(obj map[string]json.RawMessage, key string) *string {
	raw, ok := obj[key]
	if !ok {
		return nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil
	}
	return &s
}

func getFloat(obj map[string]json.RawMessage, key string) float64 {
	raw, ok := obj[key]
	if !ok {
		return 0
	}
	var f float64
	_ = json.Unmarshal(raw, &f)
	return f
}

func getOptFloat(obj map[string]json.RawMessage, key string) *float64 {
	raw, ok := obj[key]
	if !ok {
		return nil
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil
	}
	return &f
}

func getInt(obj map[string]json.RawMessage, key string) int64 {
	raw, ok := obj[key]
	if !ok {
		return 0
	}
	var i int64
	_ = json.Unmarshal(raw, &i)
	return i
}

func getOptInt(obj map[string]json.RawMessage, key string) *int64 {
	raw, ok := obj[key]
	if !ok {
		return nil
	}
	var i int64
	if err := json.Unmarshal(raw, &i); err != nil {
		return nil
	}
	return &i
}
