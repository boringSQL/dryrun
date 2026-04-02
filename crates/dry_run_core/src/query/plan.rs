use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanNode {
    pub node_type: String,
    pub relation_name: Option<String>,
    pub schema: Option<String>,
    pub alias: Option<String>,
    pub startup_cost: f64,
    pub total_cost: f64,
    pub plan_rows: f64,
    pub plan_width: i64,
    pub actual_rows: Option<f64>,
    pub actual_loops: Option<f64>,
    pub actual_startup_time: Option<f64>,
    pub actual_total_time: Option<f64>,
    pub shared_hit_blocks: Option<i64>,
    pub shared_read_blocks: Option<i64>,
    pub index_name: Option<String>,
    pub index_cond: Option<String>,
    pub filter: Option<String>,
    pub rows_removed_by_filter: Option<f64>,
    pub sort_key: Option<Vec<String>>,
    pub sort_method: Option<String>,
    pub hash_cond: Option<String>,
    pub join_type: Option<String>,
    pub subplans_removed: Option<i64>,
    pub cte_name: Option<String>,
    pub parent_relationship: Option<String>,
    pub children: Vec<PlanNode>,
}

pub fn parse_plan_json(value: &serde_json::Value) -> Result<PlanNode> {
    let obj = value
        .as_object()
        .ok_or_else(|| Error::Introspection("plan node is not an object".into()))?;

    let children = if let Some(plans) = obj.get("Plans").and_then(|p| p.as_array()) {
        plans
            .iter()
            .map(parse_plan_json)
            .collect::<Result<Vec<_>>>()?
    } else {
        vec![]
    };

    Ok(PlanNode {
        node_type: get_str(obj, "Node Type"),
        relation_name: get_opt_str(obj, "Relation Name"),
        schema: get_opt_str(obj, "Schema"),
        alias: get_opt_str(obj, "Alias"),
        startup_cost: get_f64(obj, "Startup Cost"),
        total_cost: get_f64(obj, "Total Cost"),
        plan_rows: get_f64(obj, "Plan Rows"),
        plan_width: get_i64(obj, "Plan Width"),
        actual_rows: get_opt_f64(obj, "Actual Rows"),
        actual_loops: get_opt_f64(obj, "Actual Loops"),
        actual_startup_time: get_opt_f64(obj, "Actual Startup Time"),
        actual_total_time: get_opt_f64(obj, "Actual Total Time"),
        shared_hit_blocks: get_opt_i64(obj, "Shared Hit Blocks"),
        shared_read_blocks: get_opt_i64(obj, "Shared Read Blocks"),
        index_name: get_opt_str(obj, "Index Name"),
        index_cond: get_opt_str(obj, "Index Cond"),
        filter: get_opt_str(obj, "Filter"),
        rows_removed_by_filter: get_opt_f64(obj, "Rows Removed by Filter"),
        sort_key: obj.get("Sort Key").and_then(|v| v.as_array()).map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        }),
        sort_method: get_opt_str(obj, "Sort Method"),
        hash_cond: get_opt_str(obj, "Hash Cond"),
        join_type: get_opt_str(obj, "Join Type"),
        subplans_removed: get_opt_i64(obj, "Subplans Removed"),
        cte_name: get_opt_str(obj, "CTE Name"),
        parent_relationship: get_opt_str(obj, "Parent Relationship"),
        children,
    })
}

fn get_str(obj: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    obj.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn get_opt_str(obj: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<String> {
    obj.get(key).and_then(|v| v.as_str()).map(String::from)
}

fn get_f64(obj: &serde_json::Map<String, serde_json::Value>, key: &str) -> f64 {
    obj.get(key).and_then(|v| v.as_f64()).unwrap_or(0.0)
}

fn get_opt_f64(obj: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<f64> {
    obj.get(key).and_then(|v| v.as_f64())
}

fn get_i64(obj: &serde_json::Map<String, serde_json::Value>, key: &str) -> i64 {
    obj.get(key).and_then(|v| v.as_i64()).unwrap_or(0)
}

fn get_opt_i64(obj: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<i64> {
    obj.get(key).and_then(|v| v.as_i64())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_plan() {
        let json = serde_json::json!({
            "Node Type": "Seq Scan",
            "Relation Name": "users",
            "Schema": "public",
            "Alias": "users",
            "Startup Cost": 0.0,
            "Total Cost": 35.5,
            "Plan Rows": 2550,
            "Plan Width": 64
        });
        let plan = parse_plan_json(&json).unwrap();
        assert_eq!(plan.node_type, "Seq Scan");
        assert_eq!(plan.relation_name.as_deref(), Some("users"));
        assert_eq!(plan.total_cost, 35.5);
        assert_eq!(plan.plan_rows, 2550.0);
        assert!(plan.children.is_empty());
    }

    #[test]
    fn parse_wrapped_array_format() {
        // EXPLAIN (FORMAT JSON) returns [{"Plan": {...}}]
        let json = serde_json::json!([{
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "orders",
                "Schema": "public",
                "Startup Cost": 0.0,
                "Total Cost": 450.0,
                "Plan Rows": 10000,
                "Plan Width": 48
            }
        }]);
        let plan_value = json.as_array().unwrap().first().unwrap().get("Plan").unwrap();
        let plan = parse_plan_json(plan_value).unwrap();
        assert_eq!(plan.node_type, "Seq Scan");
        assert_eq!(plan.relation_name.as_deref(), Some("orders"));
        assert_eq!(plan.plan_rows, 10000.0);
    }

    #[test]
    fn parse_bare_object_format() {
        // bare {"Plan": {...}} without wrapping array
        let json = serde_json::json!({
            "Plan": {
                "Node Type": "Index Scan",
                "Relation Name": "users",
                "Schema": "public",
                "Index Name": "users_pkey",
                "Startup Cost": 0.0,
                "Total Cost": 8.27,
                "Plan Rows": 1,
                "Plan Width": 64
            }
        });
        let plan_value = json.get("Plan").unwrap();
        let plan = parse_plan_json(plan_value).unwrap();
        assert_eq!(plan.node_type, "Index Scan");
        assert_eq!(plan.index_name.as_deref(), Some("users_pkey"));
    }

    #[test]
    fn parse_analyze_buffers_plan() {
        let json = serde_json::json!({
            "Node Type": "Seq Scan",
            "Relation Name": "events",
            "Schema": "public",
            "Startup Cost": 0.0,
            "Total Cost": 1500.0,
            "Plan Rows": 50000,
            "Plan Width": 120,
            "Actual Rows": 48732,
            "Actual Loops": 1,
            "Actual Startup Time": 0.015,
            "Actual Total Time": 42.5,
            "Shared Hit Blocks": 800,
            "Shared Read Blocks": 200,
            "Filter": "(status = 'active')",
            "Rows Removed by Filter": 1268
        });
        let plan = parse_plan_json(&json).unwrap();
        assert_eq!(plan.actual_rows, Some(48732.0));
        assert_eq!(plan.actual_total_time, Some(42.5));
        assert_eq!(plan.shared_hit_blocks, Some(800));
        assert_eq!(plan.shared_read_blocks, Some(200));
        assert_eq!(plan.rows_removed_by_filter, Some(1268.0));
        assert_eq!(plan.filter.as_deref(), Some("(status = 'active')"));
    }

    #[test]
    fn parse_subplans_removed() {
        let json = serde_json::json!({
            "Node Type": "Append",
            "Startup Cost": 0.0,
            "Total Cost": 100.0,
            "Plan Rows": 1000,
            "Plan Width": 64,
            "Subplans Removed": 8,
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "orders_2024_q1",
                    "Schema": "public",
                    "Startup Cost": 0.0,
                    "Total Cost": 25.0,
                    "Plan Rows": 250,
                    "Plan Width": 64
                }
            ]
        });
        let plan = parse_plan_json(&json).unwrap();
        assert_eq!(plan.subplans_removed, Some(8));
        assert_eq!(plan.children.len(), 1);
        assert_eq!(plan.children[0].subplans_removed, None);
    }

    #[test]
    fn parse_plan_missing_plan_key_is_error() {
        let json = serde_json::json!("not an object");
        assert!(parse_plan_json(&json).is_err());
    }

    #[test]
    fn parse_nested_plan() {
        let json = serde_json::json!({
            "Node Type": "Nested Loop",
            "Join Type": "Inner",
            "Startup Cost": 0.0,
            "Total Cost": 100.0,
            "Plan Rows": 10,
            "Plan Width": 128,
            "Plans": [
                {
                    "Node Type": "Index Scan",
                    "Relation Name": "users",
                    "Schema": "public",
                    "Index Name": "users_pkey",
                    "Startup Cost": 0.0,
                    "Total Cost": 8.0,
                    "Plan Rows": 1,
                    "Plan Width": 64
                },
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "orders",
                    "Schema": "public",
                    "Startup Cost": 0.0,
                    "Total Cost": 50.0,
                    "Plan Rows": 100,
                    "Plan Width": 64
                }
            ]
        });
        let plan = parse_plan_json(&json).unwrap();
        assert_eq!(plan.node_type, "Nested Loop");
        assert_eq!(plan.join_type.as_deref(), Some("Inner"));
        assert_eq!(plan.children.len(), 2);
        assert_eq!(plan.children[0].node_type, "Index Scan");
        assert_eq!(plan.children[1].node_type, "Seq Scan");
    }
}
