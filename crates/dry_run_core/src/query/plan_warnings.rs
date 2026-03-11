use super::explain::PlanWarning;
use super::plan::PlanNode;
use crate::schema::SchemaSnapshot;

const SEQ_SCAN_ROW_THRESHOLD: f64 = 5_000.0;

pub fn detect_plan_warnings(plan: &PlanNode, schema: Option<&SchemaSnapshot>) -> Vec<PlanWarning> {
    let mut warnings = Vec::new();
    walk_plan(plan, schema, &mut warnings);
    warnings
}

fn walk_plan(node: &PlanNode, schema: Option<&SchemaSnapshot>, warnings: &mut Vec<PlanWarning>) {
    detect_seq_scan_large_table(node, schema, warnings);
    detect_nested_loop_seq_scan(node, warnings);
    detect_sort_without_index(node, warnings);
    detect_high_rows_removed(node, warnings);

    for child in &node.children {
        walk_plan(child, schema, warnings);
    }
}

fn detect_seq_scan_large_table(
    node: &PlanNode,
    schema: Option<&SchemaSnapshot>,
    warnings: &mut Vec<PlanWarning>,
) {
    if node.node_type != "Seq Scan" {
        return;
    }

    let table_name = match &node.relation_name {
        Some(name) => name,
        None => return,
    };

    let row_count = if node.plan_rows > 0.0 {
        node.plan_rows
    } else if let Some(schema) = schema {
        let schema_name = node.schema.as_deref().unwrap_or("public");
        schema
            .tables
            .iter()
            .find(|t| t.name == *table_name && t.schema == schema_name)
            .and_then(|t| t.stats.as_ref())
            .map(|s| s.reltuples)
            .unwrap_or(0.0)
    } else {
        0.0
    };

    if row_count >= SEQ_SCAN_ROW_THRESHOLD {
        warnings.push(PlanWarning {
            severity: "warning".into(),
            message: format!(
                "sequential scan on '{}' (~{} rows) — consider adding an index",
                table_name, row_count as i64
            ),
            node_type: "Seq Scan".into(),
            detail: node.filter.clone(),
        });
    }
}

fn detect_nested_loop_seq_scan(node: &PlanNode, warnings: &mut Vec<PlanWarning>) {
    if node.node_type != "Nested Loop" {
        return;
    }

    if let Some(inner) = node.children.get(1) {
        if inner.node_type == "Seq Scan" && inner.plan_rows > 100.0 {
            let table_name = inner.relation_name.as_deref().unwrap_or("unknown");
            warnings.push(PlanWarning {
                severity: "warning".into(),
                message: format!(
                    "nested loop with sequential scan on inner side '{}' (~{} rows) — this executes once per outer row",
                    table_name,
                    inner.plan_rows as i64
                ),
                node_type: "Nested Loop".into(),
                detail: None,
            });
        }
    }
}

fn detect_sort_without_index(node: &PlanNode, warnings: &mut Vec<PlanWarning>) {
    if node.node_type != "Sort" {
        return;
    }

    if node.plan_rows > 10_000.0 {
        let sort_keys = node
            .sort_key
            .as_ref()
            .map(|k| k.join(", "))
            .unwrap_or_default();
        warnings.push(PlanWarning {
            severity: "info".into(),
            message: format!(
                "sort on ~{} rows (keys: {}) — consider an index to avoid the sort",
                node.plan_rows as i64, sort_keys
            ),
            node_type: "Sort".into(),
            detail: None,
        });
    }
}

fn detect_high_rows_removed(node: &PlanNode, warnings: &mut Vec<PlanWarning>) {
    if let Some(removed) = node.rows_removed_by_filter {
        if let Some(actual) = node.actual_rows {
            if removed > 0.0 && actual > 0.0 && removed / (removed + actual) > 0.9 {
                warnings.push(PlanWarning {
                    severity: "warning".into(),
                    message: format!(
                        "'{}' filter removed {:.0} rows, kept {:.0} — index on the filter column would help",
                        node.node_type, removed, actual
                    ),
                    node_type: node.node_type.clone(),
                    detail: node.filter.clone(),
                });
            }
        }
    }
}
