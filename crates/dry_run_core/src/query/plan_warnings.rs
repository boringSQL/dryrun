use super::explain::PlanWarning;
use super::plan::PlanNode;
use crate::jit;
use crate::schema::{AnnotatedSchema, QualifiedName, SchemaSnapshot};

const SEQ_SCAN_ROW_THRESHOLD: f64 = 5_000.0;

// Plan warnings — walks an EXPLAIN tree and surfaces patterns worth
// flagging (large seq scans, nested-loop antipatterns, missing partition
// pruning, materialized CTEs).
//
// Schema reference is `Option<&AnnotatedSchema>` because warnings work
// just fine without one — the plan itself usually has all the info we
// need. The schema unlocks two refinements:
//   - `detect_seq_scan_large_table` falls back to planner reltuples when
//     the plan's own `plan_rows` is zero (some EXPLAIN paths emit that).
//   - partition / CTE warnings need the DDL to know which tables are
//     partitioned. They read `annotated.schema` directly.
pub fn detect_plan_warnings(
    plan: &PlanNode,
    annotated: Option<&AnnotatedSchema<'_>>,
) -> Vec<PlanWarning> {
    let mut warnings = Vec::new();
    walk_plan(plan, annotated, &mut warnings);
    warnings
}

fn walk_plan(
    node: &PlanNode,
    annotated: Option<&AnnotatedSchema<'_>>,
    warnings: &mut Vec<PlanWarning>,
) {
    detect_seq_scan_large_table(node, annotated, warnings);
    detect_nested_loop_seq_scan(node, warnings);
    detect_sort_without_index(node, warnings);
    detect_high_rows_removed(node, warnings);
    detect_partition_pruning_issues(node, annotated.map(|a| a.schema), warnings);
    detect_cte_materialized(node, annotated.map(|a| a.schema), warnings);

    for child in &node.children {
        walk_plan(child, annotated, warnings);
    }
}

fn detect_seq_scan_large_table(
    node: &PlanNode,
    annotated: Option<&AnnotatedSchema<'_>>,
    warnings: &mut Vec<PlanWarning>,
) {
    if node.node_type != "Seq Scan" {
        return;
    }

    let table_name = match &node.relation_name {
        Some(name) => name,
        None => return,
    };

    // Prefer the plan's own row estimate; fall back to planner reltuples
    // when it's zero (some EXPLAIN modes don't emit it). When neither is
    // available we treat the row count as zero, which suppresses the
    // warning — better silent than wrong.
    let row_count = if node.plan_rows > 0.0 {
        node.plan_rows
    } else if let Some(annotated) = annotated {
        let schema_name = node.schema.as_deref().unwrap_or("public");
        annotated
            .reltuples(&QualifiedName::new(schema_name, table_name))
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

    if let Some(inner) = node.children.get(1)
        && inner.node_type == "Seq Scan"
        && inner.plan_rows > 100.0
    {
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
    if let Some(removed) = node.rows_removed_by_filter
        && let Some(actual) = node.actual_rows
        && removed > 0.0
        && actual > 0.0
        && removed / (removed + actual) > 0.9
    {
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

fn detect_partition_pruning_issues(
    node: &PlanNode,
    schema: Option<&SchemaSnapshot>,
    warnings: &mut Vec<PlanWarning>,
) {
    let schema = match schema {
        Some(s) => s,
        None => return,
    };

    if node.node_type != "Append" && node.node_type != "Merge Append" {
        return;
    }

    let mut parent: Option<&crate::schema::Table> = None;
    let mut scanned = 0usize;

    for child in &node.children {
        let child_name = match &child.relation_name {
            Some(n) => n,
            None => continue,
        };

        if let Some(p) = find_partition_parent(child_name, schema) {
            if parent.is_none() {
                parent = Some(p);
            }
            scanned += 1;
        }
    }

    let parent = match parent {
        Some(p) => p,
        None => return,
    };

    let pi = match &parent.partition_info {
        Some(pi) => pi,
        None => return,
    };

    let total = pi.children.len();
    let pruned = node.subplans_removed.unwrap_or(0);

    let qualified = format!("{}.{}", parent.schema, parent.name);

    if pruned == 0 {
        let e = jit::no_partition_pruning(&qualified, &pi.key, scanned, total);
        warnings.push(PlanWarning {
            severity: "warning".into(),
            message: e.to_string(),
            node_type: node.node_type.clone(),
            detail: None,
        });
    } else if scanned > total / 2 {
        warnings.push(PlanWarning {
            severity: "info".into(),
            message: format!(
                "partial pruning on '{qualified}': {pruned} partitions pruned, {scanned} still scanned"
            ),
            node_type: node.node_type.clone(),
            detail: None,
        });
    }
}

fn detect_cte_materialized(
    node: &PlanNode,
    schema: Option<&SchemaSnapshot>,
    warnings: &mut Vec<PlanWarning>,
) {
    if node.node_type != "CTE Scan" {
        return;
    }
    let cte_name = match &node.cte_name {
        Some(n) => n,
        None => return,
    };
    let rows = node.plan_rows as i64;
    if rows < 1000 {
        return;
    }

    let mut e = jit::cte_materialized(cte_name, rows);

    // check if CTE scans a partitioned table
    if let Some(schema) = schema {
        for child in &node.children {
            if child.node_type == "Append" || child.node_type == "Merge Append" {
                for grandchild in &child.children {
                    if let Some(rel) = &grandchild.relation_name
                        && let Some(p) = find_partition_parent(rel, schema)
                    {
                        let qualified = format!("{}.{}", p.schema, p.name);
                        e = jit::cte_over_partitioned_table(cte_name, &qualified);
                        break;
                    }
                }
            }
        }
    }

    warnings.push(PlanWarning {
        severity: "warning".into(),
        message: e.to_string(),
        node_type: "CTE Scan".into(),
        detail: None,
    });
}

fn find_partition_parent<'a>(
    child_table_name: &str,
    schema: &'a SchemaSnapshot,
) -> Option<&'a crate::schema::Table> {
    schema.tables.iter().find(|t| {
        t.partition_info
            .as_ref()
            .is_some_and(|pi| pi.children.iter().any(|c| c.name == child_table_name))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_seq_scan(table: &str, rows: f64) -> PlanNode {
        PlanNode {
            node_type: "Seq Scan".into(),
            relation_name: Some(table.into()),
            schema: Some("public".into()),
            alias: None,
            startup_cost: 0.0,
            total_cost: rows * 0.01,
            plan_rows: rows,
            plan_width: 64,
            actual_rows: None,
            actual_loops: None,
            actual_startup_time: None,
            actual_total_time: None,
            shared_hit_blocks: None,
            shared_read_blocks: None,
            index_name: None,
            index_cond: None,
            filter: Some("(id = 1)".into()),
            rows_removed_by_filter: None,
            sort_key: None,
            sort_method: None,
            hash_cond: None,
            join_type: None,
            subplans_removed: None,
            cte_name: None,
            parent_relationship: None,
            children: vec![],
        }
    }

    #[test]
    fn seq_scan_large_table() {
        let plan = make_seq_scan("users", 100_000.0);
        let warnings = detect_plan_warnings(&plan, None);
        assert!(
            warnings
                .iter()
                .any(|w| w.message.contains("sequential scan"))
        );
    }

    #[test]
    fn seq_scan_small_table_no_warning() {
        let plan = make_seq_scan("config", 10.0);
        let warnings = detect_plan_warnings(&plan, None);
        assert!(
            !warnings
                .iter()
                .any(|w| w.message.contains("sequential scan"))
        );
    }

    #[test]
    fn nested_loop_seq_scan_warning() {
        let outer = PlanNode {
            node_type: "Index Scan".into(),
            plan_rows: 1.0,
            total_cost: 8.0,
            ..make_seq_scan("users", 1.0)
        };
        let inner = make_seq_scan("orders", 50_000.0);
        let plan = PlanNode {
            node_type: "Nested Loop".into(),
            relation_name: None,
            schema: None,
            join_type: Some("Inner".into()),
            children: vec![outer, inner],
            ..make_seq_scan("", 100.0)
        };
        let warnings = detect_plan_warnings(&plan, None);
        assert!(warnings.iter().any(|w| w.message.contains("nested loop")));
    }

    #[test]
    fn sort_large_rows() {
        let mut plan = make_seq_scan("users", 50_000.0);
        plan.node_type = "Sort".into();
        plan.sort_key = Some(vec!["created_at".into()]);
        let warnings = detect_plan_warnings(&plan, None);
        assert!(warnings.iter().any(|w| w.message.contains("sort")));
    }

    fn partitioned_schema() -> SchemaSnapshot {
        use crate::schema::*;
        SchemaSnapshot {
            pg_version: "16.0".into(),
            database: "test".into(),
            timestamp: chrono::Utc::now(),
            content_hash: String::new(),
            source: None,
            tables: vec![Table {
                oid: 1,
                schema: "public".into(),
                name: "orders".into(),
                columns: vec![],
                constraints: vec![],
                indexes: vec![],
                comment: None,
                partition_info: Some(PartitionInfo {
                    strategy: PartitionStrategy::Range,
                    key: "created_at".into(),
                    children: vec![
                        PartitionChild {
                            schema: "public".into(),
                            name: "orders_q1".into(),
                            bound: "FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')".into(),
                        },
                        PartitionChild {
                            schema: "public".into(),
                            name: "orders_q2".into(),
                            bound: "FOR VALUES FROM ('2024-04-01') TO ('2024-07-01')".into(),
                        },
                        PartitionChild {
                            schema: "public".into(),
                            name: "orders_q3".into(),
                            bound: "FOR VALUES FROM ('2024-07-01') TO ('2024-10-01')".into(),
                        },
                        PartitionChild {
                            schema: "public".into(),
                            name: "orders_q4".into(),
                            bound: "FOR VALUES FROM ('2024-10-01') TO ('2025-01-01')".into(),
                        },
                    ],
                }),
                policies: vec![],
                triggers: vec![],
                reloptions: vec![],
                rls_enabled: false,
            }],
            enums: vec![],
            domains: vec![],
            composites: vec![],
            views: vec![],
            functions: vec![],
            extensions: vec![],
            gucs: vec![],
        }
    }

    // Wrap a bare schema in an empty annotated bundle — partition / CTE
    // tests don't need stats, just DDL.
    fn ddl_view(schema: &SchemaSnapshot) -> AnnotatedSchema<'_> {
        AnnotatedSchema {
            schema,
            planner: None,
            activity: None,
            merged: None,
        }
    }

    #[test]
    fn no_pruning_warns() {
        let schema = partitioned_schema();
        let view = ddl_view(&schema);
        // Append scanning all 4 partitions, no SubplansRemoved
        let plan = PlanNode {
            node_type: "Append".into(),
            children: vec![
                make_seq_scan("orders_q1", 1000.0),
                make_seq_scan("orders_q2", 1000.0),
                make_seq_scan("orders_q3", 1000.0),
                make_seq_scan("orders_q4", 1000.0),
            ],
            ..make_seq_scan("", 0.0)
        };
        let warnings = detect_plan_warnings(&plan, Some(&view));
        assert!(
            warnings
                .iter()
                .any(|w| w.message.contains("no pruning") && w.message.contains("4/4"))
        );
    }

    #[test]
    fn good_pruning_no_warning() {
        let schema = partitioned_schema();
        let view = ddl_view(&schema);
        // Only 1 partition scanned, 3 pruned
        let plan = PlanNode {
            node_type: "Append".into(),
            subplans_removed: Some(3),
            children: vec![make_seq_scan("orders_q1", 1000.0)],
            ..make_seq_scan("", 0.0)
        };
        let warnings = detect_plan_warnings(&plan, Some(&view));
        assert!(
            !warnings
                .iter()
                .any(|w| w.message.contains("partition pruning"))
        );
    }

    #[test]
    fn partial_pruning_info() {
        let schema = partitioned_schema();
        let view = ddl_view(&schema);
        // 3 partitions still scanned but 1 pruned — scanning > half
        let plan = PlanNode {
            node_type: "Append".into(),
            subplans_removed: Some(1),
            children: vec![
                make_seq_scan("orders_q1", 1000.0),
                make_seq_scan("orders_q2", 1000.0),
                make_seq_scan("orders_q3", 1000.0),
            ],
            ..make_seq_scan("", 0.0)
        };
        let warnings = detect_plan_warnings(&plan, Some(&view));
        assert!(
            warnings
                .iter()
                .any(|w| w.message.contains("partial pruning"))
        );
    }
}
