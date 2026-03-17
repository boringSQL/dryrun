use serde::{Deserialize, Serialize};

use super::plan::PlanNode;
use crate::knowledge;
use crate::schema::SchemaSnapshot;
use crate::version::PgVersion;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Advice {
    pub issue: String,
    pub severity: String,
    pub table: Option<String>,
    pub recommendation: String,
    pub ddl: Option<String>,
    pub knowledge_doc: Option<String>,
    pub version_note: Option<String>,
}

pub fn advise(
    plan: &PlanNode,
    schema: &SchemaSnapshot,
    pg_version: Option<&PgVersion>,
) -> Vec<Advice> {
    let mut advice = Vec::new();
    walk_for_advice(plan, schema, pg_version, &mut advice);
    advice
}

fn walk_for_advice(
    node: &PlanNode,
    schema: &SchemaSnapshot,
    pg_version: Option<&PgVersion>,
    advice: &mut Vec<Advice>,
) {
    advise_seq_scan(node, schema, pg_version, advice);
    advise_nested_loop_seq_scan(node, pg_version, advice);
    advise_sort(node, schema, pg_version, advice);

    for child in &node.children {
        walk_for_advice(child, schema, pg_version, advice);
    }
}

fn advise_seq_scan(
    node: &PlanNode,
    schema: &SchemaSnapshot,
    pg_version: Option<&PgVersion>,
    advice: &mut Vec<Advice>,
) {
    if node.node_type != "Seq Scan" {
        return;
    }
    let table_name = match &node.relation_name {
        Some(n) => n,
        None => return,
    };
    if node.plan_rows < 10_000.0 {
        return;
    }

    let schema_name = node.schema.as_deref().unwrap_or("public");
    let qualified = format!("{schema_name}.{table_name}");

    let table = schema
        .tables
        .iter()
        .find(|t| t.name == *table_name && t.schema == schema_name);

    let filter_col = node
        .filter
        .as_ref()
        .and_then(|f| extract_column_from_filter(f));

    let has_index = if let (Some(table), Some(col)) = (&table, &filter_col) {
        table
            .indexes
            .iter()
            .any(|idx| idx.columns.first().map(|c| c.as_str()) == Some(col.as_str()))
    } else {
        false
    };

    if has_index {
        advice.push(Advice {
            issue: format!(
                "sequential scan on '{qualified}' (~{} rows) despite existing index",
                node.plan_rows as i64
            ),
            severity: "info".into(),
            table: Some(qualified),
            recommendation:
                "Run ANALYZE to update statistics. The planner may correctly prefer a seq scan if selectivity is low."
                    .into(),
            ddl: Some(format!("ANALYZE {schema_name}.{table_name};")),
            knowledge_doc: None,
            version_note: None,
        });
        return;
    }

    let index_docs = knowledge::lookup_index_decisions("btree index", pg_version);
    let doc_ref = index_docs.first().map(|d| d.name.clone());

    let (ddl, recommendation) = if let Some(col) = &filter_col {
        let col_type = table
            .and_then(|t| t.columns.iter().find(|c| c.name == *col))
            .map(|c| c.type_name.as_str())
            .unwrap_or("unknown");

        let (idx_type, rec) = suggest_index_type(col_type, col);
        let idx_name = format!("idx_{table_name}_{col}");
        let ddl = format!(
            "CREATE INDEX CONCURRENTLY {idx_name} ON {schema_name}.{table_name} USING {idx_type}({col});"
        );
        (Some(ddl), rec)
    } else {
        (
            None,
            "Add an index on the filtered column(s) to avoid sequential scan.".into(),
        )
    };

    let mut full_recommendation = recommendation;

    // enrich with per-node context when available
    let node_seq_scans: Vec<(&str, i64)> = schema
        .node_stats
        .iter()
        .filter_map(|ns| {
            ns.table_stats
                .iter()
                .find(|ts| ts.table == *table_name && ts.schema == schema_name)
                .map(|ts| (ns.source.as_str(), ts.stats.seq_scan))
        })
        .collect();

    if node_seq_scans.len() >= 2 {
        let total: i64 = node_seq_scans.iter().map(|(_, v)| *v).sum();
        let parts: Vec<String> = node_seq_scans
            .iter()
            .map(|(src, v)| format!("{src}: {v}"))
            .collect();
        full_recommendation.push_str(&format!(
            "\n\nNote: across {} nodes, seq_scan totals {} ({}). \
             Check if specific replicas are serving unindexed query patterns.",
            node_seq_scans.len(),
            total,
            parts.join(", ")
        ));
    }

    advice.push(Advice {
        issue: format!(
            "sequential scan on '{qualified}' (~{} rows)",
            node.plan_rows as i64
        ),
        severity: "warning".into(),
        table: Some(qualified),
        recommendation: full_recommendation,
        ddl,
        knowledge_doc: doc_ref,
        version_note: version_note_for_index(pg_version),
    });
}

fn advise_nested_loop_seq_scan(
    node: &PlanNode,
    pg_version: Option<&PgVersion>,
    advice: &mut Vec<Advice>,
) {
    if node.node_type != "Nested Loop" {
        return;
    }

    let inner = match node.children.get(1) {
        Some(child) if child.node_type == "Seq Scan" && child.plan_rows > 100.0 => child,
        _ => return,
    };

    let table_name = inner.relation_name.as_deref().unwrap_or("unknown");
    let schema_name = inner.schema.as_deref().unwrap_or("public");
    let qualified = format!("{schema_name}.{table_name}");

    let filter_col = inner
        .filter
        .as_ref()
        .and_then(|f| extract_column_from_filter(f));

    let ddl = filter_col.as_ref().map(|col| {
        format!(
            "CREATE INDEX CONCURRENTLY idx_{table_name}_{col} ON {schema_name}.{table_name}({col});"
        )
    });

    advice.push(Advice {
        issue: format!(
            "nested loop with sequential scan on inner side '{qualified}' (~{} rows per loop)",
            inner.plan_rows as i64
        ),
        severity: "warning".into(),
        table: Some(qualified),
        recommendation:
            "Add an index on the join/filter column of the inner table to convert the seq scan to an index scan."
                .into(),
        ddl,
        knowledge_doc: Some("btree".into()),
        version_note: version_note_for_index(pg_version),
    });
}

fn advise_sort(
    node: &PlanNode,
    _schema: &SchemaSnapshot,
    pg_version: Option<&PgVersion>,
    advice: &mut Vec<Advice>,
) {
    if node.node_type != "Sort" || node.plan_rows < 10_000.0 {
        return;
    }

    let sort_keys = match &node.sort_key {
        Some(keys) if !keys.is_empty() => keys,
        _ => return,
    };

    let table_info = find_table_in_subtree(node);
    let (schema_name, table_name) = match &table_info {
        Some((s, t)) => (s.as_str(), t.as_str()),
        None => return,
    };
    let qualified = format!("{schema_name}.{table_name}");

    let first_key = sort_keys[0]
        .split_whitespace()
        .next()
        .unwrap_or(&sort_keys[0]);

    let ddl = format!(
        "CREATE INDEX CONCURRENTLY idx_{table_name}_{first_key} ON {schema_name}.{table_name}({});",
        sort_keys.join(", ")
    );

    advice.push(Advice {
        issue: format!(
            "sort on ~{} rows (keys: {})",
            node.plan_rows as i64,
            sort_keys.join(", ")
        ),
        severity: "info".into(),
        table: Some(qualified),
        recommendation: "Consider an index matching the sort order to avoid an explicit sort step."
            .into(),
        ddl: Some(ddl),
        knowledge_doc: Some("btree".into()),
        version_note: version_note_for_index(pg_version),
    });
}

// helpers

fn extract_column_from_filter(filter: &str) -> Option<String> {
    let trimmed = filter.trim().trim_start_matches('(').trim_end_matches(')');
    let first_token = trimmed.split_whitespace().next()?;
    let col = first_token.rsplit('.').next().unwrap_or(first_token);
    if col.chars().all(|c| c.is_alphanumeric() || c == '_') && !col.is_empty() {
        Some(col.to_string())
    } else {
        None
    }
}

fn suggest_index_type(col_type: &str, col_name: &str) -> (&'static str, String) {
    let ct = col_type.to_lowercase();
    if ct == "jsonb" {
        return (
            "gin",
            format!("Use GIN index for JSONB column '{col_name}' — supports @>, ?, ?& operators."),
        );
    }
    if ct == "tsvector" {
        return (
            "gin",
            format!("Use GIN index for full-text search column '{col_name}'."),
        );
    }
    if ct.contains("geometry") || ct.contains("geography") {
        return (
            "gist",
            format!("Use GiST index for spatial column '{col_name}'."),
        );
    }
    if ct.contains("range") || ct == "tsrange" || ct == "daterange" || ct == "int4range" {
        return (
            "gist",
            format!("Use GiST index for range column '{col_name}' — supports overlap (&&) and containment."),
        );
    }
    (
        "btree",
        format!("Add a B-tree index on '{col_name}' for equality/range lookups."),
    )
}

fn version_note_for_index(pg_version: Option<&PgVersion>) -> Option<String> {
    let ver = pg_version?;
    if ver.major >= 13 {
        Some("PG 13+: B-tree deduplication is enabled by default, reducing index size for low-cardinality columns.".into())
    } else if ver.major >= 11 {
        Some("PG 11+: Use INCLUDE for covering indexes to enable index-only scans.".into())
    } else {
        None
    }
}

fn find_table_in_subtree(node: &PlanNode) -> Option<(String, String)> {
    if let (Some(schema), Some(table)) = (&node.schema, &node.relation_name) {
        return Some((schema.clone(), table.clone()));
    }
    for child in &node.children {
        if let Some(result) = find_table_in_subtree(child) {
            return Some(result);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::schema::*;

    fn empty_schema() -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "test".into(),
            timestamp: Utc::now(),
            content_hash: "test".into(),
            source: None,
            tables: vec![Table {
                oid: 1, schema: "public".into(), name: "orders".into(),
                columns: vec![
                    Column { name: "id".into(), ordinal: 1, type_name: "bigint".into(), nullable: false, default: None, identity: None, comment: None, stats: None },
                    Column { name: "customer_id".into(), ordinal: 2, type_name: "bigint".into(), nullable: false, default: None, identity: None, comment: None, stats: None },
                    Column { name: "data".into(), ordinal: 3, type_name: "jsonb".into(), nullable: true, default: None, identity: None, comment: None, stats: None },
                ],
                constraints: vec![], indexes: vec![], comment: None, stats: None,
                partition_info: None, policies: vec![], triggers: vec![], rls_enabled: false,
            }],
            enums: vec![], domains: vec![], composites: vec![], views: vec![], functions: vec![], extensions: vec![], gucs: vec![],
            node_stats: vec![],
        }
    }

    fn make_seq_scan(table: &str, rows: f64, filter: Option<&str>) -> PlanNode {
        PlanNode {
            node_type: "Seq Scan".into(), relation_name: Some(table.into()), schema: Some("public".into()),
            alias: None, startup_cost: 0.0, total_cost: rows * 0.01, plan_rows: rows, plan_width: 64,
            actual_rows: None, actual_loops: None, actual_startup_time: None, actual_total_time: None,
            shared_hit_blocks: None, shared_read_blocks: None, index_name: None, index_cond: None,
            filter: filter.map(String::from), rows_removed_by_filter: None,
            sort_key: None, sort_method: None, hash_cond: None, join_type: None, children: vec![],
        }
    }

    #[test]
    fn advise_seq_scan_suggests_btree() {
        let schema = empty_schema();
        let plan = make_seq_scan("orders", 100_000.0, Some("(customer_id = 42)"));
        let advice = advise(&plan, &schema, None);
        assert!(!advice.is_empty());
        assert!(advice[0].ddl.as_ref().unwrap().contains("btree"));
        assert!(advice[0].ddl.as_ref().unwrap().contains("customer_id"));
        assert!(advice[0].ddl.as_ref().unwrap().contains("CONCURRENTLY"));
    }

    #[test]
    fn advise_seq_scan_jsonb_suggests_gin() {
        let schema = empty_schema();
        let plan = make_seq_scan("orders", 100_000.0, Some("(data @> '{}'::jsonb)"));
        let advice = advise(&plan, &schema, None);
        assert!(!advice.is_empty());
        assert!(advice[0].ddl.as_ref().unwrap().contains("gin"));
    }

    #[test]
    fn advise_small_table_no_advice() {
        let schema = empty_schema();
        let plan = make_seq_scan("orders", 50.0, Some("(id = 1)"));
        let advice = advise(&plan, &schema, None);
        assert!(advice.is_empty());
    }

    #[test]
    fn advise_includes_version_note() {
        let schema = empty_schema();
        let plan = make_seq_scan("orders", 100_000.0, Some("(customer_id = 42)"));
        let pg14 = PgVersion { major: 14, minor: 0, patch: 0 };
        let advice = advise(&plan, &schema, Some(&pg14));
        assert!(!advice.is_empty());
        assert!(advice[0].version_note.is_some());
    }

    #[test]
    fn advise_seq_scan_includes_node_context() {
        let mut schema = empty_schema();
        schema.node_stats = vec![
            NodeStats {
                source: "master".into(),
                timestamp: Utc::now(),
                table_stats: vec![NodeTableStats {
                    schema: "public".into(),
                    table: "orders".into(),
                    stats: TableStats {
                        reltuples: 100_000.0, relpages: 1250, dead_tuples: 0,
                        last_vacuum: None, last_autovacuum: None,
                        last_analyze: None, last_autoanalyze: None,
                        seq_scan: 100, idx_scan: 5000, table_size: 10_000_000,
                    },
                }],
                index_stats: vec![],
                column_stats: vec![],
            },
            NodeStats {
                source: "replica-1".into(),
                timestamp: Utc::now(),
                table_stats: vec![NodeTableStats {
                    schema: "public".into(),
                    table: "orders".into(),
                    stats: TableStats {
                        reltuples: 100_000.0, relpages: 1250, dead_tuples: 0,
                        last_vacuum: None, last_autovacuum: None,
                        last_analyze: None, last_autoanalyze: None,
                        seq_scan: 42000, idx_scan: 1000, table_size: 10_000_000,
                    },
                }],
                index_stats: vec![],
                column_stats: vec![],
            },
        ];
        let plan = make_seq_scan("orders", 100_000.0, Some("(customer_id = 42)"));
        let advice = advise(&plan, &schema, None);
        assert!(!advice.is_empty());
        assert!(advice[0].recommendation.contains("across 2 nodes"));
        assert!(advice[0].recommendation.contains("master: 100"));
        assert!(advice[0].recommendation.contains("replica-1: 42000"));
    }

    #[test]
    fn extract_column_simple() {
        assert_eq!(extract_column_from_filter("(customer_id = 42)"), Some("customer_id".into()));
        assert_eq!(extract_column_from_filter("(status IS NOT NULL)"), Some("status".into()));
        assert_eq!(extract_column_from_filter("(t.name = 'foo')"), Some("name".into()));
    }
}
