use serde::{Deserialize, Serialize};

use super::plan::PlanNode;
use super::suggest::{self, IndexSuggestion};
use crate::error::Result;
use crate::jit;
use crate::schema::{self, Column, SchemaSnapshot};
use crate::version::PgVersion;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Advice {
    pub issue: String,
    pub severity: String,
    pub table: Option<String>,
    pub recommendation: String,
    pub ddl: Option<String>,
    pub version_note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdviseResult {
    pub advice: Vec<Advice>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub index_suggestions: Vec<IndexSuggestion>,
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

// Full advise pass: plan-based advice + optional index suggestions via static SQL analysis.
// Works without a live DB when `plan` is None — falls back to query-structure analysis only.
pub fn advise_with_index_suggestions(
    sql: &str,
    plan: Option<&PlanNode>,
    schema: &SchemaSnapshot,
    pg_version: Option<&PgVersion>,
    include_index_suggestions: bool,
) -> Result<AdviseResult> {
    let advice = match plan {
        Some(p) => advise(p, schema, pg_version),
        None => Vec::new(),
    };

    let index_suggestions = if include_index_suggestions {
        suggest::suggest_index(sql, schema, plan, pg_version)?
    } else {
        Vec::new()
    };

    Ok(AdviseResult {
        advice,
        index_suggestions,
    })
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
    advise_cte(node, advice);

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
            version_note: None,
        });
        return;
    }

    let (ddl, recommendation) = if let Some(filter_col_name) = &filter_col {
        let col_obj = table.and_then(|t| t.columns.iter().find(|c| c.name == *filter_col_name));
        let col_type = col_obj.map(|c| c.type_name.as_str()).unwrap_or("unknown");

        let (idx_type, rec) = suggest_index_type(&qualified, col_type, filter_col_name);
        let mut recommendation = rec;

        // stats-aware refinements
        if let Some(col) = col_obj
            && col.stats.is_some() {
                let mut table_rows = node.plan_rows;
                if let Some(t) = table
                    && let Some(s) = &t.stats
                        && s.reltuples > table_rows {
                            table_rows = s.reltuples;
                        }
                recommendation.push_str(&stats_aware_advice(col, filter_col_name, table_rows));
            }

        let idx_name = format!("idx_{table_name}_{filter_col_name}");

        // prefer partial index for high-null or skewed columns
        let ddl = if let Some(col) = col_obj {
            if col.stats.as_ref().and_then(|s| s.null_frac).unwrap_or(0.0) > 0.5 {
                format!(
                    "CREATE INDEX CONCURRENTLY {idx_name} ON {schema_name}.{table_name} USING {idx_type}({filter_col_name}) WHERE {filter_col_name} IS NOT NULL;"
                )
            } else if let Some(stats) = &col.stats {
                if let Some((dominant, _freq)) = schema::has_skewed_distribution(stats, 0.5) {
                    format!(
                        "CREATE INDEX CONCURRENTLY {idx_name} ON {schema_name}.{table_name} USING {idx_type}({filter_col_name}) WHERE {filter_col_name} != '{dominant}';"
                    )
                } else {
                    format!(
                        "CREATE INDEX CONCURRENTLY {idx_name} ON {schema_name}.{table_name} USING {idx_type}({filter_col_name});"
                    )
                }
            } else {
                format!(
                    "CREATE INDEX CONCURRENTLY {idx_name} ON {schema_name}.{table_name} USING {idx_type}({filter_col_name});"
                )
            }
        } else {
            format!(
                "CREATE INDEX CONCURRENTLY {idx_name} ON {schema_name}.{table_name} USING {idx_type}({filter_col_name});"
            )
        };

        (Some(ddl), recommendation)
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
        version_note: version_note_for_index(pg_version),
    });
}

fn stats_aware_advice(col: &Column, filter_col: &str, table_rows: f64) -> String {
    let stats = match &col.stats {
        Some(s) => s,
        None => return String::new(),
    };
    let mut parts = Vec::new();

    // selectivity assessment
    let sel = schema::column_selectivity(col, table_rows);
    if let Some(nd) = stats.n_distinct {
        if nd > 0.0 && nd <= 5.0 {
            parts.push(format!(
                "\nColumn '{}' has only {:.0} distinct values, so a full index has poor selectivity ({:.0}% of rows per value).",
                filter_col, nd, sel * 100.0
            ));
        } else if nd > 0.0 && nd <= 20.0 {
            parts.push(format!(
                "\nColumn '{}' has {} distinct values (selectivity ~{:.1}%).",
                filter_col, nd as i64, sel * 100.0
            ));
        }
    }

    // skew detection
    if let Some((dominant, freq)) = schema::has_skewed_distribution(stats, 0.5) {
        parts.push(format!(
            "Value '{}' dominates at ~{:.0}%. A partial index excluding it would be much smaller and faster.",
            dominant, freq * 100.0
        ));
    }

    // high null fraction
    if let Some(nf) = stats.null_frac
        && nf > 0.5 {
            let null_rows = (nf * table_rows) as i64;
            parts.push(format!(
                "Column is {:.0}% NULL (~{} rows). Use a partial index WHERE {} IS NOT NULL to index only the non-null rows.",
                nf * 100.0, null_rows, filter_col
            ));
        }

    // correlation warning for range scans
    if let Some(c) = stats.correlation
        && c > -0.3 && c < 0.3 && table_rows > 10_000.0 {
            parts.push(format!(
                "Physical ordering is random (correlation: {:.2}); index range scans will cause random I/O.",
                c
            ));
        }

    parts.join(" ")
}

fn advise_cte(node: &PlanNode, advice: &mut Vec<Advice>) {
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
    let e = jit::cte_materialized(cte_name, rows);
    advice.push(Advice {
        issue: format!("materialized CTE '{cte_name}' (~{rows} rows)"),
        severity: "info".into(),
        table: None,
        recommendation: format!("{}\n{}", e.reason, e.fix),
        ddl: None,
        version_note: None,
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

fn suggest_index_type(table: &str, col_type: &str, col_name: &str) -> (&'static str, String) {
    let ct = col_type.to_lowercase();
    if ct == "jsonb" || ct == "tsvector" {
        let e = jit::suggest_gin(table, col_name, col_type);
        let rec = match &e.note {
            Some(note) => format!("{}\n{note}", e.reason),
            None => e.reason,
        };
        return ("gin", rec);
    }
    if ct.contains("geometry") || ct.contains("geography") || ct.contains("range")
        || ct == "tsrange" || ct == "daterange" || ct == "int4range"
    {
        let e = jit::suggest_gist(table, col_name, col_type);
        return ("gist", e.reason);
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
                    Column { name: "id".into(), ordinal: 1, type_name: "bigint".into(), nullable: false, default: None, identity: None, generated: None, comment: None, statistics_target: None, stats: None },
                    Column { name: "customer_id".into(), ordinal: 2, type_name: "bigint".into(), nullable: false, default: None, identity: None, generated: None, comment: None, statistics_target: None, stats: None },
                    Column { name: "data".into(), ordinal: 3, type_name: "jsonb".into(), nullable: true, default: None, identity: None, generated: None, comment: None, statistics_target: None, stats: None },
                ],
                constraints: vec![], indexes: vec![], comment: None, stats: None,
                partition_info: None, policies: vec![], triggers: vec![], reloptions: vec![], rls_enabled: false,
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
            sort_key: None, sort_method: None, hash_cond: None, join_type: None, subplans_removed: None, cte_name: None, parent_relationship: None, children: vec![],
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
                is_standby: false,
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
                is_standby: true,
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
