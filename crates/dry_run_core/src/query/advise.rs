use serde::{Deserialize, Serialize};

use super::plan::PlanNode;
use super::suggest::{self, IndexSuggestion};
use crate::error::Result;
use crate::jit;
use crate::schema::{self, AnnotatedSchema, ColumnStats, QualifiedName};
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

// Top-level advise pass — walks the plan tree and emits per-node advice.
//
// Takes the annotated view rather than a raw `&SchemaSnapshot` because
// the per-node refinements (selectivity hints, partial-index suggestions,
// per-replica seq_scan breakdown) all need planner column stats and
// activity counters. Without those, advise still works — it just
// degrades to "DDL-only" recommendations.
pub fn advise(
    plan: &PlanNode,
    annotated: &AnnotatedSchema<'_>,
    pg_version: Option<&PgVersion>,
) -> Vec<Advice> {
    let mut advice = Vec::new();
    walk_for_advice(plan, annotated, pg_version, &mut advice);
    advice
}

// Full advise pass: plan-based advice + optional index suggestions via static SQL analysis.
// Works without a live DB when `plan` is None — falls back to query-structure analysis only.
pub fn advise_with_index_suggestions(
    sql: &str,
    plan: Option<&PlanNode>,
    annotated: &AnnotatedSchema<'_>,
    pg_version: Option<&PgVersion>,
    include_index_suggestions: bool,
) -> Result<AdviseResult> {
    let advice = match plan {
        Some(p) => advise(p, annotated, pg_version),
        None => Vec::new(),
    };

    let index_suggestions = if include_index_suggestions {
        // suggest_index reads `reltuples` for size cutoffs — pass the
        // annotated view so it has access to planner sizing.
        suggest::suggest_index(sql, annotated, plan, pg_version)?
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
    annotated: &AnnotatedSchema<'_>,
    pg_version: Option<&PgVersion>,
    advice: &mut Vec<Advice>,
) {
    advise_seq_scan(node, annotated, pg_version, advice);
    advise_nested_loop_seq_scan(node, pg_version, advice);
    advise_sort(node, pg_version, advice);
    advise_cte(node, advice);

    for child in &node.children {
        walk_for_advice(child, annotated, pg_version, advice);
    }
}

fn advise_seq_scan(
    node: &PlanNode,
    annotated: &AnnotatedSchema<'_>,
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
    let qn = QualifiedName::new(schema_name, table_name);

    let table = annotated
        .schema
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
        // Column stats live in the planner snapshot, keyed by qualified
        // table name + column name. Returns None if there's no planner
        // capture yet — in which case we fall back to non-stats advice.
        let col_stats = annotated.column_stats(&qn, filter_col_name);

        let (idx_type, rec) = suggest_index_type(&qualified, col_type, filter_col_name);
        let mut recommendation = rec;

        // Stats-aware refinements — only meaningful when we actually have
        // column stats. The plan's row estimate is the floor; if planner
        // sizing reports more rows than the plan rows estimate (which can
        // happen on stale plan estimates), prefer the larger number.
        if col_stats.is_some() {
            let mut table_rows = node.plan_rows;
            if let Some(rt) = annotated.reltuples(&qn)
                && rt > table_rows
            {
                table_rows = rt;
            }
            recommendation.push_str(&stats_aware_advice(col_stats, filter_col_name, table_rows));
        }

        let idx_name = format!("idx_{table_name}_{filter_col_name}");

        // Prefer a partial index for high-null or skewed columns — a tiny
        // selective index is much cheaper than a full one when most rows
        // would never match the predicate. Falls through to a plain
        // CREATE INDEX when stats aren't available.
        let null_frac = col_stats.and_then(|s| s.null_frac).unwrap_or(0.0);
        let ddl = if null_frac > 0.5 {
            format!(
                "CREATE INDEX CONCURRENTLY {idx_name} ON {schema_name}.{table_name} USING {idx_type}({filter_col_name}) WHERE {filter_col_name} IS NOT NULL;"
            )
        } else if let Some(stats) = col_stats
            && let Some((dominant, _freq)) = schema::has_skewed_distribution(stats, 0.5)
        {
            format!(
                "CREATE INDEX CONCURRENTLY {idx_name} ON {schema_name}.{table_name} USING {idx_type}({filter_col_name}) WHERE {filter_col_name} != '{dominant}';"
            )
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

    // Per-node breakdown — surfaces "this replica is doing the unindexed
    // work, the others aren't" patterns. Empty when we only have one node
    // (or none); skipping the note in that case avoids noise.
    let per_node = annotated.seq_scan_per_node(&qn);
    if per_node.len() >= 2 {
        let total: i64 = per_node.iter().map(|(_, v)| *v).sum();
        let parts: Vec<String> = per_node
            .iter()
            .map(|(src, v)| format!("{src}: {v}"))
            .collect();
        full_recommendation.push_str(&format!(
            "\n\nNote: across {} nodes, seq_scan totals {} ({}). \
             Check if specific replicas are serving unindexed query patterns.",
            per_node.len(),
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

fn advise_sort(node: &PlanNode, pg_version: Option<&PgVersion>, advice: &mut Vec<Advice>) {
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

// Build a recommendation suffix grounded in column stats — selectivity,
// dominant-value skew, null fraction, physical correlation. Returns an
// empty string when no stats are available, which lets the caller stitch
// it on unconditionally without a `match`.
fn stats_aware_advice(stats: Option<&ColumnStats>, filter_col: &str, table_rows: f64) -> String {
    let stats = match stats {
        Some(s) => s,
        None => return String::new(),
    };
    let mut parts = Vec::new();

    // Selectivity — the fraction of rows a value-equality predicate is
    // expected to match. Low cardinality (≤ 5 distinct values) → high
    // selectivity → poor index usefulness; we call that out explicitly.
    let sel = schema::column_selectivity(Some(stats), table_rows);
    if let Some(nd) = stats.n_distinct {
        if nd > 0.0 && nd <= 5.0 {
            parts.push(format!(
                "\nColumn '{}' has only {:.0} distinct values, so a full index has poor selectivity ({:.0}% of rows per value).",
                filter_col, nd, sel * 100.0
            ));
        } else if nd > 0.0 && nd <= 20.0 {
            parts.push(format!(
                "\nColumn '{}' has {} distinct values (selectivity ~{:.1}%).",
                filter_col,
                nd as i64,
                sel * 100.0
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
        && nf > 0.5
    {
        let null_rows = (nf * table_rows) as i64;
        parts.push(format!(
                "Column is {:.0}% NULL (~{} rows). Use a partial index WHERE {} IS NOT NULL to index only the non-null rows.",
                nf * 100.0, null_rows, filter_col
            ));
    }

    // correlation warning for range scans
    if let Some(c) = stats.correlation
        && c > -0.3
        && c < 0.3
        && table_rows > 10_000.0
    {
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
    if ct.contains("geometry")
        || ct.contains("geography")
        || ct.contains("range")
        || ct == "tsrange"
        || ct == "daterange"
        || ct == "int4range"
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
    use std::collections::BTreeMap;

    use chrono::Utc;

    use super::*;
    use crate::schema::*;
    use crate::schema::{
        ActivityStatsSnapshot, AnnotatedSnapshot, IndexActivityEntry, NodeIdentity,
        PlannerStatsSnapshot, TableActivity, TableActivityEntry, TableSizing, TableSizingEntry,
    };

    fn empty_schema() -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "test".into(),
            timestamp: Utc::now(),
            content_hash: "test".into(),
            source: None,
            tables: vec![Table {
                oid: 1,
                schema: "public".into(),
                name: "orders".into(),
                columns: vec![
                    Column {
                        name: "id".into(),
                        ordinal: 1,
                        type_name: "bigint".into(),
                        nullable: false,
                        default: None,
                        identity: None,
                        generated: None,
                        comment: None,
                        statistics_target: None,
                    },
                    Column {
                        name: "customer_id".into(),
                        ordinal: 2,
                        type_name: "bigint".into(),
                        nullable: false,
                        default: None,
                        identity: None,
                        generated: None,
                        comment: None,
                        statistics_target: None,
                    },
                    Column {
                        name: "data".into(),
                        ordinal: 3,
                        type_name: "jsonb".into(),
                        nullable: true,
                        default: None,
                        identity: None,
                        generated: None,
                        comment: None,
                        statistics_target: None,
                    },
                ],
                constraints: vec![],
                indexes: vec![],
                comment: None,
                partition_info: None,
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

    fn make_seq_scan(table: &str, rows: f64, filter: Option<&str>) -> PlanNode {
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
            filter: filter.map(String::from),
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

    // Wrap a bare schema in an empty annotated bundle — no planner, no
    // activity. Mirrors what the MCP server hands tool bodies before
    // any `dryrun snapshot take` has run.
    fn ddl_only(schema: SchemaSnapshot) -> AnnotatedSnapshot {
        AnnotatedSnapshot {
            schema,
            planner: None,
            activity_by_node: BTreeMap::new(),
        }
    }

    #[test]
    fn advise_seq_scan_suggests_btree() {
        let snap = ddl_only(empty_schema());
        let plan = make_seq_scan("orders", 100_000.0, Some("(customer_id = 42)"));
        let advice = advise(&plan, &snap.view(None), None);
        assert!(!advice.is_empty());
        assert!(advice[0].ddl.as_ref().unwrap().contains("btree"));
        assert!(advice[0].ddl.as_ref().unwrap().contains("customer_id"));
        assert!(advice[0].ddl.as_ref().unwrap().contains("CONCURRENTLY"));
    }

    #[test]
    fn advise_seq_scan_jsonb_suggests_gin() {
        let snap = ddl_only(empty_schema());
        let plan = make_seq_scan("orders", 100_000.0, Some("(data @> '{}'::jsonb)"));
        let advice = advise(&plan, &snap.view(None), None);
        assert!(!advice.is_empty());
        assert!(advice[0].ddl.as_ref().unwrap().contains("gin"));
    }

    #[test]
    fn advise_small_table_no_advice() {
        let snap = ddl_only(empty_schema());
        let plan = make_seq_scan("orders", 50.0, Some("(id = 1)"));
        let advice = advise(&plan, &snap.view(None), None);
        assert!(advice.is_empty());
    }

    #[test]
    fn advise_includes_version_note() {
        let snap = ddl_only(empty_schema());
        let plan = make_seq_scan("orders", 100_000.0, Some("(customer_id = 42)"));
        let pg14 = PgVersion {
            major: 14,
            minor: 0,
            patch: 0,
        };
        let advice = advise(&plan, &snap.view(None), Some(&pg14));
        assert!(!advice.is_empty());
        assert!(advice[0].version_note.is_some());
    }

    // Helper: build an ActivityStatsSnapshot for one node with a single
    // table activity row carrying the supplied seq_scan counter.
    fn activity_for(label: &str, seq_scan: i64) -> ActivityStatsSnapshot {
        ActivityStatsSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "test".into(),
            timestamp: Utc::now(),
            content_hash: format!("h-{label}"),
            schema_ref_hash: "sh".into(),
            node: NodeIdentity {
                label: label.into(),
                host: label.into(),
                is_standby: label != "master",
                replication_lag_bytes: None,
                stats_reset: None,
            },
            tables: vec![TableActivityEntry {
                table: QualifiedName::new("public", "orders"),
                activity: TableActivity {
                    seq_scan,
                    idx_scan: 0,
                    n_live_tup: 0,
                    n_dead_tup: 0,
                    last_vacuum: None,
                    last_autovacuum: None,
                    last_analyze: None,
                    last_autoanalyze: None,
                    vacuum_count: 0,
                    autovacuum_count: 0,
                    analyze_count: 0,
                    autoanalyze_count: 0,
                },
            }],
            indexes: Vec::<IndexActivityEntry>::new(),
        }
    }

    #[test]
    fn advise_seq_scan_includes_node_context() {
        // Two-node cluster — primary handles indexed traffic, replica
        // is doing the seq scans. The recommendation should call that
        // out with the per-node breakdown.
        let mut activity_by_node = BTreeMap::new();
        activity_by_node.insert("master".into(), activity_for("master", 100));
        activity_by_node.insert("replica-1".into(), activity_for("replica-1", 42000));
        let snap = AnnotatedSnapshot {
            schema: empty_schema(),
            planner: Some(PlannerStatsSnapshot {
                pg_version: "PostgreSQL 17.0".into(),
                database: "test".into(),
                timestamp: Utc::now(),
                content_hash: "ph".into(),
                schema_ref_hash: "sh".into(),
                tables: vec![TableSizingEntry {
                    table: QualifiedName::new("public", "orders"),
                    sizing: TableSizing {
                        reltuples: 100_000.0,
                        relpages: 1250,
                        table_size: 10_000_000,
                        total_size: None,
                        index_size: None,
                    },
                }],
                columns: vec![],
                indexes: vec![],
            }),
            activity_by_node,
        };
        let plan = make_seq_scan("orders", 100_000.0, Some("(customer_id = 42)"));
        let advice = advise(&plan, &snap.view(None), None);
        assert!(!advice.is_empty());
        assert!(advice[0].recommendation.contains("across 2 nodes"));
        assert!(advice[0].recommendation.contains("master: 100"));
        assert!(advice[0].recommendation.contains("replica-1: 42000"));
    }

    #[test]
    fn extract_column_simple() {
        assert_eq!(
            extract_column_from_filter("(customer_id = 42)"),
            Some("customer_id".into())
        );
        assert_eq!(
            extract_column_from_filter("(status IS NOT NULL)"),
            Some("status".into())
        );
        assert_eq!(
            extract_column_from_filter("(t.name = 'foo')"),
            Some("name".into())
        );
    }
}
