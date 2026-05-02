use serde::{Deserialize, Serialize};

use super::parse::parse_sql;
use super::plan::PlanNode;
use crate::error::Result;
use crate::schema::{AnnotatedSchema, QualifiedName, Table};
use crate::version::PgVersion;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSuggestion {
    pub table: String,
    pub index_type: String,
    pub columns: Vec<String>,
    pub include_columns: Vec<String>,
    pub partial_predicate: Option<String>,
    pub ddl: String,
    pub rationale: String,
    pub estimated_impact: String,
}

pub(crate) fn suggest_index(
    sql: &str,
    annotated: &AnnotatedSchema<'_>,
    plan: Option<&PlanNode>,
    _pg_version: Option<&PgVersion>,
) -> Result<Vec<IndexSuggestion>> {
    let parsed = parse_sql(sql)?;
    let mut suggestions = Vec::new();

    if let Some(plan) = plan {
        suggest_from_plan(plan, annotated, &mut suggestions);
    }

    suggest_from_query_structure(&parsed, annotated, &mut suggestions);
    dedup_suggestions(&mut suggestions);

    Ok(suggestions)
}

// Plan-based suggestions — walks an EXPLAIN plan tree looking for
// patterns that an index could fix. Reads only DDL plus reltuples (for
// the "is this table large enough to bother" cutoff).
fn suggest_from_plan(
    node: &PlanNode,
    annotated: &AnnotatedSchema<'_>,
    suggestions: &mut Vec<IndexSuggestion>,
) {
    if node.node_type == "Seq Scan"
        && node.plan_rows >= 1000.0
        && let Some(table_name) = &node.relation_name
    {
        let schema_name = node.schema.as_deref().unwrap_or("public");
        let table = annotated
            .schema
            .tables
            .iter()
            .find(|t| t.name == *table_name && t.schema == schema_name);

        if let Some(filter) = &node.filter
            && let Some(col) = extract_filter_column(filter)
            && !has_leading_index(table, &col)
        {
            let idx_type = choose_index_type(table, &col);
            let qualified = format!("{schema_name}.{table_name}");
            let idx_name = format!("idx_{table_name}_{col}");
            suggestions.push(IndexSuggestion {
                table: qualified.clone(),
                index_type: idx_type.to_string(),
                columns: vec![col.clone()],
                include_columns: vec![],
                partial_predicate: None,
                ddl: format!(
                    "CREATE INDEX CONCURRENTLY {idx_name} ON {qualified} USING {idx_type}({col});"
                ),
                rationale: format!(
                    "Seq scan on '{qualified}' filtering on '{col}' (~{} rows)",
                    node.plan_rows as i64
                ),
                estimated_impact: estimate_impact(node.plan_rows),
            });
        }
    }

    if node.node_type == "Sort"
        && node.plan_rows >= 5000.0
        && let Some(sort_keys) = &node.sort_key
        && let Some((schema_name, table_name)) = find_table_in_subtree(node)
    {
        let cols: Vec<String> = sort_keys
            .iter()
            .map(|k| k.split_whitespace().next().unwrap_or(k).to_string())
            .collect();
        let qualified = format!("{schema_name}.{table_name}");
        let col_list = cols.join(", ");
        let idx_name = format!(
            "idx_{table_name}_{}",
            cols.first().unwrap_or(&"sort".into())
        );

        suggestions.push(IndexSuggestion {
            table: qualified.clone(),
            index_type: "btree".into(),
            columns: cols,
            include_columns: vec![],
            partial_predicate: None,
            ddl: format!("CREATE INDEX CONCURRENTLY {idx_name} ON {qualified}({col_list});"),
            rationale: format!(
                "Sort on ~{} rows could be avoided with an index on ({})",
                node.plan_rows as i64, col_list
            ),
            estimated_impact: "eliminates sort step".into(),
        });
    }

    for child in &node.children {
        suggest_from_plan(child, annotated, suggestions);
    }
}

// Query-structure-based suggestions — uses the parsed SQL to spot
// WHERE-clause filter columns on large tables that lack a leading index.
//
// "Large" is gated on planner reltuples; tables under the threshold or
// without any planner snapshot at all are silently skipped — there's no
// useful suggestion to make in those cases.
fn suggest_from_query_structure(
    parsed: &super::parse::ParsedQuery,
    annotated: &AnnotatedSchema<'_>,
    suggestions: &mut Vec<IndexSuggestion>,
) {
    for (alias, col_name) in &parsed.info.filter_columns {
        let table_ref = if let Some(alias) = alias {
            parsed
                .info
                .tables
                .iter()
                .find(|t| t.alias.as_deref() == Some(alias.as_str()) || t.name == *alias)
        } else if parsed.info.tables.len() == 1 {
            parsed.info.tables.first()
        } else {
            None
        };

        if let Some(table_ref) = table_ref {
            let schema_name = table_ref.schema.as_deref().unwrap_or("public");
            let table = annotated
                .schema
                .tables
                .iter()
                .find(|t| t.name == table_ref.name && t.schema == schema_name);

            if let Some(table) = table {
                let qn = QualifiedName::new(&table.schema, &table.name);
                // Reltuples is the only stat this rule needs — comes
                // from the planner snapshot (always None on a fresh
                // project, in which case we skip).
                let reltuples = annotated.reltuples(&qn).unwrap_or(0.0);
                let is_large = reltuples >= 1000.0;

                if is_large && !has_leading_index(Some(table), col_name) {
                    let idx_type = choose_index_type(Some(table), col_name);
                    let qualified = format!("{}.{}", table.schema, table.name);
                    let idx_name = format!("idx_{}_{col_name}", table.name);

                    suggestions.push(IndexSuggestion {
                        table: qualified.clone(),
                        index_type: idx_type.to_string(),
                        columns: vec![col_name.clone()],
                        include_columns: vec![],
                        partial_predicate: None,
                        ddl: format!(
                            "CREATE INDEX CONCURRENTLY {idx_name} ON {qualified} USING {idx_type}({col_name});"
                        ),
                        rationale: format!(
                            "WHERE clause filters on '{col_name}' on table '{qualified}' (~{} rows)",
                            reltuples as i64
                        ),
                        estimated_impact: estimate_impact(reltuples),
                    });
                }
            }
        }
    }
}

// helpers

fn extract_filter_column(filter: &str) -> Option<String> {
    let trimmed = filter.trim().trim_start_matches('(').trim_end_matches(')');
    let first_token = trimmed.split_whitespace().next()?;
    let col = first_token.rsplit('.').next().unwrap_or(first_token);
    if col.chars().all(|c| c.is_alphanumeric() || c == '_') && !col.is_empty() {
        Some(col.to_string())
    } else {
        None
    }
}

fn has_leading_index(table: Option<&Table>, col: &str) -> bool {
    table.is_some_and(|t| {
        t.indexes
            .iter()
            .any(|idx| idx.columns.first().is_some_and(|c| c == col))
    })
}

fn choose_index_type<'a>(table: Option<&Table>, col: &str) -> &'a str {
    if let Some(table) = table
        && let Some(column) = table.columns.iter().find(|c| c.name == col)
    {
        let ct = column.type_name.to_lowercase();
        if ct == "jsonb" || ct == "tsvector" {
            return "gin";
        }
        if ct.contains("geometry") || ct.contains("geography") || ct.contains("range") {
            return "gist";
        }
    }
    "btree"
}

fn estimate_impact(row_count: f64) -> String {
    if row_count >= 1_000_000.0 {
        "high — large table, index likely reduces query time significantly".into()
    } else if row_count >= 10_000.0 {
        "medium — moderate table size, index should help".into()
    } else {
        "low — small table, index may or may not help".into()
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

fn dedup_suggestions(suggestions: &mut Vec<IndexSuggestion>) {
    let mut seen = std::collections::HashSet::new();
    suggestions.retain(|s| {
        let key = format!("{}:{}", s.table, s.columns.join(","));
        seen.insert(key)
    });
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use std::collections::BTreeMap;

    use super::*;
    use crate::schema::*;
    use crate::schema::{AnnotatedSnapshot, PlannerStatsSnapshot, TableSizing, TableSizingEntry};

    // Build a stats-bearing AnnotatedSnapshot — wraps the legacy
    // `test_schema()` fixture and bolts on a planner snapshot with the
    // reltuples each test relies on. `with_size` lets the small-table
    // case override the row count without hand-rolling another schema.
    fn test_annotated(reltuples: f64) -> AnnotatedSnapshot {
        AnnotatedSnapshot {
            schema: test_schema(),
            planner: Some(PlannerStatsSnapshot {
                pg_version: "PostgreSQL 17.0".into(),
                database: "test".into(),
                timestamp: Utc::now(),
                content_hash: "ph".into(),
                schema_ref_hash: "sh".into(),
                tables: vec![TableSizingEntry {
                    table: QualifiedName::new("public", "users"),
                    sizing: TableSizing {
                        reltuples,
                        relpages: 6250,
                        table_size: 50_000_000,
                        total_size: None,
                        index_size: None,
                    },
                }],
                columns: vec![],
                indexes: vec![],
            }),
            activity_by_node: BTreeMap::new(),
        }
    }

    fn test_schema() -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "test".into(),
            timestamp: Utc::now(),
            content_hash: "test".into(),
            source: None,
            tables: vec![Table {
                oid: 1,
                schema: "public".into(),
                name: "users".into(),
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
                        name: "email".into(),
                        ordinal: 2,
                        type_name: "text".into(),
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
                // Stats now live in PlannerStatsSnapshot — see test_annotated.
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

    #[test]
    fn suggest_from_where_clause() {
        let snap = test_annotated(500_000.0);
        let suggestions = suggest_index(
            "SELECT * FROM users WHERE email = 'test@example.com'",
            &snap.view(),
            None,
            None,
        )
        .unwrap();
        assert!(!suggestions.is_empty());
        assert_eq!(suggestions[0].table, "public.users");
        assert!(suggestions[0].columns.contains(&"email".to_string()));
        assert_eq!(suggestions[0].index_type, "btree");
        assert!(suggestions[0].ddl.contains("CONCURRENTLY"));
    }

    #[test]
    fn suggest_gin_for_jsonb() {
        let snap = test_annotated(500_000.0);
        let suggestions = suggest_index(
            "SELECT * FROM users u WHERE u.data = '{}'",
            &snap.view(),
            None,
            None,
        )
        .unwrap();
        let jsonb = suggestions
            .iter()
            .find(|s| s.columns.contains(&"data".to_string()));
        assert!(jsonb.is_some());
        assert_eq!(jsonb.unwrap().index_type, "gin");
    }

    #[test]
    fn no_suggestion_for_small_table() {
        // Tiny reltuples (< 1000) → suggest_from_query_structure short-circuits.
        let snap = test_annotated(50.0);
        let suggestions = suggest_index(
            "SELECT * FROM users WHERE email = 'x'",
            &snap.view(),
            None,
            None,
        )
        .unwrap();
        assert!(suggestions.is_empty());
    }

    #[test]
    fn no_suggestion_when_planner_absent() {
        // Degradation case: no planner → reltuples returns None → 0.0 →
        // is_large is false → no suggestion. Pins the new "no data → no
        // suggestions" path.
        let snap = AnnotatedSnapshot {
            schema: test_schema(),
            planner: None,
            activity_by_node: BTreeMap::new(),
        };
        let suggestions = suggest_index(
            "SELECT * FROM users WHERE email = 'x'",
            &snap.view(),
            None,
            None,
        )
        .unwrap();
        assert!(suggestions.is_empty());
    }

    #[test]
    fn no_duplicate_suggestions() {
        let snap = test_annotated(500_000.0);
        let plan = PlanNode {
            node_type: "Seq Scan".into(),
            relation_name: Some("users".into()),
            schema: Some("public".into()),
            alias: None,
            startup_cost: 0.0,
            total_cost: 500.0,
            plan_rows: 100_000.0,
            plan_width: 64,
            actual_rows: None,
            actual_loops: None,
            actual_startup_time: None,
            actual_total_time: None,
            shared_hit_blocks: None,
            shared_read_blocks: None,
            index_name: None,
            index_cond: None,
            filter: Some("(email = 'test@example.com')".into()),
            rows_removed_by_filter: None,
            sort_key: None,
            sort_method: None,
            hash_cond: None,
            join_type: None,
            subplans_removed: None,
            cte_name: None,
            parent_relationship: None,
            children: vec![],
        };
        let suggestions = suggest_index(
            "SELECT * FROM users WHERE email = 'test@example.com'",
            &snap.view(),
            Some(&plan),
            None,
        )
        .unwrap();
        let email_count = suggestions
            .iter()
            .filter(|s| s.columns.contains(&"email".to_string()))
            .count();
        assert_eq!(email_count, 1, "should deduplicate");
    }
}
