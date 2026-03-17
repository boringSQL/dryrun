use serde::{Deserialize, Serialize};

use super::parse::parse_sql;
use super::plan::PlanNode;
use crate::error::Result;
use crate::knowledge;
use crate::schema::{SchemaSnapshot, Table, effective_table_stats};
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
    pub knowledge_doc: Option<String>,
    pub estimated_impact: String,
}

pub fn suggest_index(
    sql: &str,
    schema: &SchemaSnapshot,
    plan: Option<&PlanNode>,
    pg_version: Option<&PgVersion>,
) -> Result<Vec<IndexSuggestion>> {
    let parsed = parse_sql(sql)?;
    let mut suggestions = Vec::new();

    if let Some(plan) = plan {
        suggest_from_plan(plan, schema, pg_version, &mut suggestions);
    }

    suggest_from_query_structure(&parsed, schema, pg_version, &mut suggestions);
    dedup_suggestions(&mut suggestions);

    Ok(suggestions)
}

// plan-based suggestions

fn suggest_from_plan(
    node: &PlanNode,
    schema: &SchemaSnapshot,
    pg_version: Option<&PgVersion>,
    suggestions: &mut Vec<IndexSuggestion>,
) {
    if node.node_type == "Seq Scan" && node.plan_rows >= 1000.0 {
        if let Some(table_name) = &node.relation_name {
            let schema_name = node.schema.as_deref().unwrap_or("public");
            let table = schema
                .tables
                .iter()
                .find(|t| t.name == *table_name && t.schema == schema_name);

            if let Some(filter) = &node.filter {
                if let Some(col) = extract_filter_column(filter) {
                    if !has_leading_index(table, &col) {
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
                            knowledge_doc: doc_for_type(idx_type, pg_version),
                            estimated_impact: estimate_impact(node.plan_rows),
                        });
                    }
                }
            }
        }
    }

    if node.node_type == "Sort" && node.plan_rows >= 5000.0 {
        if let Some(sort_keys) = &node.sort_key {
            if let Some((schema_name, table_name)) = find_table_in_subtree(node) {
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
                    ddl: format!(
                        "CREATE INDEX CONCURRENTLY {idx_name} ON {qualified}({col_list});"
                    ),
                    rationale: format!(
                        "Sort on ~{} rows could be avoided with an index on ({})",
                        node.plan_rows as i64, col_list
                    ),
                    knowledge_doc: doc_for_type("btree", pg_version),
                    estimated_impact: "eliminates sort step".into(),
                });
            }
        }
    }

    for child in &node.children {
        suggest_from_plan(child, schema, pg_version, suggestions);
    }
}

// query structure-based suggestions

fn suggest_from_query_structure(
    parsed: &super::parse::ParsedQuery,
    schema: &SchemaSnapshot,
    pg_version: Option<&PgVersion>,
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
            let table = schema
                .tables
                .iter()
                .find(|t| t.name == table_ref.name && t.schema == schema_name);

            if let Some(table) = table {
                let effective_stats = effective_table_stats(table, schema);
                let is_large = effective_stats
                    .as_ref()
                    .is_some_and(|s| s.reltuples >= 1000.0);

                if is_large && !has_leading_index(Some(table), col_name) {
                    let idx_type = choose_index_type(Some(table), col_name);
                    let qualified = format!("{}.{}", table.schema, table.name);
                    let idx_name = format!("idx_{}_{col_name}", table.name);
                    let reltuples = effective_stats
                        .as_ref()
                        .map(|s| s.reltuples)
                        .unwrap_or(0.0);

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
                        knowledge_doc: doc_for_type(idx_type, pg_version),
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
    if let Some(table) = table {
        if let Some(column) = table.columns.iter().find(|c| c.name == col) {
            let ct = column.type_name.to_lowercase();
            if ct == "jsonb" || ct == "tsvector" {
                return "gin";
            }
            if ct.contains("geometry") || ct.contains("geography") || ct.contains("range") {
                return "gist";
            }
        }
    }
    "btree"
}

fn doc_for_type(idx_type: &str, pg_version: Option<&PgVersion>) -> Option<String> {
    let docs = knowledge::lookup_index_decisions(idx_type, pg_version);
    docs.first().map(|d| d.name.clone())
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

    use super::*;
    use crate::schema::*;

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
                    Column { name: "id".into(), ordinal: 1, type_name: "bigint".into(), nullable: false, default: None, identity: None, comment: None, stats: None },
                    Column { name: "email".into(), ordinal: 2, type_name: "text".into(), nullable: false, default: None, identity: None, comment: None, stats: None },
                    Column { name: "data".into(), ordinal: 3, type_name: "jsonb".into(), nullable: true, default: None, identity: None, comment: None, stats: None },
                ],
                constraints: vec![],
                indexes: vec![],
                comment: None,
                stats: Some(TableStats { reltuples: 500_000.0, relpages: 6250, dead_tuples: 0, last_vacuum: None, last_autovacuum: None, last_analyze: None, last_autoanalyze: None, seq_scan: 0, idx_scan: 0, table_size: 50_000_000 }),
                partition_info: None,
                policies: vec![],
                triggers: vec![],
                rls_enabled: false,
            }],
            enums: vec![], domains: vec![], composites: vec![], views: vec![], functions: vec![], extensions: vec![], gucs: vec![],
            node_stats: vec![],
        }
    }

    #[test]
    fn suggest_from_where_clause() {
        let schema = test_schema();
        let suggestions = suggest_index("SELECT * FROM users WHERE email = 'test@example.com'", &schema, None, None).unwrap();
        assert!(!suggestions.is_empty());
        assert_eq!(suggestions[0].table, "public.users");
        assert!(suggestions[0].columns.contains(&"email".to_string()));
        assert_eq!(suggestions[0].index_type, "btree");
        assert!(suggestions[0].ddl.contains("CONCURRENTLY"));
    }

    #[test]
    fn suggest_gin_for_jsonb() {
        let schema = test_schema();
        let suggestions = suggest_index("SELECT * FROM users u WHERE u.data = '{}'", &schema, None, None).unwrap();
        let jsonb = suggestions.iter().find(|s| s.columns.contains(&"data".to_string()));
        assert!(jsonb.is_some());
        assert_eq!(jsonb.unwrap().index_type, "gin");
    }

    #[test]
    fn no_suggestion_for_small_table() {
        let mut schema = test_schema();
        schema.tables[0].stats.as_mut().unwrap().reltuples = 50.0;
        let suggestions = suggest_index("SELECT * FROM users WHERE email = 'x'", &schema, None, None).unwrap();
        assert!(suggestions.is_empty());
    }

    #[test]
    fn no_duplicate_suggestions() {
        let schema = test_schema();
        let plan = PlanNode {
            node_type: "Seq Scan".into(), relation_name: Some("users".into()), schema: Some("public".into()),
            alias: None, startup_cost: 0.0, total_cost: 500.0, plan_rows: 100_000.0, plan_width: 64,
            actual_rows: None, actual_loops: None, actual_startup_time: None, actual_total_time: None,
            shared_hit_blocks: None, shared_read_blocks: None, index_name: None, index_cond: None,
            filter: Some("(email = 'test@example.com')".into()), rows_removed_by_filter: None,
            sort_key: None, sort_method: None, hash_cond: None, join_type: None, children: vec![],
        };
        let suggestions = suggest_index("SELECT * FROM users WHERE email = 'test@example.com'", &schema, Some(&plan), None).unwrap();
        let email_count = suggestions.iter().filter(|s| s.columns.contains(&"email".to_string())).count();
        assert_eq!(email_count, 1, "should deduplicate");
    }

    #[test]
    fn includes_knowledge_doc() {
        let schema = test_schema();
        let suggestions = suggest_index("SELECT * FROM users WHERE email = 'x'", &schema, None, None).unwrap();
        if !suggestions.is_empty() {
            assert!(suggestions[0].knowledge_doc.is_some());
        }
    }
}
