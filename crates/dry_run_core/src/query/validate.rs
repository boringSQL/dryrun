use serde::{Deserialize, Serialize};

use super::antipatterns::detect_antipatterns;
use super::parse::{parse_sql, ParsedQuery, ReferencedTable};
use crate::error::Result;
use crate::schema::SchemaSnapshot;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<ValidationWarning>,
    pub referenced_objects: Vec<ReferencedTable>,
    pub resolved_star_columns: Vec<ResolvedStar>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationWarning {
    pub severity: WarningSeverity,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarningSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedStar {
    pub table: String,
    pub columns: Vec<String>,
}

pub fn validate_query(sql: &str, schema: &SchemaSnapshot) -> Result<ValidationResult> {
    let parsed = parse_sql(sql)?;
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    let mut resolved_star = Vec::new();

    // check each referenced table exists
    for table_ref in &parsed.info.tables {
        let table_name = &table_ref.name;
        let schema_name = table_ref.schema.as_deref().unwrap_or("public");

        let found = schema
            .tables
            .iter()
            .find(|t| t.name == *table_name && t.schema == schema_name);

        if found.is_none() {
            let is_view = schema
                .views
                .iter()
                .any(|v| v.name == *table_name && v.schema == schema_name);

            if !is_view {
                errors.push(format!(
                    "table or view '{schema_name}.{table_name}' does not exist"
                ));
            }
        }
    }

    validate_filter_columns(&parsed, schema, &mut errors);

    // resolve SELECT *
    if parsed.info.has_select_star {
        for table_ref in &parsed.info.tables {
            let schema_name = table_ref.schema.as_deref().unwrap_or("public");
            if let Some(table) = schema
                .tables
                .iter()
                .find(|t| t.name == table_ref.name && t.schema == schema_name)
            {
                resolved_star.push(ResolvedStar {
                    table: format!("{}.{}", table.schema, table.name),
                    columns: table.columns.iter().map(|c| c.name.clone()).collect(),
                });
            }
        }
    }

    detect_antipatterns(&parsed, schema, &mut warnings);

    let valid = errors.is_empty();

    Ok(ValidationResult {
        valid,
        errors,
        warnings,
        referenced_objects: parsed.info.tables,
        resolved_star_columns: resolved_star,
    })
}

fn validate_filter_columns(
    parsed: &ParsedQuery,
    schema: &SchemaSnapshot,
    errors: &mut Vec<String>,
) {
    for (table_alias, col_name) in &parsed.info.filter_columns {
        if let Some(alias) = table_alias {
            let table_ref = parsed
                .info
                .tables
                .iter()
                .find(|t| t.alias.as_deref() == Some(alias.as_str()) || t.name == *alias);

            if let Some(table_ref) = table_ref {
                let schema_name = table_ref.schema.as_deref().unwrap_or("public");
                if let Some(table) = schema
                    .tables
                    .iter()
                    .find(|t| t.name == table_ref.name && t.schema == schema_name)
                {
                    if !table.columns.iter().any(|c| c.name == *col_name) {
                        errors.push(format!(
                            "column '{col_name}' does not exist on table '{}.{}'",
                            table.schema, table.name
                        ));
                    }
                }
            }
        }
    }
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
            tables: vec![
                Table {
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
                            comment: None,
                            stats: None,
                        },
                        Column {
                            name: "email".into(),
                            ordinal: 2,
                            type_name: "text".into(),
                            nullable: false,
                            default: None,
                            identity: None,
                            comment: None,
                            stats: None,
                        },
                    ],
                    constraints: vec![],
                    indexes: vec![],
                    comment: None,
                    stats: Some(TableStats {
                        reltuples: 1_000_000.0,
                        dead_tuples: 0,
                        last_vacuum: None,
                        last_autovacuum: None,
                        last_analyze: None,
                        last_autoanalyze: None,
                        seq_scan: 0,
                        idx_scan: 0,
                        table_size: 100_000_000,
                    }),
                    partition_info: None,
                    policies: vec![],
                    triggers: vec![],
                    rls_enabled: false,
                },
                Table {
                    oid: 2,
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
                            comment: None,
                            stats: None,
                        },
                        Column {
                            name: "user_id".into(),
                            ordinal: 2,
                            type_name: "bigint".into(),
                            nullable: false,
                            default: None,
                            identity: None,
                            comment: None,
                            stats: None,
                        },
                    ],
                    constraints: vec![],
                    indexes: vec![],
                    comment: None,
                    stats: Some(TableStats {
                        reltuples: 50.0,
                        dead_tuples: 0,
                        last_vacuum: None,
                        last_autovacuum: None,
                        last_analyze: None,
                        last_autoanalyze: None,
                        seq_scan: 0,
                        idx_scan: 0,
                        table_size: 8192,
                    }),
                    partition_info: None,
                    policies: vec![],
                    triggers: vec![],
                    rls_enabled: false,
                },
            ],
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
    fn valid_query() {
        let schema = test_schema();
        let result = validate_query("SELECT id, email FROM users WHERE id = 1", &schema).unwrap();
        assert!(result.valid);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn nonexistent_table() {
        let schema = test_schema();
        let result = validate_query("SELECT * FROM nonexistent", &schema).unwrap();
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.contains("does not exist")));
    }

    #[test]
    fn nonexistent_column_in_where() {
        let schema = test_schema();
        let result =
            validate_query("SELECT id FROM users u WHERE u.fake_col = 1", &schema).unwrap();
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.contains("fake_col")));
    }

    #[test]
    fn select_star_resolved() {
        let schema = test_schema();
        let result = validate_query("SELECT * FROM users", &schema).unwrap();
        assert!(result.valid);
        assert!(!result.resolved_star_columns.is_empty());
        assert_eq!(result.resolved_star_columns[0].columns.len(), 2);
    }

    #[test]
    fn select_star_warning() {
        let schema = test_schema();
        let result = validate_query("SELECT * FROM users", &schema).unwrap();
        assert!(result
            .warnings
            .iter()
            .any(|w| w.message.contains("SELECT *")));
    }

    #[test]
    fn unbounded_query_warning() {
        let schema = test_schema();
        let result = validate_query("SELECT id FROM users", &schema).unwrap();
        assert!(result
            .warnings
            .iter()
            .any(|w| w.message.contains("unbounded")));
    }

    #[test]
    fn cartesian_join_warning() {
        let schema = test_schema();
        let result = validate_query("SELECT * FROM users, orders", &schema).unwrap();
        assert!(result
            .warnings
            .iter()
            .any(|w| w.message.contains("cartesian") || w.message.contains("Cartesian")));
    }
}
