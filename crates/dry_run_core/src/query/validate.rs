use serde::{Deserialize, Serialize};

use super::antipatterns::detect_antipatterns;
use super::parse::{ParsedQuery, ReferencedTable, parse_sql};
use crate::error::Result;
use crate::schema::{AnnotatedSchema, SchemaSnapshot};

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

// Top-level validation entry point — combines existence checks (DDL only)
// with anti-pattern detection (mostly DDL, one stats-aware rule). Takes
// the annotated view so anti-pattern rules can reach planner stats; the
// existence-check sub-helpers borrow `annotated.schema` directly since
// they need nothing from planner / activity.
pub fn validate_query(sql: &str, annotated: &AnnotatedSchema<'_>) -> Result<ValidationResult> {
    let parsed = parse_sql(sql)?;
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    let mut resolved_star = Vec::new();
    let schema = annotated.schema;

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

    detect_antipatterns(&parsed, annotated, &mut warnings);

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
                    && !table.columns.iter().any(|c| c.name == *col_name)
                {
                    errors.push(format!(
                        "column '{col_name}' does not exist on table '{}.{}'",
                        table.schema, table.name
                    ));
                }
            }
        }
    }
}
