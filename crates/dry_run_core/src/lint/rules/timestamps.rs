use crate::jit;
use crate::schema::Table;

use super::super::types::{LintConfig, LintViolation, Severity};

pub fn check_has_created_at(
    table: &Table,
    qualified: &str,
    config: &LintConfig,
    violations: &mut Vec<LintViolation>,
) {
    if !config.require_timestamps {
        return;
    }

    let has_created_at = table.columns.iter().any(|c| c.name == "created_at");
    if !has_created_at {
        let e = jit::missing_timestamp(qualified, "created_at");
        violations.push(LintViolation {
            rule: "timestamps/has_created_at".into(),
            severity: Severity::Warning,
            table: qualified.into(),
            column: None,
            message: "table is missing 'created_at' column".into(),
            recommendation: e.reason,
            ddl_fix: Some(e.fix),
            convention_doc: "timestamps".into(),
        });
    }
}

pub fn check_has_updated_at(
    table: &Table,
    qualified: &str,
    config: &LintConfig,
    violations: &mut Vec<LintViolation>,
) {
    if !config.require_timestamps {
        return;
    }

    let has_updated_at = table.columns.iter().any(|c| c.name == "updated_at");
    if !has_updated_at {
        let e = jit::missing_timestamp(qualified, "updated_at");
        violations.push(LintViolation {
            rule: "timestamps/has_updated_at".into(),
            severity: Severity::Warning,
            table: qualified.into(),
            column: None,
            message: "table is missing 'updated_at' column".into(),
            recommendation: e.reason,
            ddl_fix: Some(e.fix),
            convention_doc: "timestamps".into(),
        });
    }
}

pub fn check_timestamp_type(
    table: &Table,
    qualified: &str,
    config: &LintConfig,
    violations: &mut Vec<LintViolation>,
) {
    if config.timestamp_type != "timestamptz" {
        return;
    }

    let timestamp_cols = ["created_at", "updated_at", "deleted_at"];

    for col in &table.columns {
        if !timestamp_cols.contains(&col.name.as_str()) {
            continue;
        }
        let type_lower = col.type_name.to_lowercase();
        if type_lower == "timestamp without time zone" || type_lower == "timestamp" {
            violations.push(LintViolation {
                rule: "timestamps/correct_type".into(),
                severity: Severity::Warning,
                table: qualified.into(),
                column: Some(col.name.clone()),
                message: format!(
                    "timestamp column '{}' uses {} instead of timestamptz",
                    col.name, col.type_name
                ),
                recommendation: "use timestamptz for timestamp columns".into(),
                ddl_fix: None,
            convention_doc: "timestamps".into(),
            });
        }
    }
}
