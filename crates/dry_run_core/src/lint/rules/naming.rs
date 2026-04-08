use regex::Regex;

use crate::schema::{ConstraintKind, Table};

use super::super::types::{LintConfig, LintViolation, Severity};

pub fn check_table_name_style(
    table: &Table,
    qualified: &str,
    config: &LintConfig,
    violations: &mut Vec<LintViolation>,
) {
    let name = &table.name;
    let valid = match config.table_name_style.as_str() {
        "snake_singular" => is_snake_case(name) && !looks_plural(name),
        "snake_plural" => is_snake_case(name),
        "camelCase" => {
            let re = Regex::new(r"^[a-z][a-zA-Z0-9]*$").unwrap();
            re.is_match(name)
        }
        "PascalCase" => {
            let re = Regex::new(r"^[A-Z][a-zA-Z0-9]*$").unwrap();
            re.is_match(name)
        }
        "custom_regex" => {
            if let Some(pattern) = &config.table_name_regex {
                Regex::new(pattern)
                    .map(|re| re.is_match(name))
                    .unwrap_or(true)
            } else {
                true
            }
        }
        _ => true,
    };

    if !valid {
        violations.push(LintViolation {
            rule: "naming/table_style".into(),
            severity: Severity::Warning,
            table: qualified.into(),
            column: None,
            message: format!(
                "table name '{}' does not match style '{}'",
                name, config.table_name_style
            ),
            recommendation: format!("rename to match {} convention", config.table_name_style),
            ddl_fix: None,
            convention_doc: "naming".into(),
        });
    }
}

pub fn check_column_name_style(
    table: &Table,
    qualified: &str,
    config: &LintConfig,
    violations: &mut Vec<LintViolation>,
) {
    let camel_re = Regex::new(r"^[a-z][a-zA-Z0-9]*$").unwrap();
    let custom_re = config
        .column_name_regex
        .as_ref()
        .and_then(|p| Regex::new(p).ok());

    for col in &table.columns {
        let valid = match config.column_name_style.as_str() {
            "snake_case" => is_snake_case(&col.name),
            "camelCase" => camel_re.is_match(&col.name),
            "custom_regex" => custom_re
                .as_ref()
                .map(|re| re.is_match(&col.name))
                .unwrap_or(true),
            _ => true,
        };

        if !valid {
            violations.push(LintViolation {
                rule: "naming/column_style".into(),
                severity: Severity::Warning,
                table: qualified.into(),
                column: Some(col.name.clone()),
                message: format!(
                    "column '{}' does not match style '{}'",
                    col.name, config.column_name_style
                ),
                recommendation: format!("rename to match {} convention", config.column_name_style),
                ddl_fix: None,
            convention_doc: "naming".into(),
            });
        }
    }
}

pub fn check_fk_naming(
    table: &Table,
    qualified: &str,
    config: &LintConfig,
    violations: &mut Vec<LintViolation>,
) {
    for constraint in &table.constraints {
        if constraint.kind != ConstraintKind::ForeignKey {
            continue;
        }
        let expected = config
            .fk_pattern
            .replace("{table}", &table.name)
            .replace("{column}", &constraint.columns.join("_"));

        if constraint.name != expected {
            violations.push(LintViolation {
                rule: "naming/fk_pattern".into(),
                severity: Severity::Info,
                table: qualified.into(),
                column: None,
                message: format!(
                    "FK constraint '{}' doesn't match pattern '{}' (expected '{}')",
                    constraint.name, config.fk_pattern, expected
                ),
                recommendation: format!("rename constraint to '{expected}'"),
                ddl_fix: None,
            convention_doc: "naming".into(),
            });
        }
    }
}

pub fn check_index_naming(
    table: &Table,
    qualified: &str,
    config: &LintConfig,
    violations: &mut Vec<LintViolation>,
) {
    for index in &table.indexes {
        if index.is_primary {
            continue;
        }
        let expected = config
            .index_pattern
            .replace("{table}", &table.name)
            .replace("{columns}", &index.columns.join("_"));

        if index.name != expected {
            violations.push(LintViolation {
                rule: "naming/index_pattern".into(),
                severity: Severity::Info,
                table: qualified.into(),
                column: None,
                message: format!(
                    "index '{}' doesn't match pattern '{}' (expected '{}')",
                    index.name, config.index_pattern, expected
                ),
                recommendation: format!("rename index to '{expected}'"),
                ddl_fix: None,
            convention_doc: "naming".into(),
            });
        }
    }
}

pub fn is_snake_case(s: &str) -> bool {
    let re = Regex::new(r"^[a-z][a-z0-9_]*$").unwrap();
    re.is_match(s)
}

// simple heuristic: looks plural if ends in 's' but not 'ss', 'us', 'is'
pub fn looks_plural(name: &str) -> bool {
    if name.ends_with('s')
        && !name.ends_with("ss")
        && !name.ends_with("us")
        && !name.ends_with("is")
        && !name.ends_with("ies")
    {
        return true;
    }
    if name.ends_with("ies") && name != "series" {
        return true;
    }
    false
}
