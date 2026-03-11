use regex::Regex;

use crate::schema::{ConstraintKind, SchemaSnapshot, Table};

use super::types::{LintConfig, LintViolation, Severity};

pub fn run_all_rules(schema: &SchemaSnapshot, config: &LintConfig) -> Vec<LintViolation> {
    let mut violations = Vec::new();

    for table in &schema.tables {
        let qualified = format!("{}.{}", table.schema, table.name);

        if !is_disabled(config, "naming/table_style") {
            check_table_name_style(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "naming/column_style") {
            check_column_name_style(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "naming/fk_pattern") {
            check_fk_naming(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "naming/index_pattern") {
            check_index_naming(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "pk/exists") {
            check_pk_exists(table, &qualified, &mut violations);
        }
        if !is_disabled(config, "pk/bigint_identity") {
            check_pk_type(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "types/text_over_varchar") {
            check_text_over_varchar(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "types/timestamptz") {
            check_timestamptz(table, &qualified, &mut violations);
        }
        if !is_disabled(config, "types/no_serial") {
            check_no_serial(table, &qualified, &mut violations);
        }
        if !is_disabled(config, "types/bigint_pk_fk") {
            check_bigint_pk_fk(table, &qualified, &mut violations);
        }
        if !is_disabled(config, "constraints/fk_has_index") {
            check_fk_has_index(table, &qualified, schema, &mut violations);
        }
        if !is_disabled(config, "constraints/unnamed") {
            check_unnamed_constraints(table, &qualified, &mut violations);
        }
        if !is_disabled(config, "timestamps/has_created_at") {
            check_has_created_at(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "timestamps/has_updated_at") {
            check_has_updated_at(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "timestamps/correct_type") {
            check_timestamp_type(table, &qualified, config, &mut violations);
        }
    }

    violations
}

fn is_disabled(config: &LintConfig, rule: &str) -> bool {
    config.disabled_rules.iter().any(|r| r == rule)
}

// naming rules

fn check_table_name_style(
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
            convention_doc: "naming".into(),
        });
    }
}

fn check_column_name_style(
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
                convention_doc: "naming".into(),
            });
        }
    }
}

fn check_fk_naming(
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
                convention_doc: "naming".into(),
            });
        }
    }
}

fn check_index_naming(
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
                convention_doc: "naming".into(),
            });
        }
    }
}

// primary key rules

fn check_pk_exists(table: &Table, qualified: &str, violations: &mut Vec<LintViolation>) {
    let has_pk = table
        .constraints
        .iter()
        .any(|c| c.kind == ConstraintKind::PrimaryKey);

    if !has_pk {
        violations.push(LintViolation {
            rule: "pk/exists".into(),
            severity: Severity::Error,
            table: qualified.into(),
            column: None,
            message: "table has no primary key".into(),
            recommendation: "add a primary key (bigint GENERATED ALWAYS AS IDENTITY recommended)"
                .into(),
            convention_doc: "primary_keys".into(),
        });
    }
}

fn check_pk_type(
    table: &Table,
    qualified: &str,
    config: &LintConfig,
    violations: &mut Vec<LintViolation>,
) {
    if config.pk_type != "bigint_identity" {
        return;
    }

    let pk_constraint = table
        .constraints
        .iter()
        .find(|c| c.kind == ConstraintKind::PrimaryKey);

    let Some(pk) = pk_constraint else {
        return;
    };

    for pk_col_name in &pk.columns {
        let Some(col) = table.columns.iter().find(|c| &c.name == pk_col_name) else {
            continue;
        };

        let type_lower = col.type_name.to_lowercase();
        let is_bigint = type_lower == "bigint" || type_lower == "int8";
        let is_identity = col.identity.is_some();

        if !is_bigint || !is_identity {
            violations.push(LintViolation {
                rule: "pk/bigint_identity".into(),
                severity: Severity::Warning,
                table: qualified.into(),
                column: Some(pk_col_name.clone()),
                message: format!(
                    "PK column '{}' is {} {}— expected bigint with identity",
                    pk_col_name,
                    col.type_name,
                    if is_identity { "(identity) " } else { "" }
                ),
                recommendation: "use bigint GENERATED ALWAYS AS IDENTITY for primary keys".into(),
                convention_doc: "primary_keys".into(),
            });
        }
    }
}

// type rules

fn check_text_over_varchar(
    table: &Table,
    qualified: &str,
    config: &LintConfig,
    violations: &mut Vec<LintViolation>,
) {
    if !config.prefer_text_over_varchar {
        return;
    }

    for col in &table.columns {
        let type_lower = col.type_name.to_lowercase();
        if type_lower.starts_with("character varying") || type_lower.starts_with("varchar") {
            violations.push(LintViolation {
                rule: "types/text_over_varchar".into(),
                severity: Severity::Warning,
                table: qualified.into(),
                column: Some(col.name.clone()),
                message: format!("column '{}' uses {} — prefer text", col.name, col.type_name),
                recommendation:
                    "use text instead of varchar; add a CHECK constraint for length if needed"
                        .into(),
                convention_doc: "types".into(),
            });
        }
    }
}

fn check_timestamptz(table: &Table, qualified: &str, violations: &mut Vec<LintViolation>) {
    for col in &table.columns {
        let type_lower = col.type_name.to_lowercase();
        if type_lower == "timestamp without time zone" || type_lower == "timestamp" {
            violations.push(LintViolation {
                rule: "types/timestamptz".into(),
                severity: Severity::Warning,
                table: qualified.into(),
                column: Some(col.name.clone()),
                message: format!("column '{}' uses timestamp without time zone", col.name),
                recommendation: "use timestamptz (timestamp with time zone) instead".into(),
                convention_doc: "types".into(),
            });
        }
    }
}

fn check_no_serial(table: &Table, qualified: &str, violations: &mut Vec<LintViolation>) {
    for col in &table.columns {
        if let Some(default) = &col.default {
            if default.to_lowercase().contains("nextval(") {
                violations.push(LintViolation {
                    rule: "types/no_serial".into(),
                    severity: Severity::Warning,
                    table: qualified.into(),
                    column: Some(col.name.clone()),
                    message: format!(
                        "column '{}' uses serial/sequence default ({})",
                        col.name, default
                    ),
                    recommendation: "use bigint GENERATED ALWAYS AS IDENTITY instead of serial"
                        .into(),
                    convention_doc: "types".into(),
                });
            }
        }
    }
}

fn check_bigint_pk_fk(table: &Table, qualified: &str, violations: &mut Vec<LintViolation>) {
    let pk_cols: Vec<&str> = table
        .constraints
        .iter()
        .filter(|c| c.kind == ConstraintKind::PrimaryKey)
        .flat_map(|c| c.columns.iter().map(|s| s.as_str()))
        .collect();

    let fk_cols: Vec<&str> = table
        .constraints
        .iter()
        .filter(|c| c.kind == ConstraintKind::ForeignKey)
        .flat_map(|c| c.columns.iter().map(|s| s.as_str()))
        .collect();

    for col in &table.columns {
        let is_pk_or_fk =
            pk_cols.contains(&col.name.as_str()) || fk_cols.contains(&col.name.as_str());
        if !is_pk_or_fk {
            continue;
        }

        let type_lower = col.type_name.to_lowercase();
        if type_lower == "integer"
            || type_lower == "int4"
            || type_lower == "int"
            || type_lower == "smallint"
            || type_lower == "int2"
        {
            violations.push(LintViolation {
                rule: "types/bigint_pk_fk".into(),
                severity: Severity::Warning,
                table: qualified.into(),
                column: Some(col.name.clone()),
                message: format!(
                    "PK/FK column '{}' uses {} — risk of 32-bit overflow",
                    col.name, col.type_name
                ),
                recommendation: "use bigint for PK and FK columns".into(),
                convention_doc: "types".into(),
            });
        }
    }
}

// constraint rules

fn check_fk_has_index(
    table: &Table,
    qualified: &str,
    _schema: &SchemaSnapshot,
    violations: &mut Vec<LintViolation>,
) {
    for constraint in &table.constraints {
        if constraint.kind != ConstraintKind::ForeignKey {
            continue;
        }
        if constraint.columns.is_empty() {
            continue;
        }

        // index must have FK columns as a leading prefix, in order
        let has_covering_index = table.indexes.iter().any(|idx| {
            if idx.columns.len() < constraint.columns.len() {
                return false;
            }
            constraint
                .columns
                .iter()
                .zip(idx.columns.iter())
                .all(|(fk_col, idx_col)| fk_col == idx_col)
        });

        if !has_covering_index {
            violations.push(LintViolation {
                rule: "constraints/fk_has_index".into(),
                severity: Severity::Error,
                table: qualified.into(),
                column: Some(constraint.columns.join(", ")),
                message: format!(
                    "FK '{}' on column(s) ({}) has no covering index",
                    constraint.name,
                    constraint.columns.join(", ")
                ),
                recommendation: format!(
                    "CREATE INDEX idx_{}_{} ON {}({})",
                    table.name,
                    constraint.columns.join("_"),
                    table.name,
                    constraint.columns.join(", ")
                ),
                convention_doc: "constraints".into(),
            });
        }
    }
}

fn check_unnamed_constraints(table: &Table, qualified: &str, violations: &mut Vec<LintViolation>) {
    for constraint in &table.constraints {
        let name = &constraint.name;
        let is_auto = name.ends_with("_pkey")
            || name.ends_with("_fkey")
            || name.ends_with("_key")
            || name.ends_with("_check")
            || name.ends_with("_excl");

        if is_auto {
            violations.push(LintViolation {
                rule: "constraints/unnamed".into(),
                severity: Severity::Info,
                table: qualified.into(),
                column: None,
                message: format!("constraint '{}' appears to be auto-generated", name),
                recommendation: "name constraints explicitly for readable error messages".into(),
                convention_doc: "constraints".into(),
            });
        }
    }
}

// timestamp rules

fn check_has_created_at(
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
        violations.push(LintViolation {
            rule: "timestamps/has_created_at".into(),
            severity: Severity::Warning,
            table: qualified.into(),
            column: None,
            message: "table is missing 'created_at' column".into(),
            recommendation: "add: created_at timestamptz NOT NULL DEFAULT now()".into(),
            convention_doc: "timestamps".into(),
        });
    }
}

fn check_has_updated_at(
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
        violations.push(LintViolation {
            rule: "timestamps/has_updated_at".into(),
            severity: Severity::Warning,
            table: qualified.into(),
            column: None,
            message: "table is missing 'updated_at' column".into(),
            recommendation: "add: updated_at timestamptz NOT NULL DEFAULT now()".into(),
            convention_doc: "timestamps".into(),
        });
    }
}

fn check_timestamp_type(
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
                convention_doc: "timestamps".into(),
            });
        }
    }
}

// helpers

fn is_snake_case(s: &str) -> bool {
    let re = Regex::new(r"^[a-z][a-z0-9_]*$").unwrap();
    re.is_match(s)
}

// simple heuristic: looks plural if ends in 's' but not 'ss', 'us', 'is'
fn looks_plural(name: &str) -> bool {
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
