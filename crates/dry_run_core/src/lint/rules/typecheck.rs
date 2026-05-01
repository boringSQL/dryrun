use crate::jit;
use crate::schema::{ConstraintKind, Table};

use super::super::types::{LintConfig, LintViolation, Severity};

pub fn check_text_over_varchar(
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
            let e = jit::text_over_varchar(qualified, &col.name);
            violations.push(LintViolation {
                rule: "types/text_over_varchar".into(),
                severity: Severity::Warning,
                table: qualified.into(),
                column: Some(col.name.clone()),
                message: format!("column '{}' uses {} — prefer text", col.name, col.type_name),
                recommendation: e.reason,
                ddl_fix: Some(e.fix),
                convention_doc: "types".into(),
            });
        }
    }
}

pub fn check_timestamptz(table: &Table, qualified: &str, violations: &mut Vec<LintViolation>) {
    for col in &table.columns {
        let type_lower = col.type_name.to_lowercase();
        if type_lower == "timestamp without time zone" || type_lower == "timestamp" {
            let e = jit::timestamp_to_timestamptz(qualified, &col.name);
            let rec = match &e.note {
                Some(note) => format!("{}\n{note}", e.reason),
                None => e.reason,
            };
            violations.push(LintViolation {
                rule: "types/timestamptz".into(),
                severity: Severity::Warning,
                table: qualified.into(),
                column: Some(col.name.clone()),
                message: format!("column '{}' uses timestamp without time zone", col.name),
                recommendation: rec,
                ddl_fix: Some(e.fix),
                convention_doc: "types".into(),
            });
        }
    }
}

pub fn check_no_serial(table: &Table, qualified: &str, violations: &mut Vec<LintViolation>) {
    for col in &table.columns {
        if let Some(default) = &col.default
            && default.to_lowercase().contains("nextval(")
        {
            violations.push(LintViolation {
                rule: "types/no_serial".into(),
                severity: Severity::Warning,
                table: qualified.into(),
                column: Some(col.name.clone()),
                message: format!(
                    "column '{}' uses serial/sequence default ({})",
                    col.name, default
                ),
                recommendation: "use bigint GENERATED ALWAYS AS IDENTITY instead of serial".into(),
                ddl_fix: None,
                convention_doc: "types".into(),
            });
        }
    }
}

pub fn check_bigint_pk_fk(
    table: &Table,
    qualified: &str,
    config: &LintConfig,
    violations: &mut Vec<LintViolation>,
) {
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
        let is_int = type_lower == "integer" || type_lower == "int4" || type_lower == "int";
        // when pk_type is int_identity, integer is acceptable
        if is_int && config.pk_type == "int_identity" {
            continue;
        }
        if is_int || type_lower == "smallint" || type_lower == "int2" {
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
                ddl_fix: None,
                convention_doc: "types".into(),
            });
        }
    }
}
