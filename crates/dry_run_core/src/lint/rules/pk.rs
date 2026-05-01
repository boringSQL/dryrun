use crate::jit;
use crate::schema::{ConstraintKind, Table};

use super::super::types::{LintConfig, LintViolation, Severity};

pub fn check_pk_exists(table: &Table, qualified: &str, violations: &mut Vec<LintViolation>) {
    let has_pk = table
        .constraints
        .iter()
        .any(|c| c.kind == ConstraintKind::PrimaryKey);

    if !has_pk {
        let e = jit::missing_primary_key(qualified);
        violations.push(LintViolation {
            rule: "pk/exists".into(),
            severity: Severity::Error,
            table: qualified.into(),
            column: None,
            message: "table has no primary key".into(),
            recommendation: e.reason,
            ddl_fix: Some(e.fix),
            convention_doc: "primary_keys".into(),
        });
    }
}

pub fn check_pk_type(
    table: &Table,
    qualified: &str,
    config: &LintConfig,
    violations: &mut Vec<LintViolation>,
) {
    if config.pk_type != "bigint_identity" && config.pk_type != "int_identity" {
        return;
    }

    let pk_constraint = table
        .constraints
        .iter()
        .find(|c| c.kind == ConstraintKind::PrimaryKey);

    let Some(pk) = pk_constraint else {
        return;
    };

    let allow_int = config.pk_type == "int_identity";

    for pk_col_name in &pk.columns {
        let Some(col) = table.columns.iter().find(|c| &c.name == pk_col_name) else {
            continue;
        };

        let type_lower = col.type_name.to_lowercase();
        let is_bigint = type_lower == "bigint" || type_lower == "int8";
        let is_int = type_lower == "integer" || type_lower == "int4" || type_lower == "int";
        let is_identity = col.identity.is_some();

        let type_ok = is_bigint || (allow_int && is_int);

        if !type_ok || !is_identity {
            let expected = if allow_int {
                "integer or bigint with identity"
            } else {
                "bigint with identity"
            };
            violations.push(LintViolation {
                rule: "pk/bigint_identity".into(),
                severity: Severity::Warning,
                table: qualified.into(),
                column: Some(pk_col_name.clone()),
                message: format!(
                    "PK column '{}' is {} {}— expected {expected}",
                    pk_col_name,
                    col.type_name,
                    if is_identity { "(identity) " } else { "" }
                ),
                recommendation: format!("use {expected} for primary keys"),
                ddl_fix: None,
                convention_doc: "primary_keys".into(),
            });
        }
    }
}
