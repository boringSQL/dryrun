use crate::schema::{ConstraintKind, SchemaSnapshot, Table};

use super::super::types::{LintViolation, Severity};

pub fn check_fk_has_index(
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
            let col_list = constraint.columns.join(", ");
            let ddl = format!(
                "CREATE INDEX CONCURRENTLY idx_{}_{} ON {}({});",
                table.name,
                constraint.columns.join("_"),
                qualified,
                col_list
            );
            violations.push(LintViolation {
                rule: "constraints/fk_has_index".into(),
                severity: Severity::Error,
                table: qualified.into(),
                column: Some(col_list.clone()),
                message: format!(
                    "FK '{}' on column(s) ({}) has no covering index",
                    constraint.name, col_list
                ),
                recommendation: "Add an index on FK columns to avoid sequential scans on DELETE/UPDATE of the referenced table.".into(),
                ddl_fix: Some(ddl),
            convention_doc: "constraints".into(),
            });
        }
    }
}

pub fn check_unnamed_constraints(
    table: &Table,
    qualified: &str,
    violations: &mut Vec<LintViolation>,
) {
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
                ddl_fix: None,
                convention_doc: "constraints".into(),
            });
        }
    }
}
