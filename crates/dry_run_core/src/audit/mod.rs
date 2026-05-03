mod rules;
pub mod types;

pub use types::{AuditConfig, AuditFinding, AuditReport, AuditSummary};

use crate::schema::AnnotatedSchema;

// Public audit entry point — takes the annotated view because two of the
// rules under the hood (`indexes/bloated`, `vacuum/large_table_defaults`)
// need planner sizing / row counts. DDL-only rules just hop through to
// `annotated.schema` internally; callers who only have a bare
// `SchemaSnapshot` can wrap it in a stats-less `AnnotatedSnapshot` to
// adapt — those rules will simply produce no findings, matching the
// pre-split behavior.
#[must_use]
pub fn run_audit(annotated: &AnnotatedSchema<'_>, config: &AuditConfig) -> AuditReport {
    let tables_analyzed = annotated.schema.tables.len();
    let findings = rules::run_all_audit_rules(annotated, config);
    AuditReport::new(findings, tables_analyzed)
}
