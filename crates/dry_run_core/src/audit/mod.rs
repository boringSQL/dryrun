mod rules;
pub mod types;

pub use types::{AuditConfig, AuditFinding, AuditReport, AuditSummary};

use crate::schema::SchemaSnapshot;

#[must_use]
pub fn run_audit(schema: &SchemaSnapshot, config: &AuditConfig) -> AuditReport {
    let tables_analyzed = schema.tables.len();
    let findings = rules::run_all_audit_rules(schema, config);
    AuditReport::new(findings, tables_analyzed)
}
