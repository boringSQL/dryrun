mod fk_graph;
mod indexes;
mod schema;

use super::types::{AuditConfig, AuditFinding};
use crate::schema::SchemaSnapshot;

// Runs all audit rules and returns findings, skipping disabled ones
#[must_use]
pub fn run_all_audit_rules(
    snapshot: &SchemaSnapshot,
    config: &AuditConfig,
) -> Vec<AuditFinding> {
    let mut findings = Vec::new();
    let disabled = &config.disabled_rules;

    macro_rules! run_rule {
        ($id:expr, $check:expr) => {
            if !disabled.iter().any(|d| d == $id) {
                findings.extend($check);
            }
        };
    }

    // index rules
    run_rule!("indexes/duplicate", indexes::check_duplicate_indexes(snapshot));
    run_rule!("indexes/redundant", indexes::check_redundant_indexes(snapshot));
    run_rule!("indexes/too_many", indexes::check_too_many_indexes(snapshot, config));
    run_rule!("indexes/wide_columns", indexes::check_wide_column_indexes(snapshot));

    // FK rules
    run_rule!("fk/type_mismatch", fk_graph::check_fk_type_mismatch(snapshot));
    run_rule!("fk/circular", fk_graph::check_circular_fks(snapshot));
    run_rule!("fk/orphan", fk_graph::check_orphan_tables(snapshot));

    // PK rules
    run_rule!("pk/non_sequential", schema::check_pk_non_sequential(snapshot));

    // naming rules
    run_rule!("naming/bool_prefix", schema::check_bool_prefix(snapshot));
    run_rule!("naming/reserved", schema::check_reserved_words(snapshot));
    run_rule!("naming/id_mismatch", schema::check_id_mismatch(snapshot));

    // documentation rules
    run_rule!("docs/no_comment", schema::check_no_comment(snapshot, config));

    findings
}
