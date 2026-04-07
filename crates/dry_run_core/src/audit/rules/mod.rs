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
    run_rule!("indexes/bloated", indexes::check_bloated_indexes(snapshot));

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

    // storage rules
    run_rule!("vacuum/large_table_defaults", schema::check_vacuum_large_table_defaults(snapshot));

    findings
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::schema::*;
    use chrono::Utc;

    fn empty_schema() -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "PostgreSQL 17.0".into(), database: "test".into(),
            timestamp: Utc::now(), content_hash: "abc".into(), source: None,
            tables: vec![], enums: vec![], domains: vec![], composites: vec![],
            views: vec![], functions: vec![], extensions: vec![], gucs: vec![],
            node_stats: vec![],
        }
    }

    #[test]
    fn empty_schema_produces_no_findings() {
        let config = AuditConfig::default();
        let findings = run_all_audit_rules(&empty_schema(), &config);
        assert!(findings.is_empty());
    }

    #[test]
    fn disabled_rules_are_skipped() {
        let schema = SchemaSnapshot {
            tables: vec![Table {
                oid: 0, schema: "public".into(), name: "user".into(),
                columns: vec![Column {
                    name: "id".into(), ordinal: 0, type_name: "bigint".into(),
                    nullable: false, default: None, identity: None, generated: None, comment: None, statistics_target: None, stats: None,
                }],
                constraints: vec![], indexes: vec![],
                comment: None, stats: None, partition_info: None,
                policies: vec![], triggers: vec![], reloptions: vec![], rls_enabled: false,
            }],
            ..empty_schema()
        };

        let config = AuditConfig::default();
        let findings = run_all_audit_rules(&schema, &config);
        assert!(findings.iter().any(|f| f.rule == "naming/reserved"));

        let config = AuditConfig {
            disabled_rules: vec!["naming/reserved".into()],
            ..AuditConfig::default()
        };
        let findings = run_all_audit_rules(&schema, &config);
        assert!(!findings.iter().any(|f| f.rule == "naming/reserved"));
    }
}
