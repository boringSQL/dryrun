mod fk_graph;
mod indexes;
mod schema;

use super::types::{AuditConfig, AuditFinding};
use crate::schema::AnnotatedSchema;

// Top-level audit entry point — runs every rule against the annotated
// snapshot, skipping anything the caller disabled via `config.disabled_rules`.
//
// Rules split into two groups based on what they need:
//   - DDL-only rules (naming, FK shape, duplicate indexes, …) read just
//     `annotated.schema`. They worked fine before the snapshot split and
//     they keep working — we hand them the schema reference directly.
//   - Stats-aware rules (`indexes/bloated`, `vacuum/large_table_defaults`)
//     need planner sizing or activity counters. They take the full
//     `&AnnotatedSchema` and use accessors like `index_sizing()` /
//     `reltuples()` so they're robust to "no stats captured yet" — they
//     simply produce no findings in that degenerate case rather than
//     panicking or lying.
#[must_use]
pub fn run_all_audit_rules(
    annotated: &AnnotatedSchema<'_>,
    config: &AuditConfig,
) -> Vec<AuditFinding> {
    let mut findings = Vec::new();
    let disabled = &config.disabled_rules;
    // Most rules just want DDL — pull the schema reference out once so
    // the per-rule sites stay readable.
    let snapshot = annotated.schema;

    macro_rules! run_rule {
        ($id:expr, $check:expr) => {
            if !disabled.iter().any(|d| d == $id) {
                findings.extend($check);
            }
        };
    }

    // ---- index rules ----
    run_rule!(
        "indexes/duplicate",
        indexes::check_duplicate_indexes(snapshot)
    );
    run_rule!(
        "indexes/redundant",
        indexes::check_redundant_indexes(snapshot)
    );
    run_rule!(
        "indexes/too_many",
        indexes::check_too_many_indexes(snapshot, config)
    );
    run_rule!(
        "indexes/wide_columns",
        indexes::check_wide_column_indexes(snapshot)
    );
    // bloated indexes need IndexSizing from the planner snapshot — gets
    // the annotated view, not the raw schema.
    run_rule!("indexes/bloated", indexes::check_bloated_indexes(annotated));

    // ---- FK rules ----
    run_rule!(
        "fk/type_mismatch",
        fk_graph::check_fk_type_mismatch(snapshot)
    );
    run_rule!("fk/circular", fk_graph::check_circular_fks(snapshot));
    run_rule!("fk/orphan", fk_graph::check_orphan_tables(snapshot));

    // ---- PK rules ----
    run_rule!(
        "pk/non_sequential",
        schema::check_pk_non_sequential(snapshot)
    );

    // ---- naming rules ----
    run_rule!("naming/bool_prefix", schema::check_bool_prefix(snapshot));
    run_rule!("naming/reserved", schema::check_reserved_words(snapshot));
    run_rule!("naming/id_mismatch", schema::check_id_mismatch(snapshot));

    // ---- documentation rules ----
    run_rule!(
        "docs/no_comment",
        schema::check_no_comment(snapshot, config)
    );

    // ---- storage rules ----
    // vacuum check needs reltuples from the planner — passes annotated.
    run_rule!(
        "vacuum/large_table_defaults",
        schema::check_vacuum_large_table_defaults(annotated)
    );

    findings
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    use crate::schema::*;
    use chrono::Utc;

    fn empty_schema() -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "test".into(),
            timestamp: Utc::now(),
            content_hash: "abc".into(),
            source: None,
            tables: vec![],
            enums: vec![],
            domains: vec![],
            composites: vec![],
            views: vec![],
            functions: vec![],
            extensions: vec![],
            gucs: vec![],
        }
    }

    // Build a stats-less annotated wrapper around a schema — mirrors
    // what the audit harness sees when no planner / activity rows exist
    // (e.g. fresh project, before the first `dryrun snapshot take`).
    fn ddl_only(schema: SchemaSnapshot) -> AnnotatedSnapshot {
        AnnotatedSnapshot {
            schema,
            planner: None,
            activity_by_node: BTreeMap::new(),
        }
    }

    #[test]
    fn empty_schema_produces_no_findings() {
        let config = AuditConfig::default();
        let snap = ddl_only(empty_schema());
        let findings = run_all_audit_rules(&snap.view(None), &config);
        assert!(findings.is_empty());
    }

    #[test]
    fn disabled_rules_are_skipped() {
        let schema = SchemaSnapshot {
            tables: vec![Table {
                oid: 0,
                schema: "public".into(),
                name: "user".into(),
                columns: vec![Column {
                    name: "id".into(),
                    ordinal: 0,
                    type_name: "bigint".into(),
                    nullable: false,
                    default: None,
                    identity: None,
                    generated: None,
                    comment: None,
                    statistics_target: None,
                }],
                constraints: vec![],
                indexes: vec![],
                comment: None,
                partition_info: None,
                policies: vec![],
                triggers: vec![],
                reloptions: vec![],
                rls_enabled: false,
            }],
            ..empty_schema()
        };
        let snap = ddl_only(schema);

        let config = AuditConfig::default();
        let findings = run_all_audit_rules(&snap.view(None), &config);
        assert!(findings.iter().any(|f| f.rule == "naming/reserved"));

        let config = AuditConfig {
            disabled_rules: vec!["naming/reserved".into()],
            ..AuditConfig::default()
        };
        let findings = run_all_audit_rules(&snap.view(None), &config);
        assert!(!findings.iter().any(|f| f.rule == "naming/reserved"));
    }
}
