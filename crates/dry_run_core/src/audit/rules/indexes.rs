use crate::audit::types::{AuditCategory, AuditConfig, AuditFinding};
use crate::lint::Severity;
use crate::schema::SchemaSnapshot;
const WIDE_TYPES: &[&str] = &["text", "varchar", "bytea", "jsonb", "json", "xml"];

#[must_use]
pub fn check_duplicate_indexes(schema: &SchemaSnapshot) -> Vec<AuditFinding> {
    let mut findings = Vec::new();

    for table in &schema.tables {
        let qualified = format!("{}.{}", table.schema, table.name);
        let non_primary: Vec<_> = table
            .indexes
            .iter()
            .filter(|idx| !idx.is_primary)
            .collect();

        for (i, a) in non_primary.iter().enumerate() {
            for b in non_primary.iter().skip(i + 1) {
                if a.columns != b.columns
                    || a.index_type != b.index_type
                    || a.predicate != b.predicate
                    || a.include_columns != b.include_columns
                {
                    continue;
                }

                if a.is_unique == b.is_unique {
                    // both back constraints — neither can be simply dropped;
                    // one owns a UNIQUE/PK constraint, the other is used by a FK.
                    // flag it but without a one-liner DDL fix
                    if a.backs_constraint && b.backs_constraint {
                        findings.push(AuditFinding {
                            rule: "indexes/duplicate".into(),
                            category: AuditCategory::Indexes,
                            severity: Severity::Warning,
                            tables: vec![qualified.clone()],
                            message: format!(
                                "Indexes '{}' and '{}' have identical columns [{}] but both back constraints",
                                a.name, b.name, a.columns.join(", "),
                            ),
                            recommendation: format!(
                                "One index is redundant but a FK depends on it — \
                                 drop the FK first, then the extra index, then re-create the FK \
                                 so PG picks the remaining index"
                            ),
                            ddl_fix: None,
                            min_pg_version: None,
                        });
                        continue;
                    }

                    // drop the one that does NOT back a constraint
                    let (to_drop, to_keep) = match (a.backs_constraint, b.backs_constraint) {
                        (true, false) => (b, a),
                        (false, true) => (a, b),
                        // neither backs a constraint — pick 2nd (b) to drop
                        _ => (b, a),
                    };
                    findings.push(AuditFinding {
                        rule: "indexes/duplicate".into(),
                        category: AuditCategory::Indexes,
                        severity: Severity::Error,
                        tables: vec![qualified.clone()],
                        message: format!(
                            "Indexes '{}' and '{}' have identical columns: [{}]",
                            to_drop.name,
                            to_keep.name,
                            a.columns.join(", "),
                        ),
                        recommendation: format!(
                            "Drop '{}' — '{}'{}",
                            to_drop.name,
                            to_keep.name,
                            if to_keep.backs_constraint { " backs a constraint" } else { " is sufficient" },
                        ),
                        ddl_fix: Some(format!("DROP INDEX {};", to_drop.name)),
                        min_pg_version: None,
                    });
                } else {
                    // one unique, one not — the non-unique is redundant
                    let (non_uniq, uniq) = if a.is_unique { (b, a) } else { (a, b) };
                    findings.push(AuditFinding {
                        rule: "indexes/duplicate".into(),
                        category: AuditCategory::Indexes,
                        severity: Severity::Warning,
                        tables: vec![qualified.clone()],
                        message: format!(
                            "Non-unique index '{}' is redundant — the unique index '{}' already covers these lookups: [{}]",
                            non_uniq.name,
                            uniq.name,
                            a.columns.join(", "),
                        ),
                        recommendation: format!(
                            "Non-unique index '{}' is redundant — the unique index '{}' already covers these lookups",
                            non_uniq.name, uniq.name,
                        ),
                        ddl_fix: Some(format!("DROP INDEX {};", non_uniq.name)),
                        min_pg_version: None,
                    });
                }
            }
        }
    }

    findings
}

#[must_use]
pub fn check_redundant_indexes(schema: &SchemaSnapshot) -> Vec<AuditFinding> {
    let mut findings = Vec::new();

    for table in &schema.tables {
        let qualified = format!("{}.{}", table.schema, table.name);
        // only btree, skip partial indexes
        let btree: Vec<_> = table
            .indexes
            .iter()
            .filter(|idx| !idx.is_primary && idx.index_type == "btree" && idx.predicate.is_none())
            .collect();

        for a in &btree {
            for b in &btree {
                if std::ptr::eq(*a, *b) {
                    continue;
                }
                // a is redundant if a's columns are strict prefix of b's columns
                if a.columns.len() < b.columns.len()
                    && b.columns.starts_with(&a.columns)
                    && !a.is_unique
                {
                    findings.push(AuditFinding {
                        rule: "indexes/redundant".into(),
                        category: AuditCategory::Indexes,
                        severity: Severity::Warning,
                        tables: vec![qualified.clone()],
                        message: format!(
                            "'{}' [{}] is a prefix of '{}' [{}]",
                            a.name,
                            a.columns.join(", "),
                            b.name,
                            b.columns.join(", "),
                        ),
                        recommendation: format!(
                            "Index '{}' is redundant — the wider index '{}' covers same queries",
                            a.name, b.name,
                        ),
                        ddl_fix: Some(format!("DROP INDEX {};", a.name)),
                        min_pg_version: None,
                    });
                }
            }
        }
    }

    findings
}

#[must_use]
pub fn check_too_many_indexes(schema: &SchemaSnapshot, config: &AuditConfig) -> Vec<AuditFinding> {
    let mut findings = Vec::new();

    for table in &schema.tables {
        if table.indexes.len() > config.max_indexes_per_table {
            let qualified = format!("{}.{}", table.schema, table.name);
            findings.push(AuditFinding {
                rule: "indexes/too_many".into(),
                category: AuditCategory::Indexes,
                severity: Severity::Info,
                tables: vec![qualified],
                message: format!(
                    "Table has {} indexes (threshold: {}) — write amplification risk",
                    table.indexes.len(),
                    config.max_indexes_per_table,
                ),
                recommendation: "Review indexes for unused or redundant ones".into(),
                ddl_fix: None,
                min_pg_version: None,
            });
        }
    }

    findings
}

#[must_use]
pub fn check_wide_column_indexes(schema: &SchemaSnapshot) -> Vec<AuditFinding> {
    let mut findings = Vec::new();

    for table in &schema.tables {
        let qualified = format!("{}.{}", table.schema, table.name);
        let col_types: std::collections::HashMap<&str, &str> = table
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.type_name.as_str()))
            .collect();

        for idx in &table.indexes {
            let wide_cols: Vec<&str> = idx
                .columns
                .iter()
                .filter(|col_name| {
                    col_types
                        .get(col_name.as_str())
                        .is_some_and(|t| WIDE_TYPES.iter().any(|w| t.starts_with(w)))
                })
                .map(|s| s.as_str())
                .collect();

            if !wide_cols.is_empty() {
                findings.push(AuditFinding {
                    rule: "indexes/wide_columns".into(),
                    category: AuditCategory::Indexes,
                    severity: Severity::Warning,
                    tables: vec![qualified.clone()],
                    message: format!(
                        "Index '{}' includes wide column(s): [{}] — bloated index pages",
                        idx.name,
                        wide_cols.join(", "),
                    ),
                    recommendation:
                        "Consider expression index, prefix index, or hash index instead".into(),
                    ddl_fix: None,
                    min_pg_version: None,
                });
            }
        }
    }

    findings
}

const DEFAULT_BLOAT_THRESHOLD: f64 = 1.5;

#[must_use]
pub fn check_bloated_indexes(schema: &SchemaSnapshot) -> Vec<AuditFinding> {
    let mut findings = Vec::new();

    for table in &schema.tables {
        let qualified = format!("{}.{}", table.schema, table.name);
        for idx in &table.indexes {
            if let Some(est) = crate::schema::bloat::estimate_index_bloat(idx, table) {
                if est.bloat_ratio > DEFAULT_BLOAT_THRESHOLD {
                    findings.push(AuditFinding {
                        rule: "indexes/bloated".into(),
                        category: AuditCategory::Storage,
                        severity: Severity::Warning,
                        tables: vec![qualified.clone()],
                        message: format!(
                            "index '{}' on '{}' has estimated bloat ratio {:.1}x ({} actual pages vs {} expected)",
                            idx.name, qualified, est.bloat_ratio, est.actual_pages, est.expected_pages
                        ),
                        recommendation: format!("REINDEX INDEX CONCURRENTLY {};", idx.name),
                        ddl_fix: Some(format!("REINDEX INDEX CONCURRENTLY {};", idx.name)),
                        min_pg_version: None,
                    });
                }
            }
        }
    }

    findings
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::*;
    use chrono::Utc;

    fn make_col(name: &str, type_name: &str) -> Column {
        Column {
            name: name.into(), ordinal: 0, type_name: type_name.into(),
            nullable: false, default: None, identity: None, comment: None, stats: None,
        }
    }

    fn make_index(name: &str, columns: &[&str]) -> Index {
        Index {
            name: name.into(),
            columns: columns.iter().map(|s| s.to_string()).collect(),
            include_columns: vec![], index_type: "btree".into(),
            is_unique: false, is_primary: false, predicate: None,
            definition: format!("CREATE INDEX {name} ON ..."),
            is_valid: true,
            backs_constraint: false,
            stats: None,
        }
    }

    fn make_table_with(
        name: &str,
        columns: Vec<Column>,
        indexes: Vec<Index>,
    ) -> Table {
        Table {
            oid: 0, schema: "public".into(), name: name.into(),
            columns, constraints: vec![], indexes,
            comment: None, stats: None, partition_info: None,
            policies: vec![], triggers: vec![], reloptions: vec![], rls_enabled: false,
        }
    }

    fn schema_with(tables: Vec<Table>) -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "PostgreSQL 17.0".into(), database: "test".into(),
            timestamp: Utc::now(), content_hash: "abc".into(), source: None,
            tables, enums: vec![], domains: vec![], composites: vec![],
            views: vec![], functions: vec![], extensions: vec![], gucs: vec![],
            node_stats: vec![],
        }
    }

    #[test]
    fn detects_duplicate_indexes() {
        let schema = schema_with(vec![make_table_with(
            "orders",
            vec![make_col("user_id", "bigint"), make_col("status", "text")],
            vec![
                make_index("idx_orders_user_1", &["user_id"]),
                make_index("idx_orders_user_2", &["user_id"]),
            ],
        )]);
        let findings = check_duplicate_indexes(&schema);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].rule, "indexes/duplicate");
        assert_eq!(findings[0].severity, Severity::Error);
    }

    #[test]
    fn no_duplicate_when_columns_differ() {
        let schema = schema_with(vec![make_table_with(
            "orders",
            vec![make_col("user_id", "bigint"), make_col("status", "text")],
            vec![
                make_index("idx_a", &["user_id"]),
                make_index("idx_b", &["status"]),
            ],
        )]);
        let findings = check_duplicate_indexes(&schema);
        assert!(findings.is_empty());
    }

    #[test]
    fn detects_redundant_prefix_index() {
        let schema = schema_with(vec![make_table_with(
            "orders",
            vec![make_col("user_id", "bigint"), make_col("status", "text")],
            vec![
                make_index("idx_user", &["user_id"]),
                make_index("idx_user_status", &["user_id", "status"]),
            ],
        )]);
        let findings = check_redundant_indexes(&schema);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].rule, "indexes/redundant");
    }

    #[test]
    fn skips_partial_indexes_for_redundancy() {
        let mut partial = make_index("idx_user_active", &["user_id"]);
        partial.predicate = Some("status = 'active'".into());
        let schema = schema_with(vec![make_table_with(
            "orders",
            vec![make_col("user_id", "bigint"), make_col("status", "text")],
            vec![
                partial,
                make_index("idx_user_status", &["user_id", "status"]),
            ],
        )]);
        let findings = check_redundant_indexes(&schema);
        assert!(findings.is_empty());
    }

    #[test]
    fn skips_unique_prefix_index_for_redundancy() {
        let mut unique = make_index("idx_task_project_uniq", &["planned_task_id", "project_id"]);
        unique.is_unique = true;
        let schema = schema_with(vec![make_table_with(
            "assignments",
            vec![
                make_col("planned_task_id", "bigint"),
                make_col("project_id", "bigint"),
                make_col("workspace_id", "bigint"),
            ],
            vec![
                unique,
                make_index(
                    "idx_task_project_workspace",
                    &["planned_task_id", "project_id", "workspace_id"],
                ),
            ],
        )]);
        let findings = check_redundant_indexes(&schema);
        assert!(findings.is_empty());
    }

    #[test]
    fn detects_too_many_indexes() {
        let cols = vec![make_col("id", "bigint")];
        let indexes: Vec<_> = (0..12)
            .map(|i| make_index(&format!("idx_{i}"), &["id"]))
            .collect();
        let schema = schema_with(vec![make_table_with("big_table", cols, indexes)]);
        let config = AuditConfig::default();
        let findings = check_too_many_indexes(&schema, &config);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].rule, "indexes/too_many");
    }

    #[test]
    fn detects_wide_column_index() {
        let schema = schema_with(vec![make_table_with(
            "posts",
            vec![make_col("body", "text"), make_col("metadata", "jsonb")],
            vec![make_index("idx_body", &["body"])],
        )]);
        let findings = check_wide_column_indexes(&schema);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].rule, "indexes/wide_columns");
    }

    #[test]
    fn no_wide_column_for_integer_indexes() {
        let schema = schema_with(vec![make_table_with(
            "posts",
            vec![make_col("user_id", "bigint")],
            vec![make_index("idx_user", &["user_id"])],
        )]);
        let findings = check_wide_column_indexes(&schema);
        assert!(findings.is_empty());
    }

    #[test]
    fn no_duplicate_when_predicates_differ() {
        let mut partial = make_index("idx_user_active", &["user_id"]);
        partial.predicate = Some("status = 'active'".into());
        let schema = schema_with(vec![make_table_with(
            "orders",
            vec![make_col("user_id", "bigint"), make_col("status", "text")],
            vec![
                make_index("idx_user_all", &["user_id"]),
                partial,
            ],
        )]);
        let findings = check_duplicate_indexes(&schema);
        assert!(findings.is_empty());
    }

    #[test]
    fn detects_nonunique_redundant_with_unique() {
        let mut unique = make_index("idx_user_uniq", &["user_id"]);
        unique.is_unique = true;
        let schema = schema_with(vec![make_table_with(
            "orders",
            vec![make_col("user_id", "bigint")],
            vec![
                make_index("idx_user_plain", &["user_id"]),
                unique,
            ],
        )]);
        let findings = check_duplicate_indexes(&schema);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].severity, Severity::Warning);
        assert!(findings[0].message.contains("Non-unique index 'idx_user_plain'"));
        assert!(findings[0].message.contains("unique index 'idx_user_uniq'"));
        assert_eq!(findings[0].ddl_fix.as_deref(), Some("DROP INDEX idx_user_plain;"));
    }

    #[test]
    fn check_duplicate_nonunique_redundant_with_partial_unique() {
        // non-unique with predicate matching unique with same predicate
        let mut unique = make_index("workspace_name_uniq", &["workspace_id", "name"]);
        unique.is_unique = true;
        unique.predicate = Some("deleted_at IS NULL".into());
        let mut plain = make_index("idx_workspace_name", &["workspace_id", "name"]);
        plain.predicate = Some("deleted_at IS NULL".into());
        let schema = schema_with(vec![make_table_with(
            "client_workspaces",
            vec![
                make_col("workspace_id", "bigint"),
                make_col("name", "bigint"),
                make_col("deleted_at", "timestamptz"),
            ],
            vec![plain, unique],
        )]);
        let findings = check_duplicate_indexes(&schema);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].severity, Severity::Warning);
        assert_eq!(findings[0].ddl_fix.as_deref(), Some("DROP INDEX idx_workspace_name;"));
    }

    #[test]
    fn no_duplicate_when_include_columns_differ() {
        let mut covering = make_index("idx_user_cover", &["user_id"]);
        covering.include_columns = vec!["status".into()];
        let schema = schema_with(vec![make_table_with(
            "orders",
            vec![make_col("user_id", "bigint"), make_col("status", "text")],
            vec![
                make_index("idx_user_plain", &["user_id"]),
                covering,
            ],
        )]);
        let findings = check_duplicate_indexes(&schema);
        assert!(findings.is_empty());
    }

    #[test]
    fn duplicate_drops_non_constraint_index() {
        // unique_task_id_workspace_id backs the UNIQUE constraint,
        // idx_unique_task_id_workspace_id is the redundant copy —
        // the DDL must drop the copy, not the constraint-backing index
        let mut constraint_idx = make_index("unique_task_id_workspace_id", &["workspace_id", "id"]);
        constraint_idx.is_unique = true;
        constraint_idx.backs_constraint = true;

        let mut copy_idx = make_index("idx_unique_task_id_workspace_id", &["workspace_id", "id"]);
        copy_idx.is_unique = true;
        let schema = schema_with(vec![make_table_with(
            "task",
            vec![make_col("workspace_id", "bigint"), make_col("id", "bigint")],
            vec![constraint_idx, copy_idx],
        )]);

        let findings = check_duplicate_indexes(&schema);
        assert_eq!(findings.len(), 1);
        assert_eq!(
            findings[0].ddl_fix.as_deref(),
            Some("DROP INDEX idx_unique_task_id_workspace_id;"),
            "must drop the copy, not the constraint-backing index"
        );
    }

    #[test]
    fn both_back_constraints_warns_without_ddl_fix() {
        // one index owns a UNIQUE constraint, the other is used by a FK —
        // neither can be simply dropped, needs FK drop+recreate
        let mut constraint_idx = make_index("unique_status_id_workspace_id", &["workspace_id", "id"]);
        constraint_idx.is_unique = true;
        constraint_idx.backs_constraint = true;

        let mut fk_used_idx = make_index("idx_unique_status_id_workspace_id", &["workspace_id", "id"]);
        fk_used_idx.is_unique = true;
        fk_used_idx.backs_constraint = true;

        let schema = schema_with(vec![make_table_with(
            "status",
            vec![make_col("workspace_id", "bigint"), make_col("id", "bigint")],
            vec![constraint_idx, fk_used_idx],
        )]);

        let findings = check_duplicate_indexes(&schema);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].severity, Severity::Warning);
        assert!(findings[0].ddl_fix.is_none(), "no simple DDL fix when both back constraints");
    }

    #[test]
    fn still_detects_duplicate_with_same_predicate() {
        let mut a = make_index("idx_user_active_1", &["user_id"]);
        a.predicate = Some("status = 'active'".into());
        let mut b = make_index("idx_user_active_2", &["user_id"]);
        b.predicate = Some("status = 'active'".into());
        let schema = schema_with(vec![make_table_with(
            "orders",
            vec![make_col("user_id", "bigint"), make_col("status", "text")],
            vec![a, b],
        )]);
        let findings = check_duplicate_indexes(&schema);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].rule, "indexes/duplicate");
    }
}
