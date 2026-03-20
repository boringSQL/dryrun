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
                if a.columns == b.columns
                    && a.index_type == b.index_type
                    && a.predicate == b.predicate
                    && a.is_unique == b.is_unique
                    && a.include_columns == b.include_columns
                {
                    findings.push(AuditFinding {
                        rule: "indexes/duplicate".into(),
                        category: AuditCategory::Indexes,
                        severity: Severity::Error,
                        tables: vec![qualified.clone()],
                        message: format!(
                            "Indexes '{}' and '{}' have identical columns: [{}]",
                            a.name,
                            b.name,
                            a.columns.join(", "),
                        ),
                        recommendation: "Drop one of the duplicate indexes".into(),
                        ddl_fix: Some(format!("DROP INDEX {};", b.name)),
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
            policies: vec![], triggers: vec![], rls_enabled: false,
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
}
