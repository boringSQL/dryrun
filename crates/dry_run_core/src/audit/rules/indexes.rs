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
                if a.columns == b.columns && a.index_type == b.index_type {
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
                });
            }
        }
    }

    findings
}
