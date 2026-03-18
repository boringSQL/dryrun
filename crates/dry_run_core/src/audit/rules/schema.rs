use std::collections::{HashMap, HashSet};

use crate::audit::types::{AuditCategory, AuditConfig, AuditFinding};
use crate::lint::Severity;
use crate::schema::{ConstraintKind, SchemaSnapshot};

const UUID_TYPES: &[&str] = &["uuid"];

const BOOL_PREFIXES: &[&str] = &["is_", "has_", "can_", "should_", "was_", "will_"];

// Top ~50 most problematic SQL reserved words
const RESERVED_WORDS: &[&str] = &[
    "all", "alter", "and", "any", "as", "asc", "between", "by", "case", "check", "column",
    "constraint", "create", "cross", "current", "default", "delete", "desc", "distinct", "drop",
    "else", "end", "exists", "false", "fetch", "for", "foreign", "from", "full", "grant", "group",
    "having", "in", "index", "inner", "insert", "into", "is", "join", "key", "left", "like",
    "limit", "not", "null", "offset", "on", "or", "order", "outer", "primary", "references",
    "right", "select", "set", "table", "then", "to", "true", "union", "unique", "update", "user",
    "using", "values", "when", "where", "with",
];

#[must_use]
pub fn check_pk_non_sequential(schema: &SchemaSnapshot) -> Vec<AuditFinding> {
    let mut findings = Vec::new();

    for table in &schema.tables {
        let qualified = format!("{}.{}", table.schema, table.name);

        let pk_columns: Vec<&str> = table
            .constraints
            .iter()
            .filter(|c| c.kind == ConstraintKind::PrimaryKey)
            .flat_map(|c| c.columns.iter().map(|s| s.as_str()))
            .collect();

        for pk_col in &pk_columns {
            if let Some(col) = table.columns.iter().find(|c| c.name == *pk_col) {
                let normalized = col.type_name.to_lowercase();
                if UUID_TYPES.iter().any(|t| normalized.contains(t)) {
                    findings.push(AuditFinding {
                        rule: "pk/non_sequential".into(),
                        category: AuditCategory::PrimaryKeys,
                        severity: Severity::Info,
                        tables: vec![qualified.clone()],
                        message: format!(
                            "PK column '{}' uses UUID type — causes btree page splits and write amplification",
                            pk_col,
                        ),
                        recommendation: "Consider UUIDv7 (time-ordered) or bigint IDENTITY for better insert performance".into(),
                        ddl_fix: None,
                    });
                }
            }
        }
    }

    findings
}

#[must_use]
pub fn check_bool_prefix(schema: &SchemaSnapshot) -> Vec<AuditFinding> {
    let mut findings = Vec::new();

    for table in &schema.tables {
        let qualified = format!("{}.{}", table.schema, table.name);

        for col in &table.columns {
            let normalized = col.type_name.to_lowercase();
            if (normalized == "boolean" || normalized == "bool")
                && !BOOL_PREFIXES.iter().any(|p| col.name.starts_with(p))
            {
                findings.push(AuditFinding {
                    rule: "naming/bool_prefix".into(),
                    category: AuditCategory::Naming,
                    severity: Severity::Info,
                    tables: vec![qualified.clone()],
                    message: format!(
                        "Boolean column '{}' missing prefix (is_, has_, can_, ...)",
                        col.name,
                    ),
                    recommendation: format!(
                        "Rename to 'is_{}' or similar for clarity",
                        col.name,
                    ),
                    ddl_fix: Some(format!(
                        "ALTER TABLE {} RENAME COLUMN {} TO is_{};",
                        qualified, col.name, col.name,
                    )),
                });
            }
        }
    }

    findings
}

#[must_use]
pub fn check_reserved_words(schema: &SchemaSnapshot) -> Vec<AuditFinding> {
    let reserved: HashSet<&str> = RESERVED_WORDS.iter().copied().collect();
    let mut findings = Vec::new();

    for table in &schema.tables {
        let qualified = format!("{}.{}", table.schema, table.name);

        if reserved.contains(table.name.to_lowercase().as_str()) {
            findings.push(AuditFinding {
                rule: "naming/reserved".into(),
                category: AuditCategory::Naming,
                severity: Severity::Error,
                tables: vec![qualified.clone()],
                message: format!(
                    "Table name '{}' is a SQL reserved word — requires quoting everywhere",
                    table.name,
                ),
                recommendation: format!("Rename table '{}' to avoid quoting issues", table.name),
                ddl_fix: None,
            });
        }

        for col in &table.columns {
            if reserved.contains(col.name.to_lowercase().as_str()) {
                findings.push(AuditFinding {
                    rule: "naming/reserved".into(),
                    category: AuditCategory::Naming,
                    severity: Severity::Error,
                    tables: vec![qualified.clone()],
                    message: format!(
                        "Column '{}' in table '{}' is a SQL reserved word",
                        col.name, table.name,
                    ),
                    recommendation: format!(
                        "Rename column '{}' to avoid quoting hell",
                        col.name,
                    ),
                    ddl_fix: None,
                });
            }
        }
    }

    findings
}

// Cross-table check: same FK target referenced with inconsistent column names
#[must_use]
pub fn check_id_mismatch(schema: &SchemaSnapshot) -> Vec<AuditFinding> {
    let mut findings = Vec::new();

    // Build map: referenced_table -> set of (fk_column_name, source_table)
    let mut ref_names: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();

    for table in &schema.tables {
        let qualified = format!("{}.{}", table.schema, table.name);

        for constraint in &table.constraints {
            if constraint.kind != ConstraintKind::ForeignKey {
                continue;
            }
            let Some(ref fk_table) = constraint.fk_table else {
                continue;
            };

            // for single-column FKs, track the column name used
            if constraint.columns.len() == 1 {
                ref_names
                    .entry(fk_table.clone())
                    .or_default()
                    .entry(constraint.columns[0].clone())
                    .or_default()
                    .push(qualified.clone());
            }
        }
    }

    for (target_table, name_map) in &ref_names {
        if name_map.len() > 1 {
            let names: Vec<&String> = name_map.keys().collect();
            let mut all_tables: Vec<String> = Vec::new();
            let mut details = Vec::new();
            for (col_name, source_tables) in name_map {
                for src in source_tables {
                    details.push(format!("{src}.{col_name}"));
                    if !all_tables.contains(src) {
                        all_tables.push(src.clone());
                    }
                }
            }

            findings.push(AuditFinding {
                rule: "naming/id_mismatch".into(),
                category: AuditCategory::Naming,
                severity: Severity::Warning,
                tables: all_tables,
                message: format!(
                    "Table '{}' referenced inconsistently: {} used as FK column names",
                    target_table,
                    names.iter().map(|n| format!("'{n}'")).collect::<Vec<_>>().join(", "),
                ),
                recommendation: "Standardize FK column naming for consistency".into(),
                ddl_fix: None,
            });
        }
    }

    findings
}

#[must_use]
pub fn check_no_comment(schema: &SchemaSnapshot, config: &AuditConfig) -> Vec<AuditFinding> {
    let mut findings = Vec::new();

    for table in &schema.tables {
        if table.columns.len() < config.no_comment_min_columns {
            continue;
        }

        let qualified = format!("{}.{}", table.schema, table.name);

        // check table-level comment
        if table.comment.is_none() {
            findings.push(AuditFinding {
                rule: "docs/no_comment".into(),
                category: AuditCategory::Documentation,
                severity: Severity::Info,
                tables: vec![qualified.clone()],
                message: format!(
                    "Table '{}' has {} columns but no table comment",
                    table.name,
                    table.columns.len(),
                ),
                recommendation: format!(
                    "Add comment: COMMENT ON TABLE {} IS '...';",
                    qualified,
                ),
                ddl_fix: None,
            });
        }

        // check columns without comment
        let uncommented: Vec<&str> = table
            .columns
            .iter()
            .filter(|c| c.comment.is_none())
            .map(|c| c.name.as_str())
            .collect();

        if !uncommented.is_empty() {
            findings.push(AuditFinding {
                rule: "docs/no_comment".into(),
                category: AuditCategory::Documentation,
                severity: Severity::Info,
                tables: vec![qualified.clone()],
                message: format!(
                    "{} column(s) in '{}' have no comment: {}",
                    uncommented.len(),
                    table.name,
                    if uncommented.len() <= 5 {
                        uncommented.join(", ")
                    } else {
                        format!("{}, ... and {} more", uncommented[..3].join(", "), uncommented.len() - 3)
                    },
                ),
                recommendation: "Add COMMENT ON COLUMN for documentation".into(),
                ddl_fix: None,
            });
        }
    }

    findings
}
