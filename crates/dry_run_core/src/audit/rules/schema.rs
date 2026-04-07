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
                        min_pg_version: None,
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
                    min_pg_version: None,
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
                min_pg_version: None,
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
                    min_pg_version: None,
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
                min_pg_version: None,
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
                min_pg_version: None,
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
                min_pg_version: None,
            });
        }
    }

    findings
}

#[must_use]
pub fn check_vacuum_large_table_defaults(schema: &SchemaSnapshot) -> Vec<AuditFinding> {
    use crate::schema::effective_table_stats;

    let mut findings = Vec::new();

    for table in &schema.tables {
        let stats = match effective_table_stats(table, schema) {
            Some(s) if s.reltuples >= 1_000_000.0 => s,
            _ => continue,
        };

        let has_overrides = table
            .reloptions
            .iter()
            .any(|opt| opt.starts_with("autovacuum_"));

        if !has_overrides {
            let qualified = format!("{}.{}", table.schema, table.name);

            let mut vac_sf = 100_000.0 / stats.reltuples;
            vac_sf = (vac_sf * 1000.0).round() / 1000.0;
            if vac_sf < 0.001 {
                vac_sf = 0.001;
            }
            let az_sf = (vac_sf / 2.0 * 1000.0).round() / 1000.0;
            let vac_thresh = ((stats.reltuples * 0.01) as i64).clamp(500, 5000);
            let az_thresh = (vac_thresh / 2).max(250);

            findings.push(AuditFinding {
                rule: "vacuum/large_table_defaults".into(),
                category: AuditCategory::Storage,
                severity: Severity::Info,
                tables: vec![qualified.clone()],
                message: format!(
                    "'{}' has {}M rows but uses default autovacuum settings",
                    qualified,
                    stats.reltuples as i64 / 1_000_000
                ),
                recommendation: format!(
                    "consider tuning autovacuum for large tables — \
                     lower scale factors alone aren't enough without explicit thresholds"
                ),
                ddl_fix: Some(format!(
                    "ALTER TABLE {qualified} SET (\n  \
                       autovacuum_vacuum_scale_factor = {vac_sf},\n  \
                       autovacuum_vacuum_threshold = {vac_thresh},\n  \
                       autovacuum_analyze_scale_factor = {az_sf},\n  \
                       autovacuum_analyze_threshold = {az_thresh}\n\
                     );"
                )),
                min_pg_version: None,
            });
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
            nullable: false, default: None, identity: None, generated: None, comment: None, statistics_target: None, stats: None,
        }
    }

    fn make_col_with_comment(name: &str, type_name: &str, comment: &str) -> Column {
        Column {
            name: name.into(), ordinal: 0, type_name: type_name.into(),
            nullable: false, default: None, identity: None, generated: None,
            comment: Some(comment.into()), statistics_target: None, stats: None,
        }
    }

    fn make_pk(name: &str, columns: &[&str]) -> Constraint {
        Constraint {
            name: name.into(), kind: ConstraintKind::PrimaryKey,
            columns: columns.iter().map(|s| s.to_string()).collect(),
            definition: None, fk_table: None, fk_columns: vec![], backing_index: None, comment: None,
        }
    }

    fn make_fk(name: &str, columns: &[&str], fk_table: &str, fk_columns: &[&str]) -> Constraint {
        Constraint {
            name: name.into(), kind: ConstraintKind::ForeignKey,
            columns: columns.iter().map(|s| s.to_string()).collect(),
            definition: None, fk_table: Some(fk_table.into()),
            fk_columns: fk_columns.iter().map(|s| s.to_string()).collect(),
            backing_index: None, comment: None,
        }
    }

    fn make_table(name: &str, columns: Vec<Column>, constraints: Vec<Constraint>) -> Table {
        Table {
            oid: 0, schema: "public".into(), name: name.into(),
            columns, constraints, indexes: vec![],
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
    fn detects_uuid_pk() {
        let schema = schema_with(vec![make_table(
            "events",
            vec![make_col("event_id", "uuid"), make_col("data", "jsonb")],
            vec![make_pk("pk_events", &["event_id"])],
        )]);
        let findings = check_pk_non_sequential(&schema);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].rule, "pk/non_sequential");
    }

    #[test]
    fn no_finding_for_bigint_pk() {
        let schema = schema_with(vec![make_table(
            "users",
            vec![make_col("user_id", "bigint")],
            vec![make_pk("pk_users", &["user_id"])],
        )]);
        let findings = check_pk_non_sequential(&schema);
        assert!(findings.is_empty());
    }

    #[test]
    fn detects_bool_without_prefix() {
        let schema = schema_with(vec![make_table(
            "users",
            vec![
                make_col("id", "bigint"),
                make_col("active", "boolean"),
                make_col("is_verified", "boolean"),
            ],
            vec![],
        )]);
        let findings = check_bool_prefix(&schema);
        assert_eq!(findings.len(), 1);
        assert!(findings[0].message.contains("active"));
    }

    #[test]
    fn no_finding_for_prefixed_bool() {
        let schema = schema_with(vec![make_table(
            "users",
            vec![
                make_col("id", "bigint"),
                make_col("is_active", "bool"),
                make_col("has_avatar", "boolean"),
            ],
            vec![],
        )]);
        let findings = check_bool_prefix(&schema);
        assert!(findings.is_empty());
    }

    #[test]
    fn detects_reserved_table_name() {
        let schema = schema_with(vec![make_table(
            "user",
            vec![make_col("id", "bigint")],
            vec![],
        )]);
        let findings = check_reserved_words(&schema);
        assert_eq!(findings.len(), 1);
        assert!(findings[0].message.contains("user"));
    }

    #[test]
    fn detects_reserved_column_name() {
        let schema = schema_with(vec![make_table(
            "accounts",
            vec![make_col("id", "bigint"), make_col("order", "integer")],
            vec![],
        )]);
        let findings = check_reserved_words(&schema);
        assert_eq!(findings.len(), 1);
        assert!(findings[0].message.contains("order"));
    }

    #[test]
    fn detects_inconsistent_fk_naming() {
        let schema = schema_with(vec![
            make_table(
                "users",
                vec![make_col("user_id", "bigint")],
                vec![],
            ),
            make_table(
                "orders",
                vec![make_col("id", "bigint"), make_col("user_id", "bigint")],
                vec![make_fk("fk_orders_user", &["user_id"], "public.users", &["user_id"])],
            ),
            make_table(
                "comments",
                vec![make_col("id", "bigint"), make_col("uid", "bigint")],
                vec![make_fk("fk_comments_user", &["uid"], "public.users", &["user_id"])],
            ),
        ]);
        let findings = check_id_mismatch(&schema);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].rule, "naming/id_mismatch");
    }

    #[test]
    fn no_mismatch_when_consistent() {
        let schema = schema_with(vec![
            make_table("users", vec![make_col("user_id", "bigint")], vec![]),
            make_table(
                "orders",
                vec![make_col("id", "bigint"), make_col("user_id", "bigint")],
                vec![make_fk("fk_o", &["user_id"], "public.users", &["user_id"])],
            ),
            make_table(
                "comments",
                vec![make_col("id", "bigint"), make_col("user_id", "bigint")],
                vec![make_fk("fk_c", &["user_id"], "public.users", &["user_id"])],
            ),
        ]);
        let findings = check_id_mismatch(&schema);
        assert!(findings.is_empty());
    }

    #[test]
    fn detects_no_comment_on_large_table() {
        let schema = schema_with(vec![make_table(
            "orders",
            vec![
                make_col("id", "bigint"),
                make_col("user_id", "bigint"),
                make_col("status", "text"),
                make_col("total", "numeric"),
                make_col("created_at", "timestamptz"),
            ],
            vec![],
        )]);
        let config = AuditConfig::default();
        let findings = check_no_comment(&schema, &config);
        assert!(findings.len() >= 2);
        assert!(findings.iter().all(|f| f.rule == "docs/no_comment"));
    }

    #[test]
    fn skips_small_tables_for_comments() {
        let schema = schema_with(vec![make_table(
            "config",
            vec![
                make_col("key", "text"),
                make_col("value", "text"),
            ],
            vec![],
        )]);
        let config = AuditConfig::default();
        let findings = check_no_comment(&schema, &config);
        assert!(findings.is_empty(), "tables with < 5 columns should be skipped");
    }

    #[test]
    fn no_finding_when_comments_present() {
        let mut table = make_table(
            "orders",
            vec![
                make_col_with_comment("id", "bigint", "primary key"),
                make_col_with_comment("user_id", "bigint", "owner"),
                make_col_with_comment("status", "text", "order status"),
                make_col_with_comment("total", "numeric", "total amount"),
                make_col_with_comment("created_at", "timestamptz", "creation time"),
            ],
            vec![],
        );
        table.comment = Some("customer orders".into());
        let schema = schema_with(vec![table]);
        let config = AuditConfig::default();
        let findings = check_no_comment(&schema, &config);
        assert!(findings.is_empty());
    }
}
