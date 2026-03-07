mod rules;
mod types;

pub use types::{LintConfig, LintReport, LintSummary, LintViolation, Severity};

use crate::schema::SchemaSnapshot;

pub fn lint_schema(schema: &SchemaSnapshot, config: &LintConfig) -> LintReport {
    let tables_checked = schema.tables.len();
    let violations = rules::run_all_rules(schema, config);
    let config_source = if config.disabled_rules.is_empty() {
        "default (boringsql)".into()
    } else {
        format!("custom ({} rules disabled)", config.disabled_rules.len())
    };
    LintReport::new(violations, tables_checked, config_source)
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::schema::*;

    fn empty_snapshot() -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "test".into(),
            timestamp: Utc::now(),
            content_hash: "abc".into(),
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

    fn make_table(
        name: &str,
        columns: Vec<Column>,
        constraints: Vec<Constraint>,
        indexes: Vec<Index>,
    ) -> Table {
        Table {
            oid: 0,
            schema: "public".into(),
            name: name.into(),
            columns,
            constraints,
            indexes,
            comment: None,
            stats: None,
            partition_info: None,
            policies: vec![],
            triggers: vec![],
            rls_enabled: false,
        }
    }

    fn make_col(name: &str, type_name: &str) -> Column {
        Column {
            name: name.into(),
            ordinal: 0,
            type_name: type_name.into(),
            nullable: false,
            default: None,
            identity: None,
            comment: None,
            stats: None,
        }
    }

    fn make_pk(name: &str, columns: &[&str]) -> Constraint {
        Constraint {
            name: name.into(),
            kind: ConstraintKind::PrimaryKey,
            columns: columns.iter().map(|s| s.to_string()).collect(),
            definition: None,
            fk_table: None,
            fk_columns: vec![],
            comment: None,
        }
    }

    fn make_fk(name: &str, columns: &[&str], fk_table: &str) -> Constraint {
        Constraint {
            name: name.into(),
            kind: ConstraintKind::ForeignKey,
            columns: columns.iter().map(|s| s.to_string()).collect(),
            definition: None,
            fk_table: Some(fk_table.into()),
            fk_columns: vec!["id".into()],
            comment: None,
        }
    }

    fn make_index(name: &str, columns: &[&str]) -> Index {
        Index {
            name: name.into(),
            columns: columns.iter().map(|s| s.to_string()).collect(),
            include_columns: vec![],
            index_type: "btree".into(),
            is_unique: false,
            is_primary: false,
            predicate: None,
            definition: format!("CREATE INDEX {} ON ...", name),
        }
    }

    #[test]
    fn clean_schema_no_violations() {
        let mut snapshot = empty_snapshot();
        let mut id_col = make_col("id", "bigint");
        id_col.identity = Some("ALWAYS".into());
        let created = make_col("created_at", "timestamp with time zone");
        let updated = make_col("updated_at", "timestamp with time zone");

        snapshot.tables.push(make_table(
            "user",
            vec![id_col, make_col("email", "text"), created, updated],
            vec![make_pk("pk_user", &["id"])],
            vec![],
        ));

        let report = lint_schema(&snapshot, &LintConfig::default());
        let errors: Vec<_> = report
            .violations
            .iter()
            .filter(|v| v.severity == Severity::Error)
            .collect();
        assert!(
            errors.is_empty(),
            "clean schema should have no errors: {:?}",
            errors
        );
    }

    #[test]
    fn missing_pk_is_error() {
        let mut snapshot = empty_snapshot();
        snapshot.tables.push(make_table(
            "log",
            vec![
                make_col("message", "text"),
                make_col("created_at", "timestamp with time zone"),
                make_col("updated_at", "timestamp with time zone"),
            ],
            vec![],
            vec![],
        ));

        let report = lint_schema(&snapshot, &LintConfig::default());
        assert!(report.violations.iter().any(|v| v.rule == "pk/exists"));
    }

    #[test]
    fn varchar_flagged() {
        let mut snapshot = empty_snapshot();
        let mut id_col = make_col("id", "bigint");
        id_col.identity = Some("ALWAYS".into());
        snapshot.tables.push(make_table(
            "item",
            vec![
                id_col,
                make_col("name", "character varying(100)"),
                make_col("created_at", "timestamp with time zone"),
                make_col("updated_at", "timestamp with time zone"),
            ],
            vec![make_pk("pk_item", &["id"])],
            vec![],
        ));

        let report = lint_schema(&snapshot, &LintConfig::default());
        assert!(report
            .violations
            .iter()
            .any(|v| v.rule == "types/text_over_varchar"));
    }

    #[test]
    fn timestamp_without_tz_flagged() {
        let mut snapshot = empty_snapshot();
        let mut id_col = make_col("id", "bigint");
        id_col.identity = Some("ALWAYS".into());
        snapshot.tables.push(make_table(
            "event",
            vec![
                id_col,
                make_col("created_at", "timestamp without time zone"),
                make_col("updated_at", "timestamp with time zone"),
            ],
            vec![make_pk("pk_event", &["id"])],
            vec![],
        ));

        let report = lint_schema(&snapshot, &LintConfig::default());
        assert!(report
            .violations
            .iter()
            .any(|v| v.rule == "types/timestamptz"));
        assert!(report
            .violations
            .iter()
            .any(|v| v.rule == "timestamps/correct_type"));
    }

    #[test]
    fn serial_flagged() {
        let mut snapshot = empty_snapshot();
        let mut id_col = make_col("id", "integer");
        id_col.default = Some("nextval('item_id_seq'::regclass)".into());
        snapshot.tables.push(make_table(
            "item",
            vec![
                id_col,
                make_col("created_at", "timestamp with time zone"),
                make_col("updated_at", "timestamp with time zone"),
            ],
            vec![make_pk("pk_item", &["id"])],
            vec![],
        ));

        let report = lint_schema(&snapshot, &LintConfig::default());
        assert!(report
            .violations
            .iter()
            .any(|v| v.rule == "types/no_serial"));
    }

    #[test]
    fn fk_without_index_is_error() {
        let mut snapshot = empty_snapshot();
        let mut id_col = make_col("id", "bigint");
        id_col.identity = Some("ALWAYS".into());
        snapshot.tables.push(make_table(
            "order_item",
            vec![
                id_col,
                make_col("order_id", "bigint"),
                make_col("created_at", "timestamp with time zone"),
                make_col("updated_at", "timestamp with time zone"),
            ],
            vec![
                make_pk("pk_order_item", &["id"]),
                make_fk("fk_order_item_order_id", &["order_id"], "public.order"),
            ],
            vec![],
        ));

        let report = lint_schema(&snapshot, &LintConfig::default());
        assert!(report
            .violations
            .iter()
            .any(|v| v.rule == "constraints/fk_has_index"));
    }

    #[test]
    fn fk_with_prefix_index_passes() {
        let mut snapshot = empty_snapshot();
        let mut id_col = make_col("id", "bigint");
        id_col.identity = Some("ALWAYS".into());
        snapshot.tables.push(make_table(
            "order_item",
            vec![
                id_col,
                make_col("order_id", "bigint"),
                make_col("product_id", "bigint"),
                make_col("created_at", "timestamp with time zone"),
                make_col("updated_at", "timestamp with time zone"),
            ],
            vec![
                make_pk("pk_order_item", &["id"]),
                make_fk("fk_order_item_order_id", &["order_id"], "public.order"),
            ],
            vec![make_index(
                "idx_order_item_order_id_product_id",
                &["order_id", "product_id"],
            )],
        ));

        let report = lint_schema(&snapshot, &LintConfig::default());
        assert!(!report
            .violations
            .iter()
            .any(|v| v.rule == "constraints/fk_has_index"));
    }

    #[test]
    fn missing_timestamps_flagged() {
        let mut snapshot = empty_snapshot();
        let mut id_col = make_col("id", "bigint");
        id_col.identity = Some("ALWAYS".into());
        snapshot.tables.push(make_table(
            "item",
            vec![id_col, make_col("name", "text")],
            vec![make_pk("pk_item", &["id"])],
            vec![],
        ));

        let report = lint_schema(&snapshot, &LintConfig::default());
        assert!(report
            .violations
            .iter()
            .any(|v| v.rule == "timestamps/has_created_at"));
        assert!(report
            .violations
            .iter()
            .any(|v| v.rule == "timestamps/has_updated_at"));
    }

    #[test]
    fn disabled_rules_skipped() {
        let mut snapshot = empty_snapshot();
        snapshot.tables.push(make_table(
            "log",
            vec![make_col("message", "text")],
            vec![],
            vec![],
        ));

        let mut config = LintConfig::default();
        config.disabled_rules = vec![
            "pk/exists".into(),
            "timestamps/has_created_at".into(),
            "timestamps/has_updated_at".into(),
        ];

        let report = lint_schema(&snapshot, &config);
        assert!(!report.violations.iter().any(|v| v.rule == "pk/exists"));
        assert!(!report
            .violations
            .iter()
            .any(|v| v.rule == "timestamps/has_created_at"));
    }

    #[test]
    fn plural_table_name_flagged() {
        let mut snapshot = empty_snapshot();
        let mut id_col = make_col("id", "bigint");
        id_col.identity = Some("ALWAYS".into());
        snapshot.tables.push(make_table(
            "users",
            vec![
                id_col,
                make_col("email", "text"),
                make_col("created_at", "timestamp with time zone"),
                make_col("updated_at", "timestamp with time zone"),
            ],
            vec![make_pk("pk_users", &["id"])],
            vec![],
        ));

        let report = lint_schema(&snapshot, &LintConfig::default());
        assert!(
            report
                .violations
                .iter()
                .any(|v| v.rule == "naming/table_style"),
            "plural table name should be flagged: {:?}",
            report.violations
        );
    }

    #[test]
    fn integer_pk_flagged() {
        let mut snapshot = empty_snapshot();
        snapshot.tables.push(make_table(
            "item",
            vec![
                make_col("id", "integer"),
                make_col("created_at", "timestamp with time zone"),
                make_col("updated_at", "timestamp with time zone"),
            ],
            vec![make_pk("pk_item", &["id"])],
            vec![],
        ));

        let report = lint_schema(&snapshot, &LintConfig::default());
        assert!(report
            .violations
            .iter()
            .any(|v| v.rule == "types/bigint_pk_fk"));
        assert!(report
            .violations
            .iter()
            .any(|v| v.rule == "pk/bigint_identity"));
    }

    #[test]
    fn auto_generated_constraint_names_flagged() {
        let mut snapshot = empty_snapshot();
        let mut id_col = make_col("id", "bigint");
        id_col.identity = Some("ALWAYS".into());
        snapshot.tables.push(make_table(
            "item",
            vec![
                id_col,
                make_col("created_at", "timestamp with time zone"),
                make_col("updated_at", "timestamp with time zone"),
            ],
            vec![Constraint {
                name: "item_pkey".into(),
                kind: ConstraintKind::PrimaryKey,
                columns: vec!["id".into()],
                definition: None,
                fk_table: None,
                fk_columns: vec![],
                comment: None,
            }],
            vec![],
        ));

        let report = lint_schema(&snapshot, &LintConfig::default());
        assert!(report
            .violations
            .iter()
            .any(|v| v.rule == "constraints/unnamed"));
    }

    #[test]
    fn custom_regex_table_name() {
        let mut snapshot = empty_snapshot();
        let mut id_col = make_col("id", "bigint");
        id_col.identity = Some("ALWAYS".into());
        snapshot.tables.push(make_table(
            "tbl_user",
            vec![
                id_col,
                make_col("created_at", "timestamp with time zone"),
                make_col("updated_at", "timestamp with time zone"),
            ],
            vec![make_pk("pk_tbl_user", &["id"])],
            vec![],
        ));

        let mut config = LintConfig::default();
        config.table_name_style = "custom_regex".into();
        config.table_name_regex = Some("^tbl_[a-z][a-z0-9_]*$".into());

        let report = lint_schema(&snapshot, &config);
        assert!(
            !report
                .violations
                .iter()
                .any(|v| v.rule == "naming/table_style"),
            "table matching custom regex should pass"
        );
    }

    #[test]
    fn report_summary_counts() {
        let mut snapshot = empty_snapshot();
        snapshot.tables.push(make_table(
            "bad_table",
            vec![make_col("name", "character varying(50)")],
            vec![],
            vec![],
        ));

        let report = lint_schema(&snapshot, &LintConfig::default());
        assert!(report.summary.errors > 0);
        assert!(report.summary.warnings > 0);
        assert_eq!(report.tables_checked, 1);
    }

    #[test]
    fn default_config_matches_hardcoded() {
        let config = LintConfig::default();
        assert_eq!(config.table_name_style, "snake_singular");
        assert_eq!(config.column_name_style, "snake_case");
        assert_eq!(config.pk_type, "bigint_identity");
        assert!(config.require_timestamps);
        assert_eq!(config.timestamp_type, "timestamptz");
        assert!(config.prefer_text_over_varchar);
        assert!(config.disabled_rules.is_empty());
    }
}
