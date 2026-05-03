mod constraints;
mod naming;
mod partitions;
mod pk;
mod timestamps;
mod typecheck;

use std::borrow::Cow;
use std::collections::HashSet;

use crate::schema::{SchemaSnapshot, Table};

use super::types::{LintConfig, LintViolation};

// Walk the partition tree transitively and collect all descendant (schema, name) pairs.
fn collect_partition_children(tables: &[Table]) -> HashSet<(String, String)> {
    let mut children = HashSet::new();

    // seed with direct children
    for table in tables {
        if let Some(ref info) = table.partition_info {
            for child in &info.children {
                children.insert((child.schema.clone(), child.name.clone()));
            }
        }
    }

    // expand transitively: if a collected child itself has partition_info, add its children
    loop {
        let mut new = Vec::new();
        for table in tables {
            if !children.contains(&(table.schema.clone(), table.name.clone())) {
                continue;
            }
            if let Some(ref info) = table.partition_info {
                for child in &info.children {
                    let key = (child.schema.clone(), child.name.clone());
                    if !children.contains(&key) {
                        new.push(key);
                    }
                }
            }
        }
        if new.is_empty() {
            break;
        }
        children.extend(new);
    }

    children
}

fn detect_table_name_style(tables: &[Table]) -> String {
    let mut plural = 0u32;
    let mut singular = 0u32;

    for table in tables {
        if !naming::is_snake_case(&table.name) {
            continue;
        }
        if naming::looks_plural(&table.name) {
            plural += 1;
        } else {
            singular += 1;
        }
    }

    if plural + singular < 5 {
        return "snake_singular".into();
    }

    if plural > singular {
        "snake_plural".into()
    } else {
        "snake_singular".into()
    }
}

pub fn run_all_rules(schema: &SchemaSnapshot, config: &LintConfig) -> Vec<LintViolation> {
    let mut violations = Vec::new();
    let partition_children = collect_partition_children(&schema.tables);

    // resolve "auto" table_name_style
    let resolved_style: Cow<'_, str> = if config.table_name_style == "auto" {
        let detected = detect_table_name_style(&schema.tables);
        tracing::info!(detected = %detected, "auto-detected table name style");
        Cow::Owned(detected)
    } else {
        Cow::Borrowed(&config.table_name_style)
    };
    let effective_config;
    let config = if *resolved_style != config.table_name_style {
        effective_config = LintConfig {
            table_name_style: resolved_style.into_owned(),
            ..config.clone()
        };
        &effective_config
    } else {
        config
    };

    for table in &schema.tables {
        let key = (table.schema.clone(), table.name.clone());
        if partition_children.contains(&key) {
            tracing::debug!(schema = %table.schema, table = %table.name, "skipping partition child");
            continue;
        }

        let qualified = format!("{}.{}", table.schema, table.name);

        if !is_disabled(config, "naming/table_style") {
            naming::check_table_name_style(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "naming/column_style") {
            naming::check_column_name_style(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "naming/fk_pattern") {
            naming::check_fk_naming(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "naming/index_pattern") {
            naming::check_index_naming(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "pk/exists") {
            pk::check_pk_exists(table, &qualified, &mut violations);
        }
        if !is_disabled(config, "pk/bigint_identity") {
            pk::check_pk_type(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "types/text_over_varchar") {
            typecheck::check_text_over_varchar(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "types/timestamptz") {
            typecheck::check_timestamptz(table, &qualified, &mut violations);
        }
        if !is_disabled(config, "types/no_serial") {
            typecheck::check_no_serial(table, &qualified, &mut violations);
        }
        if !is_disabled(config, "types/bigint_pk_fk") {
            typecheck::check_bigint_pk_fk(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "constraints/fk_has_index") {
            constraints::check_fk_has_index(table, &qualified, schema, &mut violations);
        }
        if !is_disabled(config, "constraints/unnamed") {
            constraints::check_unnamed_constraints(table, &qualified, &mut violations);
        }
        if !is_disabled(config, "timestamps/has_created_at") {
            timestamps::check_has_created_at(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "timestamps/has_updated_at") {
            timestamps::check_has_updated_at(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "timestamps/correct_type") {
            timestamps::check_timestamp_type(table, &qualified, config, &mut violations);
        }
        if !is_disabled(config, "partition/too_many_children") {
            partitions::check_partition_too_many_children(table, &qualified, &mut violations);
        }
        if !is_disabled(config, "partition/range_gaps") {
            partitions::check_partition_range_gaps(table, &qualified, &mut violations);
        }
        if !is_disabled(config, "partition/no_default") {
            partitions::check_partition_no_default(table, &qualified, &mut violations);
        }
    }

    // schema-level rules (not per-table)
    if !is_disabled(config, "partition/gucs") {
        partitions::check_partition_gucs(schema, &mut violations);
    }

    suppress_overlapping(&mut violations);
    violations.retain(|v| v.severity >= config.min_severity);
    violations
}

fn suppress_overlapping(violations: &mut Vec<LintViolation>) {
    // (winner, loser) pairs — winner is more specific
    const PAIRS: &[(&str, &str)] = &[
        ("timestamps/correct_type", "types/timestamptz"),
        ("pk/bigint_identity", "types/no_serial"),
        ("pk/bigint_identity", "types/bigint_pk_fk"),
    ];

    for &(winner, loser) in PAIRS {
        let winner_keys: HashSet<(String, Option<String>)> = violations
            .iter()
            .filter(|v| v.rule == winner)
            .map(|v| (v.table.clone(), v.column.clone()))
            .collect();

        if winner_keys.is_empty() {
            continue;
        }

        violations.retain(|v| {
            v.rule != loser || !winner_keys.contains(&(v.table.clone(), v.column.clone()))
        });
    }
}

fn is_disabled(config: &LintConfig, rule: &str) -> bool {
    config.disabled_rules.iter().any(|r| r == rule)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lint::Severity;
    use crate::schema::*;
    use chrono::Utc;

    fn make_col(name: &str, type_name: &str) -> Column {
        Column {
            name: name.into(),
            ordinal: 0,
            type_name: type_name.into(),
            nullable: false,
            default: None,
            identity: None,
            generated: None,
            comment: None,
            statistics_target: None,
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
            backing_index: None,
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
            definition: format!("CREATE INDEX {name} ON ..."),
            is_valid: true,
            backs_constraint: false,
        }
    }

    fn make_table_with(
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
            partition_info: None,
            policies: vec![],
            triggers: vec![],
            reloptions: vec![],
            rls_enabled: false,
        }
    }

    fn schema_with(tables: Vec<Table>) -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "test".into(),
            timestamp: Utc::now(),
            content_hash: "abc".into(),
            source: None,
            tables,
            enums: vec![],
            domains: vec![],
            composites: vec![],
            views: vec![],
            functions: vec![],
            extensions: vec![],
            gucs: vec![],
        }
    }

    fn only_fk_rules() -> LintConfig {
        let mut config = LintConfig::default();
        config.min_severity = Severity::Info;
        // disable everything except fk_has_index to isolate the test
        config.disabled_rules = vec![
            "naming/table_style".into(),
            "naming/column_style".into(),
            "naming/fk_pattern".into(),
            "naming/index_pattern".into(),
            "pk/exists".into(),
            "pk/bigint_identity".into(),
            "types/text_over_varchar".into(),
            "types/timestamptz".into(),
            "types/no_serial".into(),
            "types/bigint_pk_fk".into(),
            "constraints/unnamed".into(),
            "timestamps/has_created_at".into(),
            "timestamps/has_updated_at".into(),
            "timestamps/correct_type".into(),
        ];
        config
    }

    #[test]
    fn composite_fk_with_prefix_index_passes() {
        // FK (order_id, product_id) covered by index (order_id, product_id, status)
        let schema = schema_with(vec![make_table_with(
            "line_item",
            vec![
                make_col("order_id", "bigint"),
                make_col("product_id", "bigint"),
                make_col("status", "text"),
            ],
            vec![make_fk(
                "fk_line_item_order_product",
                &["order_id", "product_id"],
                "public.order",
            )],
            vec![make_index(
                "idx_line_item_composite",
                &["order_id", "product_id", "status"],
            )],
        )]);
        let violations = run_all_rules(&schema, &only_fk_rules());
        assert!(
            !violations
                .iter()
                .any(|v| v.rule == "constraints/fk_has_index"),
            "3-col index covering 2-col FK as prefix should pass"
        );
    }

    #[test]
    fn composite_fk_wrong_column_order_fails() {
        // FK (order_id, product_id) but index is (product_id, order_id) — wrong prefix order
        let schema = schema_with(vec![make_table_with(
            "line_item",
            vec![
                make_col("order_id", "bigint"),
                make_col("product_id", "bigint"),
            ],
            vec![make_fk(
                "fk_line_item_order_product",
                &["order_id", "product_id"],
                "public.order",
            )],
            vec![make_index(
                "idx_line_item_wrong_order",
                &["product_id", "order_id"],
            )],
        )]);
        let violations = run_all_rules(&schema, &only_fk_rules());
        assert!(
            violations
                .iter()
                .any(|v| v.rule == "constraints/fk_has_index"),
            "index with swapped column order should NOT satisfy the FK"
        );
    }

    #[test]
    fn composite_fk_partial_index_coverage_fails() {
        // FK (order_id, product_id) but index only on (order_id) — not enough columns
        let schema = schema_with(vec![make_table_with(
            "line_item",
            vec![
                make_col("order_id", "bigint"),
                make_col("product_id", "bigint"),
            ],
            vec![make_fk(
                "fk_line_item_order_product",
                &["order_id", "product_id"],
                "public.order",
            )],
            vec![make_index("idx_line_item_order_id", &["order_id"])],
        )]);
        let violations = run_all_rules(&schema, &only_fk_rules());
        assert!(
            violations
                .iter()
                .any(|v| v.rule == "constraints/fk_has_index"),
            "single-col index should NOT satisfy 2-col FK"
        );
    }

    #[test]
    fn composite_fk_exact_match_passes() {
        // FK (order_id, product_id) with index (order_id, product_id) — exact match
        let schema = schema_with(vec![make_table_with(
            "line_item",
            vec![
                make_col("order_id", "bigint"),
                make_col("product_id", "bigint"),
            ],
            vec![make_fk(
                "fk_line_item_order_product",
                &["order_id", "product_id"],
                "public.order",
            )],
            vec![make_index(
                "idx_line_item_order_product",
                &["order_id", "product_id"],
            )],
        )]);
        let violations = run_all_rules(&schema, &only_fk_rules());
        assert!(
            !violations
                .iter()
                .any(|v| v.rule == "constraints/fk_has_index"),
            "exact match index should satisfy the FK"
        );
    }

    // --- partition dedup helpers ---

    fn make_partition_child(name: &str) -> PartitionChild {
        PartitionChild {
            schema: "public".into(),
            name: name.into(),
            bound: "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')".into(),
        }
    }

    fn make_partitioned_table(name: &str, children: Vec<PartitionChild>) -> Table {
        Table {
            oid: 0,
            schema: "public".into(),
            name: name.into(),
            columns: vec![make_col("id", "integer")],
            constraints: vec![],
            indexes: vec![],
            comment: None,
            partition_info: Some(PartitionInfo {
                strategy: PartitionStrategy::Range,
                key: "created_at".into(),
                children,
            }),
            policies: vec![],
            triggers: vec![],
            reloptions: vec![],
            rls_enabled: false,
        }
    }

    /// Config that only enables the given rules
    fn config_with_only(rules: &[&str]) -> LintConfig {
        let all_rules = [
            "naming/table_style",
            "naming/column_style",
            "naming/fk_pattern",
            "naming/index_pattern",
            "pk/exists",
            "pk/bigint_identity",
            "types/text_over_varchar",
            "types/timestamptz",
            "types/no_serial",
            "types/bigint_pk_fk",
            "constraints/fk_has_index",
            "constraints/unnamed",
            "timestamps/has_created_at",
            "timestamps/has_updated_at",
            "timestamps/correct_type",
            "partition/too_many_children",
            "partition/range_gaps",
            "partition/no_default",
            "partition/gucs",
        ];
        let mut config = LintConfig::default();
        config.min_severity = Severity::Info;
        config.disabled_rules = all_rules
            .iter()
            .filter(|r| !rules.contains(r))
            .map(|r| r.to_string())
            .collect();
        config
    }

    fn make_pk(name: &str, columns: &[&str]) -> Constraint {
        Constraint {
            name: name.into(),
            kind: ConstraintKind::PrimaryKey,
            columns: columns.iter().map(|s| s.to_string()).collect(),
            definition: None,
            fk_table: None,
            fk_columns: vec![],
            backing_index: None,
            comment: None,
        }
    }

    fn make_col_with_default(name: &str, type_name: &str, default: &str) -> Column {
        Column {
            name: name.into(),
            ordinal: 0,
            type_name: type_name.into(),
            nullable: false,
            default: Some(default.into()),
            identity: None,
            generated: None,
            comment: None,
            statistics_target: None,
        }
    }

    // --- Change 1: partition dedup tests ---

    #[test]
    fn partition_parent_with_three_children_only_parent_violations() {
        let parent = make_partitioned_table(
            "event",
            vec![
                make_partition_child("event_2024_01"),
                make_partition_child("event_2024_02"),
                make_partition_child("event_2024_03"),
            ],
        );
        let child1 = make_table_with(
            "event_2024_01",
            vec![make_col("id", "integer")],
            vec![],
            vec![],
        );
        let child2 = make_table_with(
            "event_2024_02",
            vec![make_col("id", "integer")],
            vec![],
            vec![],
        );
        let child3 = make_table_with(
            "event_2024_03",
            vec![make_col("id", "integer")],
            vec![],
            vec![],
        );

        let schema = schema_with(vec![parent, child1, child2, child3]);
        let config = config_with_only(&["pk/exists"]);
        let violations = run_all_rules(&schema, &config);

        // only the parent should fire pk/exists
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].table, "public.event");
    }

    #[test]
    fn nested_partitions_grandchild_also_skipped() {
        let parent = make_partitioned_table("event", vec![make_partition_child("event_2024_01")]);
        let mid = Table {
            oid: 0,
            schema: "public".into(),
            name: "event_2024_01".into(),
            columns: vec![make_col("id", "integer")],
            constraints: vec![],
            indexes: vec![],
            comment: None,
            partition_info: Some(PartitionInfo {
                strategy: PartitionStrategy::Hash,
                key: "id".into(),
                children: vec![make_partition_child("event_2024_01_h0")],
            }),
            policies: vec![],
            triggers: vec![],
            reloptions: vec![],
            rls_enabled: false,
        };
        let grandchild = make_table_with(
            "event_2024_01_h0",
            vec![make_col("id", "integer")],
            vec![],
            vec![],
        );

        let schema = schema_with(vec![parent, mid, grandchild]);
        let config = config_with_only(&["pk/exists"]);
        let violations = run_all_rules(&schema, &config);

        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].table, "public.event");
    }

    // --- Change 2: overlapping rule suppression tests ---

    #[test]
    fn timestamp_correct_type_suppresses_timestamptz() {
        // created_at with wrong type should fire timestamps/correct_type but NOT types/timestamptz
        let table = make_table_with(
            "user",
            vec![make_col("created_at", "timestamp without time zone")],
            vec![],
            vec![],
        );
        let schema = schema_with(vec![table]);
        let config = config_with_only(&["timestamps/correct_type", "types/timestamptz"]);
        let violations = run_all_rules(&schema, &config);

        let rules: Vec<&str> = violations.iter().map(|v| v.rule.as_str()).collect();
        assert!(
            rules.contains(&"timestamps/correct_type"),
            "winner rule should fire"
        );
        assert!(
            !rules.contains(&"types/timestamptz"),
            "loser rule should be suppressed"
        );
    }

    #[test]
    fn serial_pk_suppresses_no_serial() {
        // integer PK with serial default should fire pk/bigint_identity but NOT types/no_serial
        let table = make_table_with(
            "user",
            vec![make_col_with_default(
                "id",
                "integer",
                "nextval('user_id_seq')",
            )],
            vec![make_pk("user_pkey", &["id"])],
            vec![],
        );
        let schema = schema_with(vec![table]);
        let config = config_with_only(&["pk/bigint_identity", "types/no_serial"]);
        let violations = run_all_rules(&schema, &config);

        let rules: Vec<&str> = violations.iter().map(|v| v.rule.as_str()).collect();
        assert!(
            rules.contains(&"pk/bigint_identity"),
            "winner rule should fire"
        );
        assert!(
            !rules.contains(&"types/no_serial"),
            "loser rule should be suppressed"
        );
    }

    #[test]
    fn loser_fires_when_winner_disabled() {
        // if timestamps/correct_type is disabled, types/timestamptz should still fire
        let table = make_table_with(
            "user",
            vec![make_col("created_at", "timestamp without time zone")],
            vec![],
            vec![],
        );
        let schema = schema_with(vec![table]);
        let config = config_with_only(&["types/timestamptz"]);
        let violations = run_all_rules(&schema, &config);

        assert!(
            violations.iter().any(|v| v.rule == "types/timestamptz"),
            "loser should fire when winner is disabled"
        );
    }

    // --- Change 3: auto-detect table name style tests ---

    #[test]
    fn auto_detect_picks_snake_plural_when_majority_plural() {
        // 4 plural + 1 singular = majority plural (>= 5 total)
        let tables: Vec<Table> = ["users", "orders", "products", "invoices", "config"]
            .iter()
            .map(|n| make_table_with(n, vec![make_col("id", "bigint")], vec![], vec![]))
            .collect();

        let result = detect_table_name_style(&tables);
        assert_eq!(result, "snake_plural");
    }

    #[test]
    fn auto_detect_fallback_when_fewer_than_5_tables() {
        let tables: Vec<Table> = ["user", "orders", "config"]
            .iter()
            .map(|n| make_table_with(n, vec![make_col("id", "bigint")], vec![], vec![]))
            .collect();

        let result = detect_table_name_style(&tables);
        assert_eq!(result, "snake_singular");
    }

    #[test]
    fn auto_detect_resolves_in_run_all_rules() {
        // 3 plural + 2 singular tables, auto should resolve to snake_plural
        // the singular tables should get naming violations
        let tables: Vec<Table> = ["users", "orders", "products", "config", "setting"]
            .iter()
            .map(|n| make_table_with(n, vec![make_col("id", "bigint")], vec![], vec![]))
            .collect();
        let schema = schema_with(tables);

        let mut config = config_with_only(&["naming/table_style"]);
        config.table_name_style = "auto".into();
        let violations = run_all_rules(&schema, &config);

        // snake_plural doesn't check for plural (just snake_case), so no violations expected
        assert!(
            violations.is_empty(),
            "auto-resolved to snake_plural should accept all snake_case names, got: {:?}",
            violations.iter().map(|v| &v.table).collect::<Vec<_>>()
        );
    }

    // --- partition lint rules ---

    #[test]
    fn partition_too_many_children_warns() {
        let children: Vec<PartitionChild> = (0..600)
            .map(|i| PartitionChild {
                schema: "public".into(),
                name: format!("orders_{i}"),
                bound: format!("FOR VALUES FROM ('{i}') TO ('{}')", i + 1),
            })
            .collect();

        let table = make_partitioned_table("orders", children);
        let schema = schema_with(vec![table]);
        let config = config_with_only(&["partition/too_many_children"]);
        let violations = run_all_rules(&schema, &config);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].message.contains("600 partitions"));
    }

    #[test]
    fn partition_too_many_children_no_warn_under_threshold() {
        let children: Vec<PartitionChild> = (0..10)
            .map(|i| PartitionChild {
                schema: "public".into(),
                name: format!("orders_{i}"),
                bound: format!("FOR VALUES FROM ('{i}') TO ('{}')", i + 1),
            })
            .collect();

        let table = make_partitioned_table("orders", children);
        let schema = schema_with(vec![table]);
        let config = config_with_only(&["partition/too_many_children"]);
        let violations = run_all_rules(&schema, &config);
        assert!(violations.is_empty());
    }

    #[test]
    fn partition_range_gaps_detected() {
        let table = Table {
            oid: 0,
            schema: "public".into(),
            name: "events".into(),
            columns: vec![make_col("id", "integer")],
            constraints: vec![],
            indexes: vec![],
            comment: None,
            partition_info: Some(PartitionInfo {
                strategy: PartitionStrategy::Range,
                key: "created_at".into(),
                children: vec![
                    PartitionChild {
                        schema: "public".into(),
                        name: "events_q1".into(),
                        bound: "FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')".into(),
                    },
                    // gap: 2024-04-01 to 2024-07-01 missing
                    PartitionChild {
                        schema: "public".into(),
                        name: "events_q3".into(),
                        bound: "FOR VALUES FROM ('2024-07-01') TO ('2024-10-01')".into(),
                    },
                ],
            }),
            policies: vec![],
            triggers: vec![],
            reloptions: vec![],
            rls_enabled: false,
        };
        let schema = schema_with(vec![table]);
        let config = config_with_only(&["partition/range_gaps"]);
        let violations = run_all_rules(&schema, &config);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].message.contains("gap"));
    }

    #[test]
    fn partition_no_default_warns() {
        let table = make_partitioned_table(
            "orders",
            vec![PartitionChild {
                schema: "public".into(),
                name: "orders_q1".into(),
                bound: "FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')".into(),
            }],
        );
        let schema = schema_with(vec![table]);
        let config = config_with_only(&["partition/no_default"]);
        let violations = run_all_rules(&schema, &config);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].message.contains("no DEFAULT"));
    }

    #[test]
    fn partition_no_default_skips_when_default_exists() {
        let table = make_partitioned_table(
            "orders",
            vec![
                PartitionChild {
                    schema: "public".into(),
                    name: "orders_q1".into(),
                    bound: "FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')".into(),
                },
                PartitionChild {
                    schema: "public".into(),
                    name: "orders_default".into(),
                    bound: "DEFAULT".into(),
                },
            ],
        );
        let schema = schema_with(vec![table]);
        let config = config_with_only(&["partition/no_default"]);
        let violations = run_all_rules(&schema, &config);
        assert!(violations.is_empty());
    }

    #[test]
    fn partition_gucs_warns_when_pruning_off() {
        let table = make_partitioned_table("orders", vec![make_partition_child("orders_q1")]);
        let mut schema = schema_with(vec![table]);
        schema.gucs.push(GucSetting {
            name: "enable_partition_pruning".into(),
            setting: "off".into(),
            unit: None,
        });
        let config = config_with_only(&["partition/gucs"]);
        let violations = run_all_rules(&schema, &config);
        assert!(
            violations
                .iter()
                .any(|v| v.message.contains("enable_partition_pruning"))
        );
    }
}
