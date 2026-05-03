use super::*;

#[test]
fn deserialize_analyze_plan_params() {
    let json = serde_json::json!({
        "sql": "SELECT * FROM orders WHERE customer_id = 42",
        "plan_json": [{"Plan": {
            "Node Type": "Seq Scan",
            "Relation Name": "orders",
            "Schema": "public",
            "Startup Cost": 0.0,
            "Total Cost": 450.0,
            "Plan Rows": 10000,
            "Plan Width": 48
        }}]
    });
    let params: AnalyzePlanParams = serde_json::from_value(json).unwrap();
    assert_eq!(params.sql, "SELECT * FROM orders WHERE customer_id = 42");
    assert!(params.plan_json.is_array());
    // default value
    assert_eq!(params.include_index_suggestions, Some(true));
}

#[test]
fn deserialize_analyze_plan_params_with_explicit_false() {
    let json = serde_json::json!({
        "sql": "SELECT 1",
        "plan_json": {"Plan": {"Node Type": "Result", "Startup Cost": 0.0, "Total Cost": 0.01, "Plan Rows": 1, "Plan Width": 4}},
        "include_index_suggestions": false
    });
    let params: AnalyzePlanParams = serde_json::from_value(json).unwrap();
    assert_eq!(params.include_index_suggestions, Some(false));
    assert!(params.plan_json.is_object());
}

#[test]
fn plan_json_extraction_wrapped_array() {
    let plan_json = serde_json::json!([{
        "Plan": {
            "Node Type": "Seq Scan",
            "Relation Name": "users",
            "Schema": "public",
            "Startup Cost": 0.0,
            "Total Cost": 35.5,
            "Plan Rows": 2550,
            "Plan Width": 64
        }
    }]);
    let plan_value = plan_json
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|obj| obj.get("Plan"))
        .unwrap();
    let plan = dry_run_core::query::parse_plan_json(plan_value).unwrap();
    assert_eq!(plan.node_type, "Seq Scan");
    assert_eq!(plan.relation_name.as_deref(), Some("users"));
}

#[test]
fn plan_json_extraction_bare_object() {
    let plan_json = serde_json::json!({
        "Plan": {
            "Node Type": "Index Scan",
            "Relation Name": "orders",
            "Schema": "public",
            "Index Name": "orders_pkey",
            "Startup Cost": 0.0,
            "Total Cost": 8.27,
            "Plan Rows": 1,
            "Plan Width": 64
        }
    });
    let plan_value = plan_json.get("Plan").unwrap();
    let plan = dry_run_core::query::parse_plan_json(plan_value).unwrap();
    assert_eq!(plan.node_type, "Index Scan");
}

#[test]
fn plan_json_missing_plan_key_array() {
    let plan_json = serde_json::json!([{"Something": "else"}]);
    let result = plan_json
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|obj| obj.get("Plan"));
    assert!(result.is_none());
}

#[test]
fn plan_json_missing_plan_key_object() {
    let plan_json = serde_json::json!({"NotPlan": {}});
    assert!(plan_json.get("Plan").is_none());
}

#[tokio::test]
async fn list_tables_includes_pg_version() {
    let snapshot = test_snapshot();
    let server = DryRunServer::from_annotated_with_db(
        crate::mcp::wrap_schema_only(snapshot),
        None,
        LintConfig::default(),
        None,
        "test",
        vec![],
    );
    let result = server
        .list_tables(Parameters(ListTablesParams {
            schema: None,
            sort: None,
            limit: None,
            offset: None,
        }))
        .await
        .unwrap();
    let text = result.content.first().unwrap();
    let text_str = format!("{text:?}");
    assert!(
        text_str.contains("PostgreSQL 18.3.0"),
        "list_tables output should contain PG version"
    );
}

#[tokio::test]
async fn describe_table_includes_pg_version() {
    let snapshot = test_snapshot();
    let server = DryRunServer::from_annotated_with_db(
        crate::mcp::wrap_schema_only(snapshot),
        None,
        LintConfig::default(),
        None,
        "test",
        vec![],
    );
    let result = server
        .describe_table(Parameters(DescribeTableParams {
            table: "orders".into(),
            schema: None,
            detail: None,
        }))
        .await
        .unwrap();
    let text = result.content.first().unwrap();
    let text_str = format!("{text:?}");
    assert!(
        text_str.contains("pg_version"),
        "describe_table output should contain pg_version field"
    );
}

fn test_snapshot() -> dry_run_core::SchemaSnapshot {
    use dry_run_core::schema::*;
    SchemaSnapshot {
        pg_version: "PostgreSQL 18.3.0 on x86_64-pc-linux-gnu".into(),
        database: "testdb".into(),
        timestamp: chrono::Utc::now(),
        content_hash: "abc123".into(),
        source: None,
        tables: vec![Table {
            oid: 1,
            schema: "public".into(),
            name: "orders".into(),
            columns: vec![Column {
                name: "id".into(),
                ordinal: 1,
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
        enums: vec![],
        domains: vec![],
        composites: vec![],
        views: vec![],
        functions: vec![],
        extensions: vec![],
        gucs: vec![],
    }
}

#[test]
fn analyze_plan_with_analyze_buffers_data() {
    // realistic EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) output
    let plan_json = serde_json::json!([{
        "Plan": {
            "Node Type": "Seq Scan",
            "Relation Name": "orders",
            "Schema": "public",
            "Startup Cost": 0.0,
            "Total Cost": 15234.5,
            "Plan Rows": 500000,
            "Plan Width": 120,
            "Actual Rows": 487320,
            "Actual Loops": 1,
            "Actual Startup Time": 0.02,
            "Actual Total Time": 320.5,
            "Shared Hit Blocks": 8000,
            "Shared Read Blocks": 2000,
            "Filter": "(customer_id = 42)",
            "Rows Removed by Filter": 487278
        },
        "Planning Time": 0.1,
        "Execution Time": 320.6
    }]);
    let plan_value = plan_json
        .as_array()
        .unwrap()
        .first()
        .unwrap()
        .get("Plan")
        .unwrap();
    let plan = dry_run_core::query::parse_plan_json(plan_value).unwrap();
    assert_eq!(plan.total_cost, 15234.5);
    assert_eq!(plan.actual_rows, Some(487320.0));
    assert_eq!(plan.shared_hit_blocks, Some(8000));
    assert_eq!(plan.rows_removed_by_filter, Some(487278.0));
}

#[tokio::test]
async fn persist_refresh_writes_activity_for_primary() {
    use dry_run_core::history::{DatabaseId, ProjectId};
    use dry_run_core::schema::{
        ActivityStatsSnapshot, IndexActivity, IndexActivityEntry, NodeIdentity, QualifiedName,
        TableActivity, TableActivityEntry,
    };

    let dir = tempfile::TempDir::new().unwrap();
    let store = HistoryStore::open(&dir.path().join("history.db")).unwrap();
    let key = SnapshotKey {
        project_id: ProjectId("test".into()),
        database_id: DatabaseId("test-db".into()),
    };

    let schema = test_snapshot();
    let schema_hash = schema.content_hash.clone();

    let activity = ActivityStatsSnapshot {
        pg_version: schema.pg_version.clone(),
        database: schema.database.clone(),
        timestamp: chrono::Utc::now(),
        content_hash: "act-h1".into(),
        schema_ref_hash: schema_hash.clone(),
        node: NodeIdentity {
            label: "primary".into(),
            host: "localhost".into(),
            is_standby: false,
            replication_lag_bytes: None,
            stats_reset: None,
        },
        tables: vec![TableActivityEntry {
            table: QualifiedName::new("public", "orders"),
            activity: TableActivity {
                seq_scan: 1,
                idx_scan: 0,
                n_live_tup: 0,
                n_dead_tup: 0,
                last_vacuum: None,
                last_autovacuum: None,
                last_analyze: None,
                last_autoanalyze: None,
                vacuum_count: 0,
                autovacuum_count: 0,
                analyze_count: 0,
                autoanalyze_count: 0,
            },
        }],
        indexes: vec![IndexActivityEntry {
            index: QualifiedName::new("public", "orders_pkey"),
            activity: IndexActivity {
                idx_scan: 0,
                idx_tup_read: 0,
                idx_tup_fetch: 0,
            },
        }],
    };

    let mut activity_by_node = std::collections::BTreeMap::new();
    activity_by_node.insert("primary".to_string(), activity);

    super::persist_refresh(&store, &key, &schema, None, &activity_by_node).await;

    let bundle = store
        .get_annotated(&key, SnapshotRef::Latest)
        .await
        .unwrap();
    assert_eq!(bundle.schema.content_hash, schema_hash);
    assert!(
        bundle.activity_by_node.contains_key("primary"),
        "persist_refresh should have written activity_stats for 'primary'"
    );
}

fn make_activity_row(
    schema_ref: &str,
    label: &str,
    hash: &str,
) -> dry_run_core::ActivityStatsSnapshot {
    use dry_run_core::schema::{
        ActivityStatsSnapshot, IndexActivity, IndexActivityEntry, NodeIdentity, QualifiedName,
        TableActivity, TableActivityEntry,
    };
    ActivityStatsSnapshot {
        pg_version: "PostgreSQL 18.3.0".into(),
        database: "testdb".into(),
        timestamp: chrono::Utc::now(),
        content_hash: hash.into(),
        schema_ref_hash: schema_ref.into(),
        node: NodeIdentity {
            label: label.into(),
            host: format!("host-{label}"),
            is_standby: label != "primary",
            replication_lag_bytes: None,
            stats_reset: None,
        },
        tables: vec![TableActivityEntry {
            table: QualifiedName::new("public", "orders"),
            activity: TableActivity {
                seq_scan: 1,
                idx_scan: 0,
                n_live_tup: 0,
                n_dead_tup: 0,
                last_vacuum: None,
                last_autovacuum: None,
                last_analyze: None,
                last_autoanalyze: None,
                vacuum_count: 0,
                autovacuum_count: 0,
                analyze_count: 0,
                autoanalyze_count: 0,
            },
        }],
        indexes: vec![IndexActivityEntry {
            index: QualifiedName::new("public", "orders_pkey"),
            activity: IndexActivity {
                idx_scan: 0,
                idx_tup_read: 0,
                idx_tup_fetch: 0,
            },
        }],
    }
}

#[test]
fn build_inline_inserts_primary_when_present() {
    let bundle = super::build_inline(
        test_snapshot(),
        None,
        Some(make_activity_row("abc123", "primary", "act-1")),
    );
    assert_eq!(bundle.activity_by_node.len(), 1);
    assert!(bundle.activity_by_node.contains_key("primary"));
}

#[test]
fn build_inline_yields_empty_map_without_activity() {
    let bundle = super::build_inline(test_snapshot(), None, None);
    assert!(bundle.activity_by_node.is_empty());
    assert!(bundle.planner.is_none());
}

// Regression for 2f85792: refresh must not drop replica activity rows
// already in history.db. Before the fix, the cache was rebuilt with
// primary-only. This exercises the cache-rebuild logic directly via
// `rebuild_after_refresh`, no live DB needed.
#[tokio::test]
async fn rebuild_after_refresh_preserves_replica_activity() {
    let dir = tempfile::TempDir::new().unwrap();
    let store = HistoryStore::open(&dir.path().join("history.db")).unwrap();
    let key = SnapshotKey {
        project_id: dry_run_core::history::ProjectId("test".into()),
        database_id: dry_run_core::history::DatabaseId("test-db".into()),
    };

    let schema = test_snapshot();
    let schema_hash = schema.content_hash.clone();

    SnapshotStore::put(&store, &key, &schema)
        .await
        .expect("seed schema");
    let replica = make_activity_row(&schema_hash, "replica1", "replica-h1");
    store
        .put_activity_stats(&key, &replica)
        .await
        .expect("seed replica activity");

    let live_primary = make_activity_row(&schema_hash, "primary", "primary-h1");
    let bundle =
        super::rebuild_after_refresh(schema, None, Some(live_primary), Some((&store, &key))).await;

    assert!(
        bundle.activity_by_node.contains_key("primary"),
        "freshly-introspected primary activity must end up in the cache"
    );
    assert!(
        bundle.activity_by_node.contains_key("replica1"),
        "pre-seeded replica1 activity must survive rebuild \
         (regression: rebuild used to drop everything except primary)"
    );
}

#[tokio::test]
async fn rebuild_after_refresh_without_history_uses_inline_only() {
    let bundle = super::rebuild_after_refresh(
        test_snapshot(),
        None,
        Some(make_activity_row("abc123", "primary", "primary-h1")),
        None,
    )
    .await;
    assert_eq!(bundle.activity_by_node.len(), 1);
    assert!(bundle.activity_by_node.contains_key("primary"));
}
