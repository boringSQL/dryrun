use std::collections::BTreeMap;

use chrono::Utc;

use super::*;
use crate::schema::*;
use crate::schema::{
    ActivityStatsSnapshot, AnnotatedSnapshot, IndexActivityEntry, NodeIdentity,
    PlannerStatsSnapshot, TableActivity, TableActivityEntry, TableSizing, TableSizingEntry,
};

fn empty_schema() -> SchemaSnapshot {
    SchemaSnapshot {
        pg_version: "PostgreSQL 17.0".into(),
        database: "test".into(),
        timestamp: Utc::now(),
        content_hash: "test".into(),
        source: None,
        tables: vec![Table {
            oid: 1,
            schema: "public".into(),
            name: "orders".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    ordinal: 1,
                    type_name: "bigint".into(),
                    nullable: false,
                    default: None,
                    identity: None,
                    generated: None,
                    comment: None,
                    statistics_target: None,
                },
                Column {
                    name: "customer_id".into(),
                    ordinal: 2,
                    type_name: "bigint".into(),
                    nullable: false,
                    default: None,
                    identity: None,
                    generated: None,
                    comment: None,
                    statistics_target: None,
                },
                Column {
                    name: "data".into(),
                    ordinal: 3,
                    type_name: "jsonb".into(),
                    nullable: true,
                    default: None,
                    identity: None,
                    generated: None,
                    comment: None,
                    statistics_target: None,
                },
            ],
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

fn make_seq_scan(table: &str, rows: f64, filter: Option<&str>) -> PlanNode {
    PlanNode {
        node_type: "Seq Scan".into(),
        relation_name: Some(table.into()),
        schema: Some("public".into()),
        alias: None,
        startup_cost: 0.0,
        total_cost: rows * 0.01,
        plan_rows: rows,
        plan_width: 64,
        actual_rows: None,
        actual_loops: None,
        actual_startup_time: None,
        actual_total_time: None,
        shared_hit_blocks: None,
        shared_read_blocks: None,
        index_name: None,
        index_cond: None,
        filter: filter.map(String::from),
        rows_removed_by_filter: None,
        sort_key: None,
        sort_method: None,
        hash_cond: None,
        join_type: None,
        subplans_removed: None,
        cte_name: None,
        parent_relationship: None,
        children: vec![],
    }
}

// Wrap a bare schema in an empty annotated bundle — no planner, no
// activity. Mirrors what the MCP server hands tool bodies before
// any `dryrun snapshot take` has run.
fn ddl_only(schema: SchemaSnapshot) -> AnnotatedSnapshot {
    AnnotatedSnapshot {
        schema,
        planner: None,
        activity_by_node: BTreeMap::new(),
    }
}

#[test]
fn advise_seq_scan_suggests_btree() {
    let snap = ddl_only(empty_schema());
    let plan = make_seq_scan("orders", 100_000.0, Some("(customer_id = 42)"));
    let advice = advise(&plan, &snap.view(), None);
    assert!(!advice.is_empty());
    assert!(advice[0].ddl.as_ref().unwrap().contains("btree"));
    assert!(advice[0].ddl.as_ref().unwrap().contains("customer_id"));
    assert!(advice[0].ddl.as_ref().unwrap().contains("CONCURRENTLY"));
}

#[test]
fn advise_seq_scan_jsonb_suggests_gin() {
    let snap = ddl_only(empty_schema());
    let plan = make_seq_scan("orders", 100_000.0, Some("(data @> '{}'::jsonb)"));
    let advice = advise(&plan, &snap.view(), None);
    assert!(!advice.is_empty());
    assert!(advice[0].ddl.as_ref().unwrap().contains("gin"));
}

#[test]
fn advise_small_table_no_advice() {
    let snap = ddl_only(empty_schema());
    let plan = make_seq_scan("orders", 50.0, Some("(id = 1)"));
    let advice = advise(&plan, &snap.view(), None);
    assert!(advice.is_empty());
}

#[test]
fn advise_includes_version_note() {
    let snap = ddl_only(empty_schema());
    let plan = make_seq_scan("orders", 100_000.0, Some("(customer_id = 42)"));
    let pg14 = PgVersion {
        major: 14,
        minor: 0,
        patch: 0,
    };
    let advice = advise(&plan, &snap.view(), Some(&pg14));
    assert!(!advice.is_empty());
    assert!(advice[0].version_note.is_some());
}

// Helper: build an ActivityStatsSnapshot for one node with a single
// table activity row carrying the supplied seq_scan counter.
fn activity_for(label: &str, seq_scan: i64) -> ActivityStatsSnapshot {
    ActivityStatsSnapshot {
        pg_version: "PostgreSQL 17.0".into(),
        database: "test".into(),
        timestamp: Utc::now(),
        content_hash: format!("h-{label}"),
        schema_ref_hash: "sh".into(),
        node: NodeIdentity {
            label: label.into(),
            host: label.into(),
            is_standby: label != "master",
            replication_lag_bytes: None,
            stats_reset: None,
        },
        tables: vec![TableActivityEntry {
            table: QualifiedName::new("public", "orders"),
            activity: TableActivity {
                seq_scan,
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
        indexes: Vec::<IndexActivityEntry>::new(),
    }
}

#[test]
fn advise_seq_scan_includes_node_context() {
    // Two-node cluster — primary handles indexed traffic, replica
    // is doing the seq scans. The recommendation should call that
    // out with the per-node breakdown.
    let mut activity_by_node = BTreeMap::new();
    activity_by_node.insert("master".into(), activity_for("master", 100));
    activity_by_node.insert("replica-1".into(), activity_for("replica-1", 42000));
    let snap = AnnotatedSnapshot {
        schema: empty_schema(),
        planner: Some(PlannerStatsSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "test".into(),
            timestamp: Utc::now(),
            content_hash: "ph".into(),
            schema_ref_hash: "sh".into(),
            tables: vec![TableSizingEntry {
                table: QualifiedName::new("public", "orders"),
                sizing: TableSizing {
                    reltuples: 100_000.0,
                    relpages: 1250,
                    table_size: 10_000_000,
                    total_size: None,
                    index_size: None,
                },
            }],
            columns: vec![],
            indexes: vec![],
        }),
        activity_by_node,
    };
    let plan = make_seq_scan("orders", 100_000.0, Some("(customer_id = 42)"));
    let advice = advise(&plan, &snap.view(), None);
    assert!(!advice.is_empty());
    assert!(advice[0].recommendation.contains("across 2 nodes"));
    assert!(advice[0].recommendation.contains("master: 100"));
    assert!(advice[0].recommendation.contains("replica-1: 42000"));
}

#[test]
fn extract_column_simple() {
    assert_eq!(
        extract_column_from_filter("(customer_id = 42)"),
        Some("customer_id".into())
    );
    assert_eq!(
        extract_column_from_filter("(status IS NOT NULL)"),
        Some("status".into())
    );
    assert_eq!(
        extract_column_from_filter("(t.name = 'foo')"),
        Some("name".into())
    );
}
