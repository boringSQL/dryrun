use std::collections::BTreeMap;

use super::*;
use crate::schema::*;

fn ddl_table(name: &str) -> Table {
    Table {
        oid: 0,
        schema: "public".into(),
        name: name.into(),
        columns: vec![],
        constraints: vec![],
        indexes: vec![],
        comment: None,
        partition_info: None,
        policies: vec![],
        triggers: vec![],
        reloptions: vec![],
        rls_enabled: false,
    }
}

fn make_snap(tables: Vec<Table>) -> SchemaSnapshot {
    SchemaSnapshot {
        pg_version: "16.0".into(),
        database: "test".into(),
        timestamp: chrono::Utc::now(),
        content_hash: String::new(),
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

fn annotated(
    tables: Vec<Table>,
    sizing: Vec<(&str, f64, i64)>,
    dead_by_table: Vec<(&str, i64)>,
) -> AnnotatedSnapshot {
    let schema = make_snap(tables);
    let planner = PlannerStatsSnapshot {
        pg_version: "16.0".into(),
        database: "test".into(),
        timestamp: chrono::Utc::now(),
        content_hash: "ph".into(),
        schema_ref_hash: "sh".into(),
        tables: sizing
            .into_iter()
            .map(|(name, reltuples, table_size)| TableSizingEntry {
                table: QualifiedName::new("public", name),
                sizing: TableSizing {
                    reltuples,
                    relpages: 1000,
                    table_size,
                    total_size: None,
                    index_size: None,
                },
            })
            .collect(),
        columns: vec![],
        indexes: vec![],
    };
    let activity = ActivityStatsSnapshot {
        pg_version: "16.0".into(),
        database: "test".into(),
        timestamp: chrono::Utc::now(),
        content_hash: "ah".into(),
        schema_ref_hash: "sh".into(),
        node: NodeIdentity {
            label: "primary".into(),
            host: "p".into(),
            is_standby: false,
            replication_lag_bytes: None,
            stats_reset: None,
        },
        tables: dead_by_table
            .into_iter()
            .map(|(name, dead)| TableActivityEntry {
                table: QualifiedName::new("public", name),
                activity: TableActivity {
                    seq_scan: 0,
                    idx_scan: 0,
                    n_live_tup: 0,
                    n_dead_tup: dead,
                    last_vacuum: None,
                    last_autovacuum: None,
                    last_analyze: None,
                    last_autoanalyze: None,
                    vacuum_count: 0,
                    autovacuum_count: 0,
                    analyze_count: 0,
                    autoanalyze_count: 0,
                },
            })
            .collect(),
        indexes: Vec::<IndexActivityEntry>::new(),
    };
    let mut activity_by_node = BTreeMap::new();
    activity_by_node.insert("primary".into(), activity);
    AnnotatedSnapshot {
        schema,
        planner: Some(planner),
        activity_by_node,
    }
}

#[test]
fn skips_small_tables() {
    let snap = annotated(
        vec![ddl_table("tiny")],
        vec![("tiny", 100.0, 0)],
        vec![("tiny", 10)],
    );
    let results = analyze_vacuum_health(&snap.view());
    assert!(results.is_empty());
}

#[test]
fn reports_large_table_with_defaults() {
    let snap = annotated(
        vec![ddl_table("big")],
        vec![("big", 5_000_000.0, 0)],
        vec![("big", 100)],
    );
    let results = analyze_vacuum_health(&snap.view());
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .recommendations
            .iter()
            .any(|r| r.contains("large table"))
    );
}

#[test]
fn reports_high_dead_ratio() {
    let snap = annotated(
        vec![ddl_table("dirty")],
        vec![("dirty", 100_000.0, 0)],
        vec![("dirty", 20_000)],
    );
    let results = analyze_vacuum_health(&snap.view());
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .recommendations
            .iter()
            .any(|r| r.contains("high dead tuple"))
    );
}

#[test]
fn disabled_autovacuum_warns() {
    let mut table = ddl_table("bad");
    table.reloptions = vec!["autovacuum_enabled=false".into()];
    let snap = annotated(vec![table], vec![("bad", 100_000.0, 0)], vec![("bad", 100)]);
    let results = analyze_vacuum_health(&snap.view());
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .recommendations
            .iter()
            .any(|r| r.contains("disabled"))
    );
    assert!(!results[0].autovacuum_enabled);
}

#[test]
fn skipped_when_planner_absent() {
    // Degradation case: schema has the table but planner is None → reltuples
    // returns None → skipped. Pins the new "no data → no findings" path.
    let snap = AnnotatedSnapshot {
        schema: make_snap(vec![ddl_table("big")]),
        planner: None,
        activity_by_node: BTreeMap::new(),
    };
    assert!(analyze_vacuum_health(&snap.view()).is_empty());
}

#[test]
fn dead_tuples_summed_across_replicas() {
    // 3-node cluster, dead_tuples reported per node. Cluster sum drives the
    // ratio check.
    let schema = make_snap(vec![ddl_table("hot")]);
    let planner = PlannerStatsSnapshot {
        pg_version: "16.0".into(),
        database: "test".into(),
        timestamp: chrono::Utc::now(),
        content_hash: "ph".into(),
        schema_ref_hash: "sh".into(),
        tables: vec![TableSizingEntry {
            table: QualifiedName::new("public", "hot"),
            sizing: TableSizing {
                reltuples: 100_000.0,
                relpages: 1000,
                table_size: 0,
                total_size: None,
                index_size: None,
            },
        }],
        columns: vec![],
        indexes: vec![],
    };
    let mut activity_by_node = BTreeMap::new();
    for (label, dead) in [
        ("primary", 8_000_i64),
        ("replica1", 7_000),
        ("replica2", 6_000),
    ] {
        activity_by_node.insert(
            label.into(),
            ActivityStatsSnapshot {
                pg_version: "16.0".into(),
                database: "test".into(),
                timestamp: chrono::Utc::now(),
                content_hash: format!("h-{label}"),
                schema_ref_hash: "sh".into(),
                node: NodeIdentity {
                    label: label.into(),
                    host: label.into(),
                    is_standby: label != "primary",
                    replication_lag_bytes: None,
                    stats_reset: None,
                },
                tables: vec![TableActivityEntry {
                    table: QualifiedName::new("public", "hot"),
                    activity: TableActivity {
                        seq_scan: 0,
                        idx_scan: 0,
                        n_live_tup: 0,
                        n_dead_tup: dead,
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
                indexes: vec![],
            },
        );
    }
    let snap = AnnotatedSnapshot {
        schema,
        planner: Some(planner),
        activity_by_node,
    };
    let results = analyze_vacuum_health(&snap.view());
    assert_eq!(results.len(), 1);
    // 8k+7k+6k = 21k dead vs 100k live → 21% > 10% threshold
    assert_eq!(results[0].dead_tuples, 21_000);
    assert!(
        results[0]
            .recommendations
            .iter()
            .any(|r| r.contains("high dead tuple"))
    );
}

#[test]
fn parses_defaults_from_gucs() {
    let gucs = vec![
        GucSetting {
            name: "autovacuum_vacuum_threshold".into(),
            setting: "100".into(),
            unit: None,
        },
        GucSetting {
            name: "autovacuum_vacuum_scale_factor".into(),
            setting: "0.05".into(),
            unit: None,
        },
        GucSetting {
            name: "autovacuum_analyze_threshold".into(),
            setting: "200".into(),
            unit: None,
        },
        GucSetting {
            name: "autovacuum_analyze_scale_factor".into(),
            setting: "0.02".into(),
            unit: None,
        },
    ];
    let d = parse_autovacuum_defaults(&gucs);
    assert_eq!(d.vacuum_threshold, 100);
    assert!((d.vacuum_scale_factor - 0.05).abs() < f64::EPSILON);
    assert_eq!(d.analyze_threshold, 200);
    assert!((d.analyze_scale_factor - 0.02).abs() < f64::EPSILON);
}
