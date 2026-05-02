use super::*;

#[test]
fn qualified_name_displays_schema_dot_name() {
    let qn = QualifiedName::new("public", "orders");
    assert_eq!(qn.to_string(), "public.orders");
}

#[test]
fn qualified_name_round_trips_through_serde() {
    let qn = QualifiedName::new("public", "orders");
    let json = serde_json::to_string(&qn).unwrap();
    let back: QualifiedName = serde_json::from_str(&json).unwrap();
    assert_eq!(back, qn);
}

fn sample_planner_stats() -> PlannerStatsSnapshot {
    PlannerStatsSnapshot {
        pg_version: "PostgreSQL 17.0".into(),
        database: "accounts".into(),
        timestamp: Utc::now(),
        content_hash: "abc123".into(),
        schema_ref_hash: "def456".into(),
        tables: vec![TableSizingEntry {
            table: QualifiedName::new("public", "orders"),
            sizing: TableSizing {
                reltuples: 1234.0,
                relpages: 42,
                table_size: 1_000_000,
                total_size: Some(2_000_000),
                index_size: Some(1_000_000),
            },
        }],
        columns: vec![ColumnStatsEntry {
            table: QualifiedName::new("public", "orders"),
            column: "user_id".into(),
            stats: ColumnStats {
                null_frac: Some(0.0),
                n_distinct: Some(-0.5),
                most_common_vals: None,
                most_common_freqs: None,
                histogram_bounds: None,
                correlation: Some(0.1),
            },
        }],
        indexes: vec![IndexSizingEntry {
            index: QualifiedName::new("public", "orders_pkey"),
            sizing: IndexSizing {
                size: 8192,
                relpages: 1,
                reltuples: 1234.0,
            },
        }],
    }
}

fn sample_activity_stats() -> ActivityStatsSnapshot {
    ActivityStatsSnapshot {
        pg_version: "PostgreSQL 17.0".into(),
        database: "accounts".into(),
        timestamp: Utc::now(),
        content_hash: "h1".into(),
        schema_ref_hash: "h2".into(),
        node: NodeIdentity {
            label: "primary".into(),
            host: "10.0.0.1".into(),
            is_standby: false,
            replication_lag_bytes: None,
            stats_reset: None,
        },
        tables: vec![TableActivityEntry {
            table: QualifiedName::new("public", "orders"),
            activity: TableActivity {
                seq_scan: 7,
                idx_scan: 100,
                n_live_tup: 1000,
                n_dead_tup: 5,
                last_vacuum: None,
                last_autovacuum: None,
                last_analyze: None,
                last_autoanalyze: None,
                vacuum_count: 0,
                autovacuum_count: 1,
                analyze_count: 0,
                autoanalyze_count: 1,
            },
        }],
        indexes: vec![IndexActivityEntry {
            index: QualifiedName::new("public", "orders_pkey"),
            activity: IndexActivity {
                idx_scan: 100,
                idx_tup_read: 200,
                idx_tup_fetch: 150,
            },
        }],
    }
}

#[test]
fn planner_stats_round_trips_through_json() {
    let snap = sample_planner_stats();
    let json = serde_json::to_string(&snap).unwrap();
    let back: PlannerStatsSnapshot = serde_json::from_str(&json).unwrap();
    assert_eq!(back.tables.len(), 1);
    assert_eq!(back.tables[0].table, snap.tables[0].table);
    assert_eq!(back.columns.len(), 1);
    assert_eq!(back.columns[0].column, "user_id");
    assert_eq!(back.indexes.len(), 1);
    assert_eq!(back.indexes[0].index.name, "orders_pkey");
    assert_eq!(back.schema_ref_hash, "def456");
}

#[test]
fn activity_stats_round_trips_through_json() {
    let snap = sample_activity_stats();
    let json = serde_json::to_string(&snap).unwrap();
    let back: ActivityStatsSnapshot = serde_json::from_str(&json).unwrap();
    assert_eq!(back.node.label, "primary");
    assert!(!back.node.is_standby);
    assert_eq!(back.tables[0].activity.seq_scan, 7);
    assert_eq!(back.indexes[0].activity.idx_scan, 100);
}

#[test]
fn activity_stats_accepts_missing_optional_fields() {
    // Older payloads without the *_count fields and without lag should still load.
    let json = r#"{
        "pg_version": "PostgreSQL 17.0",
        "database": "accounts",
        "timestamp": "2026-01-01T00:00:00Z",
        "content_hash": "h1",
        "schema_ref_hash": "h2",
        "node": {
            "label": "replica1",
            "host": "10.0.0.2",
            "is_standby": true
        },
        "tables": [{
            "table": {"schema": "public", "name": "orders"},
            "activity": {
                "seq_scan": 1,
                "idx_scan": 2,
                "last_vacuum": null,
                "last_autovacuum": null,
                "last_analyze": null,
                "last_autoanalyze": null
            }
        }],
        "indexes": []
    }"#;
    let back: ActivityStatsSnapshot = serde_json::from_str(json).unwrap();
    assert!(back.node.is_standby);
    assert!(back.node.replication_lag_bytes.is_none());
    assert_eq!(back.tables[0].activity.n_live_tup, 0);
    assert_eq!(back.tables[0].activity.vacuum_count, 0);
}

#[test]
fn node_selector_variants_are_constructable() {
    let _ = NodeSelector::All;
    match NodeSelector::Some(vec!["primary".into(), "replica1".into()]) {
        NodeSelector::Some(v) => assert_eq!(v.len(), 2),
        NodeSelector::All => panic!("wrong variant"),
    }
}

fn activity_for(
    label: &str,
    idx_scan: i64,
    seq_scan: i64,
    n_dead_tup: i64,
    last_vacuum: Option<DateTime<Utc>>,
    last_autovacuum: Option<DateTime<Utc>>,
    stats_reset: Option<DateTime<Utc>>,
) -> ActivityStatsSnapshot {
    ActivityStatsSnapshot {
        pg_version: "PostgreSQL 17.0".into(),
        database: "accounts".into(),
        timestamp: Utc::now(),
        content_hash: format!("hash-{label}"),
        schema_ref_hash: "schema-h".into(),
        node: NodeIdentity {
            label: label.into(),
            host: format!("10.0.0.{label}"),
            is_standby: label != "primary",
            replication_lag_bytes: None,
            stats_reset,
        },
        tables: vec![TableActivityEntry {
            table: QualifiedName::new("public", "orders"),
            activity: TableActivity {
                seq_scan,
                idx_scan,
                n_live_tup: 0,
                n_dead_tup,
                last_vacuum,
                last_autovacuum,
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
                idx_scan,
                idx_tup_read: 0,
                idx_tup_fetch: 0,
            },
        }],
    }
}

fn empty_schema_snap() -> SchemaSnapshot {
    SchemaSnapshot {
        pg_version: "PostgreSQL 17.0".into(),
        database: "accounts".into(),
        timestamp: Utc::now(),
        content_hash: "schema-h".into(),
        source: None,
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

fn snap_with_nodes(nodes: Vec<ActivityStatsSnapshot>) -> AnnotatedSnapshot {
    let mut activity_by_node = BTreeMap::new();
    for n in nodes {
        activity_by_node.insert(n.node.label.clone(), n);
    }
    AnnotatedSnapshot {
        schema: empty_schema_snap(),
        planner: None,
        activity_by_node,
    }
}

#[test]
fn merged_activity_idx_scan_sum_across_nodes() {
    let snap = snap_with_nodes(vec![
        activity_for("primary", 10, 0, 0, None, None, None),
        activity_for("replica1", 20, 0, 0, None, None, None),
        activity_for("replica2", 5, 0, 0, None, None, None),
    ]);
    let merged = snap.merged(&NodeSelector::All).expect("3 nodes");
    let ix = QualifiedName::new("public", "orders_pkey");
    assert_eq!(merged.idx_scan_sum(&ix), 35);
}

#[test]
fn merged_activity_idx_scan_per_node_returns_breakdown() {
    let snap = snap_with_nodes(vec![
        activity_for("primary", 10, 0, 0, None, None, None),
        activity_for("replica1", 20, 0, 0, None, None, None),
    ]);
    let merged = snap.merged(&NodeSelector::All).unwrap();
    let ix = QualifiedName::new("public", "orders_pkey");
    let per_node = merged.idx_scan_per_node(&ix);
    // BTreeMap ordering: primary < replica1
    assert_eq!(
        per_node,
        vec![("primary".into(), 10), ("replica1".into(), 20)]
    );
}

#[test]
fn merged_activity_seq_scan_sum_across_nodes() {
    let snap = snap_with_nodes(vec![
        activity_for("primary", 0, 3, 0, None, None, None),
        activity_for("replica1", 0, 7, 0, None, None, None),
    ]);
    let merged = snap.merged(&NodeSelector::All).unwrap();
    let t = QualifiedName::new("public", "orders");
    assert_eq!(merged.seq_scan_sum(&t), 10);
}

#[test]
fn merged_activity_n_dead_tup_sums_across_nodes() {
    let snap = snap_with_nodes(vec![
        activity_for("primary", 0, 0, 100, None, None, None),
        activity_for("replica1", 0, 0, 50, None, None, None),
    ]);
    let merged = snap.merged(&NodeSelector::All).unwrap();
    let t = QualifiedName::new("public", "orders");
    assert_eq!(merged.n_dead_tup_sum(&t), 150);
}

#[test]
fn merged_activity_last_vacuum_max_picks_max_across_nodes_and_kinds() {
    let early = "2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
    let mid = "2026-02-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
    let late = "2026-03-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
    let snap = snap_with_nodes(vec![
        // primary: manual at early, autovacuum at mid → node max = mid
        activity_for("primary", 0, 0, 0, Some(early), Some(mid), None),
        // replica1: autovacuum at late → node max = late
        activity_for("replica1", 0, 0, 0, None, Some(late), None),
    ]);
    let merged = snap.merged(&NodeSelector::All).unwrap();
    let t = QualifiedName::new("public", "orders");
    assert_eq!(merged.last_vacuum_max(&t), Some(late));
}

#[test]
fn merged_activity_last_vacuum_max_returns_none_when_never_vacuumed() {
    let snap = snap_with_nodes(vec![
        activity_for("primary", 0, 0, 0, None, None, None),
        activity_for("replica1", 0, 0, 0, None, None, None),
    ]);
    let merged = snap.merged(&NodeSelector::All).unwrap();
    let t = QualifiedName::new("public", "orders");
    assert_eq!(merged.last_vacuum_max(&t), None);
}

#[test]
fn annotated_snapshot_view_defaults_to_primary() {
    let snap = snap_with_nodes(vec![
        activity_for("primary", 1, 0, 0, None, None, None),
        activity_for("replica1", 2, 0, 0, None, None, None),
    ]);
    let view = snap.view(None);
    let activity = view.activity.expect("primary should resolve by default");
    assert_eq!(activity.node.label, "primary");
}

#[test]
fn annotated_snapshot_view_unknown_label_yields_no_activity() {
    let snap = snap_with_nodes(vec![activity_for("primary", 1, 0, 0, None, None, None)]);
    let view = snap.view(Some("ghost"));
    assert!(view.activity.is_none());
}

#[test]
fn annotated_snapshot_view_single_node_has_no_merged() {
    let snap = snap_with_nodes(vec![activity_for("primary", 1, 0, 0, None, None, None)]);
    let view = snap.view(None);
    assert!(view.merged.is_none());
}

#[test]
fn annotated_snapshot_view_multi_node_populates_merged() {
    let snap = snap_with_nodes(vec![
        activity_for("primary", 1, 0, 0, None, None, None),
        activity_for("replica1", 2, 0, 0, None, None, None),
    ]);
    let view = snap.view(None);
    let merged = view.merged.expect("multi-node should produce merged view");
    assert_eq!(merged.nodes.len(), 2);
}

#[test]
fn annotated_snapshot_merged_partial_when_any_node_lacks_reset() {
    let reset = "2026-04-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
    let snap = snap_with_nodes(vec![
        activity_for("primary", 0, 0, 0, None, None, Some(reset)),
        activity_for("replica1", 0, 0, 0, None, None, None),
    ]);
    let merged = snap.merged(&NodeSelector::All).unwrap();
    assert!(
        merged.partial,
        "partial should be true when a node lacks stats_reset"
    );
}

#[test]
fn annotated_snapshot_merged_window_start_is_min_reset() {
    let early = "2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
    let later = "2026-02-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
    let snap = snap_with_nodes(vec![
        activity_for("primary", 0, 0, 0, None, None, Some(later)),
        activity_for("replica1", 0, 0, 0, None, None, Some(early)),
    ]);
    let merged = snap.merged(&NodeSelector::All).unwrap();
    assert_eq!(merged.window_start, early);
    assert!(!merged.partial);
}

#[test]
fn annotated_snapshot_merged_node_selector_some_filters() {
    let snap = snap_with_nodes(vec![
        activity_for("primary", 1, 0, 0, None, None, None),
        activity_for("replica1", 2, 0, 0, None, None, None),
        activity_for("replica2", 4, 0, 0, None, None, None),
    ]);
    let merged = snap
        .merged(&NodeSelector::Some(vec![
            "replica1".into(),
            "replica2".into(),
        ]))
        .unwrap();
    let ix = QualifiedName::new("public", "orders_pkey");
    assert_eq!(merged.idx_scan_sum(&ix), 6);
    assert_eq!(merged.nodes.len(), 2);
}

#[test]
fn annotated_snapshot_merged_returns_none_for_empty_selector() {
    let snap = snap_with_nodes(vec![]);
    assert!(snap.merged(&NodeSelector::All).is_none());
}

// -----------------------------------------------------------------------
// Layer A: AnnotatedSchema accessors — planner reads + activity fall-through
// -----------------------------------------------------------------------

fn planner_for_orders(reltuples: f64, table_size: i64) -> PlannerStatsSnapshot {
    PlannerStatsSnapshot {
        pg_version: "PostgreSQL 17.0".into(),
        database: "accounts".into(),
        timestamp: Utc::now(),
        content_hash: "ph".into(),
        schema_ref_hash: "schema-h".into(),
        tables: vec![TableSizingEntry {
            table: QualifiedName::new("public", "orders"),
            sizing: TableSizing {
                reltuples,
                relpages: 7,
                table_size,
                total_size: None,
                index_size: None,
            },
        }],
        columns: vec![ColumnStatsEntry {
            table: QualifiedName::new("public", "orders"),
            column: "user_id".into(),
            stats: ColumnStats {
                null_frac: Some(0.1),
                n_distinct: Some(-0.5),
                most_common_vals: None,
                most_common_freqs: None,
                histogram_bounds: None,
                correlation: Some(0.5),
            },
        }],
        indexes: vec![IndexSizingEntry {
            index: QualifiedName::new("public", "orders_pkey"),
            sizing: IndexSizing {
                size: 16384,
                relpages: 2,
                reltuples,
            },
        }],
    }
}

fn snap_with_planner(p: PlannerStatsSnapshot) -> AnnotatedSnapshot {
    AnnotatedSnapshot {
        schema: empty_schema_snap(),
        planner: Some(p),
        activity_by_node: BTreeMap::new(),
    }
}

fn snap_full(
    planner: Option<PlannerStatsSnapshot>,
    activity: Vec<ActivityStatsSnapshot>,
) -> AnnotatedSnapshot {
    let mut activity_by_node = BTreeMap::new();
    for a in activity {
        activity_by_node.insert(a.node.label.clone(), a);
    }
    AnnotatedSnapshot {
        schema: empty_schema_snap(),
        planner,
        activity_by_node,
    }
}

#[test]
fn reltuples_reads_from_planner() {
    let snap = snap_with_planner(planner_for_orders(1234.0, 1_000_000));
    let view = snap.view(None);
    assert_eq!(
        view.reltuples(&QualifiedName::new("public", "orders")),
        Some(1234.0)
    );
}

#[test]
fn reltuples_returns_none_when_planner_missing() {
    let snap = snap_full(None, vec![]);
    let view = snap.view(None);
    assert!(
        view.reltuples(&QualifiedName::new("public", "orders"))
            .is_none()
    );
}

#[test]
fn reltuples_returns_none_for_unknown_table() {
    let snap = snap_with_planner(planner_for_orders(1234.0, 1_000_000));
    let view = snap.view(None);
    assert!(
        view.reltuples(&QualifiedName::new("public", "ghost"))
            .is_none()
    );
}

#[test]
fn table_size_relpages_index_sizing_read_from_planner() {
    let snap = snap_with_planner(planner_for_orders(50.0, 99));
    let view = snap.view(None);
    let t = QualifiedName::new("public", "orders");
    let ix = QualifiedName::new("public", "orders_pkey");
    assert_eq!(view.table_size(&t), Some(99));
    assert_eq!(view.relpages(&t), Some(7));
    assert_eq!(view.index_sizing(&ix).map(|s| s.size), Some(16384));
}

#[test]
fn column_stats_reads_from_planner() {
    let snap = snap_with_planner(planner_for_orders(1.0, 1));
    let view = snap.view(None);
    let stats = view
        .column_stats(&QualifiedName::new("public", "orders"), "user_id")
        .expect("user_id stats");
    assert_eq!(stats.null_frac, Some(0.1));
    assert!(
        view.column_stats(&QualifiedName::new("public", "orders"), "ghost")
            .is_none()
    );
}

#[test]
fn idx_scan_sum_falls_through_merged_to_single_to_zero() {
    let ix = QualifiedName::new("public", "orders_pkey");

    // 1. multi-node activity → uses merged
    let multi = snap_full(
        None,
        vec![
            activity_for("primary", 10, 0, 0, None, None, None),
            activity_for("replica1", 5, 0, 0, None, None, None),
        ],
    );
    assert_eq!(multi.view(None).idx_scan_sum(&ix), 15);

    // 2. single-node activity, merged is None → reads single
    let single = snap_full(
        None,
        vec![activity_for("primary", 7, 0, 0, None, None, None)],
    );
    assert_eq!(single.view(None).idx_scan_sum(&ix), 7);

    // 3. no activity at all → 0
    let none = snap_full(None, vec![]);
    assert_eq!(none.view(None).idx_scan_sum(&ix), 0);
}

#[test]
fn seq_scan_sum_falls_through_merged_to_single_to_zero() {
    let t = QualifiedName::new("public", "orders");
    let multi = snap_full(
        None,
        vec![
            activity_for("primary", 0, 3, 0, None, None, None),
            activity_for("replica1", 0, 4, 0, None, None, None),
        ],
    );
    let single = snap_full(
        None,
        vec![activity_for("primary", 0, 9, 0, None, None, None)],
    );
    let none = snap_full(None, vec![]);
    assert_eq!(multi.view(None).seq_scan_sum(&t), 7);
    assert_eq!(single.view(None).seq_scan_sum(&t), 9);
    assert_eq!(none.view(None).seq_scan_sum(&t), 0);
}

#[test]
fn n_dead_tup_sum_falls_through_merged_to_single_to_zero() {
    let t = QualifiedName::new("public", "orders");
    let multi = snap_full(
        None,
        vec![
            activity_for("primary", 0, 0, 100, None, None, None),
            activity_for("replica1", 0, 0, 50, None, None, None),
        ],
    );
    let single = snap_full(
        None,
        vec![activity_for("primary", 0, 0, 42, None, None, None)],
    );
    let none = snap_full(None, vec![]);
    assert_eq!(multi.view(None).n_dead_tup_sum(&t), 150);
    assert_eq!(single.view(None).n_dead_tup_sum(&t), 42);
    assert_eq!(none.view(None).n_dead_tup_sum(&t), 0);
}

#[test]
fn last_vacuum_max_falls_through_merged_to_single_to_none() {
    let t = QualifiedName::new("public", "orders");
    let early = "2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
    let late = "2026-03-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
    let multi = snap_full(
        None,
        vec![
            activity_for("primary", 0, 0, 0, Some(early), None, None),
            activity_for("replica1", 0, 0, 0, None, Some(late), None),
        ],
    );
    let single = snap_full(
        None,
        vec![activity_for("primary", 0, 0, 0, Some(early), None, None)],
    );
    let none = snap_full(None, vec![]);
    assert_eq!(multi.view(None).last_vacuum_max(&t), Some(late));
    assert_eq!(single.view(None).last_vacuum_max(&t), Some(early));
    assert!(none.view(None).last_vacuum_max(&t).is_none());
}

#[test]
fn idx_scan_per_node_works_for_single_and_multi() {
    let ix = QualifiedName::new("public", "orders_pkey");
    let single = snap_full(
        None,
        vec![activity_for("primary", 7, 0, 0, None, None, None)],
    );
    assert_eq!(
        single.view(None).idx_scan_per_node(&ix),
        vec![("primary".into(), 7)]
    );

    let multi = snap_full(
        None,
        vec![
            activity_for("primary", 1, 0, 0, None, None, None),
            activity_for("replica1", 2, 0, 0, None, None, None),
        ],
    );
    assert_eq!(
        multi.view(None).idx_scan_per_node(&ix),
        vec![("primary".into(), 1), ("replica1".into(), 2)],
    );

    let none = snap_full(None, vec![]);
    assert!(none.view(None).idx_scan_per_node(&ix).is_empty());
}

#[test]
fn single_node_and_multi_node_one_node_parity_for_cluster_sums() {
    // The "merged is None when only one node" trap: single-node activity vs.
    // a one-entry activity_by_node map must produce the same totals.
    let ix = QualifiedName::new("public", "orders_pkey");
    let t = QualifiedName::new("public", "orders");
    // build via view default (single-node mode, merged = None)
    let one = snap_full(
        None,
        vec![activity_for("primary", 11, 5, 3, None, None, None)],
    );
    let view = one.view(None);
    assert_eq!(view.idx_scan_sum(&ix), 11);
    assert_eq!(view.seq_scan_sum(&t), 5);
    assert_eq!(view.n_dead_tup_sum(&t), 3);
}

#[test]
fn no_panic_on_fully_empty_annotated() {
    let snap = AnnotatedSnapshot {
        schema: empty_schema_snap(),
        planner: None,
        activity_by_node: BTreeMap::new(),
    };
    let view = snap.view(None);
    let t = QualifiedName::new("public", "orders");
    let ix = QualifiedName::new("public", "orders_pkey");
    assert!(view.reltuples(&t).is_none());
    assert!(view.table_size(&t).is_none());
    assert!(view.relpages(&t).is_none());
    assert!(view.column_stats(&t, "x").is_none());
    assert!(view.index_sizing(&ix).is_none());
    assert_eq!(view.seq_scan_sum(&t), 0);
    assert_eq!(view.idx_scan_sum(&ix), 0);
    assert!(view.idx_scan_per_node(&ix).is_empty());
    assert_eq!(view.n_dead_tup_sum(&t), 0);
    assert!(view.last_vacuum_max(&t).is_none());
    assert!(view.last_analyze_max(&t).is_none());
    assert_eq!(view.vacuum_count_sum(&t), 0);
}

// -----------------------------------------------------------------------
// Layer A: AnnotatedSnapshot helpers — parity with legacy free functions
// -----------------------------------------------------------------------

fn schema_with_index_def(idx_name: &str, is_primary: bool, is_unique: bool) -> SchemaSnapshot {
    SchemaSnapshot {
        tables: vec![Table {
            oid: 1,
            schema: "public".into(),
            name: "orders".into(),
            columns: vec![],
            constraints: vec![],
            indexes: vec![Index {
                name: idx_name.into(),
                columns: vec!["id".into()],
                include_columns: vec![],
                index_type: "btree".into(),
                is_unique,
                is_primary,
                predicate: None,
                definition: format!("CREATE INDEX {idx_name} ON public.orders (id)"),
                is_valid: true,
                backs_constraint: false,
            }],
            comment: None,
            partition_info: None,
            policies: vec![],
            triggers: vec![],
            reloptions: vec![],
            rls_enabled: false,
        }],
        ..empty_schema_snap()
    }
}

#[test]
fn unused_indexes_aggregates_across_nodes() {
    let schema = schema_with_index_def("idx_dead", false, false);
    let planner = PlannerStatsSnapshot {
        pg_version: "PostgreSQL 17.0".into(),
        database: "accounts".into(),
        timestamp: Utc::now(),
        content_hash: "ph".into(),
        schema_ref_hash: "schema-h".into(),
        tables: vec![],
        columns: vec![],
        indexes: vec![IndexSizingEntry {
            index: QualifiedName::new("public", "idx_dead"),
            sizing: IndexSizing {
                size: 16384,
                relpages: 2,
                reltuples: 0.0,
            },
        }],
    };
    let mut activity_by_node = BTreeMap::new();
    for label in ["primary", "replica1"] {
        activity_by_node.insert(
            label.into(),
            ActivityStatsSnapshot {
                pg_version: "PostgreSQL 17.0".into(),
                database: "accounts".into(),
                timestamp: Utc::now(),
                content_hash: format!("h-{label}"),
                schema_ref_hash: "schema-h".into(),
                node: NodeIdentity {
                    label: label.into(),
                    host: label.into(),
                    is_standby: label != "primary",
                    replication_lag_bytes: None,
                    stats_reset: None,
                },
                tables: vec![],
                indexes: vec![IndexActivityEntry {
                    index: QualifiedName::new("public", "idx_dead"),
                    activity: IndexActivity {
                        idx_scan: 0,
                        idx_tup_read: 0,
                        idx_tup_fetch: 0,
                    },
                }],
            },
        );
    }
    let snap = AnnotatedSnapshot {
        schema,
        planner: Some(planner),
        activity_by_node,
    };
    let result = snap.unused_indexes(&NodeSelector::All);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].index_name, "idx_dead");
    assert_eq!(result[0].total_size_bytes, 16384);
    assert_eq!(result[0].total_idx_scan, 0);
}

#[test]
fn unused_indexes_skips_primary_keys() {
    let schema = schema_with_index_def("orders_pkey", true, true);
    let snap = AnnotatedSnapshot {
        schema,
        planner: None,
        activity_by_node: {
            let mut m = BTreeMap::new();
            m.insert(
                "primary".into(),
                ActivityStatsSnapshot {
                    pg_version: "PostgreSQL 17.0".into(),
                    database: "accounts".into(),
                    timestamp: Utc::now(),
                    content_hash: "a".into(),
                    schema_ref_hash: "s".into(),
                    node: NodeIdentity {
                        label: "primary".into(),
                        host: "p".into(),
                        is_standby: false,
                        replication_lag_bytes: None,
                        stats_reset: None,
                    },
                    tables: vec![],
                    indexes: vec![IndexActivityEntry {
                        index: QualifiedName::new("public", "orders_pkey"),
                        activity: IndexActivity {
                            idx_scan: 0,
                            idx_tup_read: 0,
                            idx_tup_fetch: 0,
                        },
                    }],
                },
            );
            m
        },
    };
    assert!(snap.unused_indexes(&NodeSelector::All).is_empty());
}

#[test]
fn unused_indexes_empty_when_no_activity() {
    let schema = schema_with_index_def("idx_dead", false, false);
    let snap = AnnotatedSnapshot {
        schema,
        planner: None,
        activity_by_node: BTreeMap::new(),
    };
    assert!(snap.unused_indexes(&NodeSelector::All).is_empty());
}

#[test]
fn seq_scan_imbalance_flags_hot_node() {
    let snap = snap_full(
        None,
        vec![
            activity_for("primary", 0, 1000, 0, None, None, None),
            activity_for("replica1", 0, 100, 0, None, None, None),
        ],
    );
    let result = snap
        .seq_scan_imbalance(&QualifiedName::new("public", "orders"))
        .expect("10x imbalance should fire");
    assert_eq!(result.hot_node, "primary");
    assert_eq!(result.multiplier, 10);
}
