use chrono::Utc;

use crate::history::{DatabaseId, ProjectId, SnapshotKey};
use crate::schema::{
    ActivityStatsSnapshot, IndexActivity, IndexActivityEntry, NodeIdentity, PlannerStatsSnapshot,
    QualifiedName, SchemaSnapshot, TableActivity, TableActivityEntry,
};

pub(super) fn make_snap(hash: &str, database: &str) -> SchemaSnapshot {
    SchemaSnapshot {
        pg_version: "PostgreSQL 17.0".into(),
        database: database.into(),
        timestamp: Utc::now(),
        content_hash: hash.into(),
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

pub(super) fn make_planner(schema_ref: &str, db: &str, hash: &str) -> PlannerStatsSnapshot {
    PlannerStatsSnapshot {
        pg_version: "PostgreSQL 17.0".into(),
        database: db.into(),
        timestamp: Utc::now(),
        content_hash: hash.into(),
        schema_ref_hash: schema_ref.into(),
        tables: vec![],
        columns: vec![],
        indexes: vec![],
    }
}

pub(super) fn make_activity(
    schema_ref: &str,
    db: &str,
    label: &str,
    hash: &str,
) -> ActivityStatsSnapshot {
    ActivityStatsSnapshot {
        pg_version: "PostgreSQL 17.0".into(),
        database: db.into(),
        timestamp: Utc::now(),
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
                idx_scan: 2,
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

pub(super) fn key(proj: &str, db: &str) -> SnapshotKey {
    SnapshotKey {
        project_id: ProjectId(proj.into()),
        database_id: DatabaseId(db.into()),
    }
}
