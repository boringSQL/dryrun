mod stats;

use chrono::Utc;
use pg_introspect::IntrospectOptions;
use sha2::{Digest, Sha256};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use tracing::info;

use super::from_pg_introspect::catalog_to_snapshot_parts;
use super::hash::{HashInput, compute_content_hash};
use super::snapshot::*;
use super::types::*;
use crate::error::{Error, Result};

pub async fn introspect_schema(pool: &PgPool) -> Result<SchemaSnapshot> {
    let pg_version: String = sqlx::query_scalar("SELECT version()")
        .fetch_one(pool)
        .await?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await?;

    let cat = pg_introspect::introspect(pool, &IntrospectOptions::default())
        .await
        .map_err(|e| Error::Introspection(format!("pg_introspect: {e}")))?;
    let parts = catalog_to_snapshot_parts(cat);

    let gucs = fetch_gucs(pool).await?;

    let content_hash = compute_content_hash(&HashInput {
        pg_version: &pg_version,
        tables: &parts.tables,
        enums: &parts.enums,
        domains: &parts.domains,
        composites: &parts.composites,
        views: &parts.views,
        functions: &parts.functions,
        extensions: &parts.extensions,
    });

    let snapshot = SchemaSnapshot {
        pg_version,
        database,
        timestamp: Utc::now(),
        content_hash,
        source: None,
        tables: parts.tables,
        enums: parts.enums,
        domains: parts.domains,
        composites: parts.composites,
        views: parts.views,
        functions: parts.functions,
        extensions: parts.extensions,
        gucs,
    };

    info!(
        tables = snapshot.tables.len(),
        enums = snapshot.enums.len(),
        domains = snapshot.domains.len(),
        composites = snapshot.composites.len(),
        views = snapshot.views.len(),
        functions = snapshot.functions.len(),
        extensions = snapshot.extensions.len(),
        hash = %snapshot.content_hash,
        "schema introspection complete"
    );

    Ok(snapshot)
}

async fn fetch_gucs(pool: &PgPool) -> Result<Vec<GucSetting>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT name, setting, unit
          FROM pg_catalog.pg_settings
         WHERE name IN (
               'work_mem', 'effective_cache_size', 'random_page_cost',
               'seq_page_cost', 'effective_io_concurrency', 'shared_buffers',
               'maintenance_work_mem', 'default_statistics_target',
               'autovacuum', 'autovacuum_vacuum_threshold',
               'autovacuum_vacuum_scale_factor', 'autovacuum_analyze_threshold',
               'autovacuum_analyze_scale_factor'
         )
         ORDER BY name
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| GucSetting {
            name: r.get("name"),
            setting: r.get("setting"),
            unit: r.get("unit"),
        })
        .collect())
}

pub async fn fetch_is_standby(pool: &PgPool) -> Result<bool> {
    let row: PgRow = sqlx::query("SELECT pg_catalog.pg_is_in_recovery() AS is_standby")
        .fetch_one(pool)
        .await?;
    Ok(row.get("is_standby"))
}

pub async fn introspect_planner_stats(
    pool: &PgPool,
    schema_ref_hash: &str,
) -> Result<PlannerStatsSnapshot> {
    if fetch_is_standby(pool).await? {
        return Err(Error::Introspection(
            "planner stats must be captured from the primary; \
             use `dryrun snapshot activity --from <replica>` for per-node activity"
                .into(),
        ));
    }

    let pg_version: String = sqlx::query_scalar("SELECT version()")
        .fetch_one(pool)
        .await?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await?;

    let (table_sizing, index_sizing, columns) = tokio::try_join!(
        stats::fetch_named_table_sizing(pool),
        stats::fetch_named_index_sizing(pool),
        stats::fetch_named_column_stats(pool),
    )?;

    let mut snapshot = PlannerStatsSnapshot {
        pg_version,
        database,
        timestamp: Utc::now(),
        content_hash: String::new(),
        schema_ref_hash: schema_ref_hash.to_string(),
        tables: table_sizing,
        columns,
        indexes: index_sizing,
    };
    snapshot.content_hash = hash_payload(&snapshot)?;

    info!(
        tables = snapshot.tables.len(),
        columns = snapshot.columns.len(),
        indexes = snapshot.indexes.len(),
        hash = %snapshot.content_hash,
        schema_ref = %snapshot.schema_ref_hash,
        "planner stats introspection complete"
    );

    Ok(snapshot)
}

pub async fn introspect_activity_stats(
    pool: &PgPool,
    schema_ref_hash: &str,
    label: &str,
) -> Result<ActivityStatsSnapshot> {
    let pg_version: String = sqlx::query_scalar("SELECT version()")
        .fetch_one(pool)
        .await?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await?;

    let (node, table_activity, index_activity) = tokio::try_join!(
        resolve_node_identity(pool, label),
        stats::fetch_named_table_activity(pool),
        stats::fetch_named_index_activity(pool),
    )?;

    let mut snapshot = ActivityStatsSnapshot {
        pg_version,
        database,
        timestamp: Utc::now(),
        content_hash: String::new(),
        schema_ref_hash: schema_ref_hash.to_string(),
        node,
        tables: table_activity,
        indexes: index_activity,
    };
    snapshot.content_hash = hash_payload(&snapshot)?;

    info!(
        label = %snapshot.node.label,
        is_standby = snapshot.node.is_standby,
        tables = snapshot.tables.len(),
        indexes = snapshot.indexes.len(),
        hash = %snapshot.content_hash,
        schema_ref = %snapshot.schema_ref_hash,
        "activity stats introspection complete"
    );

    Ok(snapshot)
}

async fn resolve_node_identity(pool: &PgPool, label: &str) -> Result<NodeIdentity> {
    let row: PgRow = sqlx::query(
        r#"
        SELECT pg_catalog.pg_is_in_recovery()                           AS is_standby,
               COALESCE(host(pg_catalog.inet_server_addr())::text, '')  AS host,
               (SELECT stats_reset
                  FROM pg_catalog.pg_stat_database
                 WHERE datname = current_database())                    AS stats_reset,
               CASE
                 WHEN pg_catalog.pg_is_in_recovery()
                   THEN pg_catalog.pg_wal_lsn_diff(
                          pg_catalog.pg_last_wal_receive_lsn(),
                          pg_catalog.pg_last_wal_replay_lsn())::int8
                 ELSE NULL
               END                                                      AS lag_bytes
        "#,
    )
    .fetch_one(pool)
    .await?;

    Ok(NodeIdentity {
        label: label.to_string(),
        host: row.get::<String, _>("host"),
        is_standby: row.get("is_standby"),
        replication_lag_bytes: row.get::<Option<i64>, _>("lag_bytes"),
        stats_reset: row.get("stats_reset"),
    })
}

fn hash_payload<T: serde::Serialize>(value: &T) -> Result<String> {
    let json = serde_json::to_vec(value)
        .map_err(|e| Error::Introspection(format!("cannot serialize for hashing: {e}")))?;
    let digest = Sha256::digest(&json);
    Ok(format!("{digest:x}"))
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn fixed_planner() -> PlannerStatsSnapshot {
        PlannerStatsSnapshot {
            pg_version: "PostgreSQL 17.0".into(),
            database: "accounts".into(),
            timestamp: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            content_hash: String::new(),
            schema_ref_hash: "schema-h1".into(),
            tables: vec![],
            columns: vec![],
            indexes: vec![],
        }
    }

    #[test]
    fn hash_payload_is_deterministic_for_identical_inputs() {
        let a = fixed_planner();
        let b = fixed_planner();
        assert_eq!(hash_payload(&a).unwrap(), hash_payload(&b).unwrap());
    }

    #[test]
    fn hash_payload_changes_when_payload_changes() {
        let a = fixed_planner();
        let mut b = fixed_planner();
        b.schema_ref_hash = "schema-h2".into();
        assert_ne!(hash_payload(&a).unwrap(), hash_payload(&b).unwrap());
    }

    #[test]
    fn hash_payload_emits_hex_sha256() {
        let h = hash_payload(&fixed_planner()).unwrap();
        assert_eq!(h.len(), 64);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
