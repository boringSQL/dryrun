mod catalog;
mod comments;
mod indexes;
mod objects;
mod partitions;
mod policies;
mod raw_types;
mod stats;
mod tables;

use std::collections::HashMap;

use chrono::Utc;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use tracing::info;

use sha2::{Digest, Sha256};

use super::hash::{HashInput, compute_content_hash};
use super::types::*;
use crate::error::{Error, Result};

pub async fn introspect_schema(pool: &PgPool) -> Result<SchemaSnapshot> {
    let pg_version: String = sqlx::query_scalar("SELECT version()")
        .fetch_one(pool)
        .await?;

    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await?;

    // Group 1: table-centric data. Stats now live in PlannerStatsSnapshot /
    // ActivityStatsSnapshot; introspect_schema is DDL-only.
    let (
        raw_tables,
        raw_columns,
        raw_constraints,
        table_comments,
        column_comments,
        raw_indexes,
        raw_partitions,
        raw_partition_children,
        raw_policies,
        raw_triggers,
    ) = tokio::try_join!(
        tables::fetch_tables(pool),
        tables::fetch_columns(pool),
        tables::fetch_constraints(pool),
        comments::fetch_table_comments(pool),
        comments::fetch_column_comments(pool),
        indexes::fetch_indexes(pool),
        partitions::fetch_partition_info(pool),
        partitions::fetch_partition_children(pool),
        policies::fetch_policies(pool),
        policies::fetch_triggers(pool),
    )?;

    // Group 2: top-level objects.
    let (enums, domains, composites, views, functions, extensions, gucs, _is_standby) = tokio::try_join!(
        catalog::fetch_enums(pool),
        catalog::fetch_domains(pool),
        catalog::fetch_composites(pool),
        objects::fetch_views(pool),
        objects::fetch_functions(pool),
        objects::fetch_extensions(pool),
        objects::fetch_gucs(pool),
        fetch_is_standby(pool),
    )?;

    let tables = assemble_tables(
        raw_tables,
        raw_columns,
        raw_constraints,
        table_comments,
        column_comments,
        raw_indexes,
        raw_partitions,
        raw_partition_children,
        raw_policies,
        raw_triggers,
    );

    let content_hash = compute_content_hash(&HashInput {
        pg_version: &pg_version,
        tables: &tables,
        enums: &enums,
        domains: &domains,
        composites: &composites,
        views: &views,
        functions: &functions,
        extensions: &extensions,
    });

    let snapshot = SchemaSnapshot {
        pg_version,
        database,
        timestamp: Utc::now(),
        content_hash,
        source: None,
        tables,
        enums,
        domains,
        composites,
        views,
        functions,
        extensions,
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

pub async fn fetch_is_standby(pool: &PgPool) -> Result<bool> {
    let row: PgRow = sqlx::query("SELECT pg_catalog.pg_is_in_recovery() AS is_standby")
        .fetch_one(pool)
        .await?;
    Ok(row.get("is_standby"))
}

// Snapshot split: planner-only and per-node activity captures

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

// ---------------------------------------------------------------------------
// Assembly: merge parts into Table structs
// ---------------------------------------------------------------------------

use raw_types::*;

#[allow(clippy::too_many_arguments)]
fn assemble_tables(
    raw_tables: Vec<RawTable>,
    raw_columns: Vec<RawColumn>,
    raw_constraints: Vec<RawConstraint>,
    table_comments: Vec<RawTableComment>,
    column_comments: Vec<RawColumnComment>,
    raw_indexes: Vec<RawIndex>,
    raw_partitions: Vec<RawPartitionInfo>,
    raw_partition_children: Vec<RawPartitionChild>,
    raw_policies: Vec<RawPolicy>,
    raw_triggers: Vec<RawTrigger>,
) -> Vec<Table> {
    // --- Columns ---
    let mut columns_by_oid: HashMap<u32, Vec<Column>> = HashMap::new();
    for rc in raw_columns {
        columns_by_oid
            .entry(rc.table_oid)
            .or_default()
            .push(Column {
                name: rc.name,
                ordinal: rc.ordinal,
                type_name: rc.type_name,
                nullable: rc.nullable,
                default: rc.default,
                identity: rc.identity,
                generated: rc.generated,
                comment: None,
                statistics_target: rc.statistics_target,
            });
    }

    // --- Constraints ---
    let mut constraints_by_oid: HashMap<u32, Vec<Constraint>> = HashMap::new();
    for rc in raw_constraints {
        let kind = match ConstraintKind::from_pg_contype(&rc.contype) {
            Some(k) => k,
            None => continue,
        };
        constraints_by_oid
            .entry(rc.table_oid)
            .or_default()
            .push(Constraint {
                name: rc.name,
                kind,
                columns: rc.columns,
                definition: rc.definition,
                fk_table: rc.fk_table,
                fk_columns: rc.fk_columns,
                backing_index: rc.backing_index,
                comment: rc.comment,
            });
    }

    // --- Table comments ---
    let table_comment_map: HashMap<u32, String> = table_comments
        .into_iter()
        .map(|tc| (tc.table_oid, tc.comment))
        .collect();

    // --- Column comments ---
    let col_comment_map: HashMap<(u32, String), String> = column_comments
        .into_iter()
        .map(|cc| ((cc.table_oid, cc.column_name), cc.comment))
        .collect();

    for (oid, cols) in &mut columns_by_oid {
        for col in cols.iter_mut() {
            if let Some(comment) = col_comment_map.get(&(*oid, col.name.clone())) {
                col.comment = Some(comment.clone());
            }
        }
    }

    // --- Indexes ---
    let mut indexes_by_oid: HashMap<u32, Vec<Index>> = HashMap::new();
    for ri in raw_indexes {
        indexes_by_oid.entry(ri.table_oid).or_default().push(Index {
            name: ri.name,
            columns: ri.columns,
            include_columns: ri.include_columns,
            index_type: ri.index_type,
            is_unique: ri.is_unique,
            is_primary: ri.is_primary,
            predicate: ri.predicate,
            definition: ri.definition,
            is_valid: ri.is_valid,
            backs_constraint: ri.backs_constraint,
        });
    }

    // --- Partition info ---
    let mut children_by_parent: HashMap<u32, Vec<PartitionChild>> = HashMap::new();
    for pc in raw_partition_children {
        children_by_parent
            .entry(pc.parent_oid)
            .or_default()
            .push(PartitionChild {
                schema: pc.schema,
                name: pc.name,
                bound: pc.bound,
            });
    }

    let partition_info_by_oid: HashMap<u32, PartitionInfo> = raw_partitions
        .into_iter()
        .filter_map(|rp| {
            let strategy = PartitionStrategy::from_pg_partstrat(&rp.strategy)?;
            Some((
                rp.table_oid,
                PartitionInfo {
                    strategy,
                    key: rp.key,
                    children: children_by_parent.remove(&rp.table_oid).unwrap_or_default(),
                },
            ))
        })
        .collect();

    // --- Policies ---
    let mut policies_by_oid: HashMap<u32, Vec<RlsPolicy>> = HashMap::new();
    for rp in raw_policies {
        policies_by_oid
            .entry(rp.table_oid)
            .or_default()
            .push(RlsPolicy {
                name: rp.name,
                command: rp.command,
                permissive: rp.permissive,
                roles: rp.roles,
                using_expr: rp.using_expr,
                with_check_expr: rp.with_check_expr,
            });
    }

    // --- Triggers ---
    let mut triggers_by_oid: HashMap<u32, Vec<Trigger>> = HashMap::new();
    for rt in raw_triggers {
        triggers_by_oid
            .entry(rt.table_oid)
            .or_default()
            .push(Trigger {
                name: rt.name,
                definition: rt.definition,
            });
    }

    // --- Assemble ---
    raw_tables
        .into_iter()
        .map(|rt| Table {
            oid: rt.oid,
            schema: rt.schema,
            name: rt.name,
            columns: columns_by_oid.remove(&rt.oid).unwrap_or_default(),
            constraints: constraints_by_oid.remove(&rt.oid).unwrap_or_default(),
            indexes: indexes_by_oid.remove(&rt.oid).unwrap_or_default(),
            comment: table_comment_map.get(&rt.oid).cloned(),
            partition_info: partition_info_by_oid.get(&rt.oid).cloned(),
            policies: policies_by_oid.remove(&rt.oid).unwrap_or_default(),
            triggers: triggers_by_oid.remove(&rt.oid).unwrap_or_default(),
            reloptions: rt.reloptions,
            rls_enabled: rt.rls_enabled,
        })
        .collect()
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
