use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use crate::error::Result;

use super::raw_types::*;

pub(super) async fn fetch_partition_info(pool: &PgPool) -> Result<Vec<RawPartitionInfo>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT pt.partrelid::int4       AS table_oid,
               pt.partstrat::text        AS strategy,
               pg_catalog.pg_get_partkeydef(pt.partrelid) AS part_key
          FROM pg_catalog.pg_partitioned_table pt
          JOIN pg_catalog.pg_class c ON c.oid = pt.partrelid
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| RawPartitionInfo {
            table_oid: r.get::<i32, _>("table_oid") as u32,
            strategy: r.get("strategy"),
            key: r.get("part_key"),
        })
        .collect())
}

pub(super) async fn fetch_partition_children(pool: &PgPool) -> Result<Vec<RawPartitionChild>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT inh.inhparent::int4     AS parent_oid,
               n.nspname                AS schema_name,
               c.relname                AS table_name,
               pg_catalog.pg_get_expr(c.relpartbound, c.oid) AS bound
          FROM pg_catalog.pg_inherits inh
          JOIN pg_catalog.pg_class c ON c.oid = inh.inhrelid
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relispartition
           AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND n.nspname NOT LIKE 'pg_temp_%'
         ORDER BY inh.inhparent, c.relname
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| RawPartitionChild {
            parent_oid: r.get::<i32, _>("parent_oid") as u32,
            schema: r.get("schema_name"),
            name: r.get("table_name"),
            bound: r.get::<Option<String>, _>("bound").unwrap_or_default(),
        })
        .collect())
}
