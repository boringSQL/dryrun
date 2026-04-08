use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use super::super::types::*;
use crate::error::Result;

pub(super) async fn fetch_named_table_stats(pool: &PgPool) -> Result<Vec<NodeTableStats>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT n.nspname                AS schema_name,
               c.relname                AS table_name,
               c.reltuples::float8      AS reltuples,
               c.relpages::int8         AS relpages,
               COALESCE(s.n_dead_tup, 0)::int8 AS dead_tuples,
               s.last_vacuum,
               s.last_autovacuum,
               s.last_analyze,
               s.last_autoanalyze,
               COALESCE(s.seq_scan, 0)::int8  AS seq_scan,
               COALESCE(s.idx_scan, 0)::int8  AS idx_scan,
               pg_catalog.pg_total_relation_size(c.oid)::int8 AS table_size
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          LEFT JOIN pg_catalog.pg_stat_user_tables s ON s.relid = c.oid
         WHERE c.relkind IN ('r', 'p')
           AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND n.nspname NOT LIKE 'pg_temp_%'
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| NodeTableStats {
            schema: r.get("schema_name"),
            table: r.get("table_name"),
            stats: TableStats {
                reltuples: r.get("reltuples"),
                relpages: r.get("relpages"),
                dead_tuples: r.get("dead_tuples"),
                last_vacuum: r.get("last_vacuum"),
                last_autovacuum: r.get("last_autovacuum"),
                last_analyze: r.get("last_analyze"),
                last_autoanalyze: r.get("last_autoanalyze"),
                seq_scan: r.get("seq_scan"),
                idx_scan: r.get("idx_scan"),
                table_size: r.get("table_size"),
            },
        })
        .collect())
}

pub(super) async fn fetch_named_index_stats(pool: &PgPool) -> Result<Vec<NodeIndexStats>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT n.nspname            AS schema_name,
               s.relname             AS table_name,
               s.indexrelname        AS index_name,
               COALESCE(s.idx_scan, 0)::int8      AS idx_scan,
               COALESCE(s.idx_tup_read, 0)::int8  AS idx_tup_read,
               COALESCE(s.idx_tup_fetch, 0)::int8 AS idx_tup_fetch,
               pg_catalog.pg_relation_size(s.indexrelid)::int8 AS index_size,
               ci.relpages::int8     AS index_relpages,
               ci.reltuples::float8  AS index_reltuples
          FROM pg_catalog.pg_stat_user_indexes s
          JOIN pg_catalog.pg_class ct ON ct.oid = s.relid
          JOIN pg_catalog.pg_namespace n ON n.oid = ct.relnamespace
          JOIN pg_catalog.pg_class ci ON ci.oid = s.indexrelid
         WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND n.nspname NOT LIKE 'pg_temp_%'
         ORDER BY n.nspname, s.relname, s.indexrelname
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| NodeIndexStats {
            schema: r.get("schema_name"),
            table: r.get("table_name"),
            index_name: r.get("index_name"),
            stats: IndexStats {
                idx_scan: r.get("idx_scan"),
                idx_tup_read: r.get("idx_tup_read"),
                idx_tup_fetch: r.get("idx_tup_fetch"),
                size: r.get("index_size"),
                relpages: r.get("index_relpages"),
                reltuples: r.get("index_reltuples"),
            },
        })
        .collect())
}

pub(super) async fn fetch_named_column_stats(pool: &PgPool) -> Result<Vec<NodeColumnStats>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT s.schemaname              AS schema_name,
               s.tablename               AS table_name,
               s.attname                  AS column_name,
               s.null_frac::float8        AS null_frac,
               s.n_distinct::float8       AS n_distinct,
               s.most_common_vals::text   AS most_common_vals,
               s.most_common_freqs::text  AS most_common_freqs,
               s.histogram_bounds::text   AS histogram_bounds,
               s.correlation::float8      AS correlation
          FROM pg_catalog.pg_stats s
         WHERE s.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND s.schemaname NOT LIKE 'pg_temp_%'
         ORDER BY s.schemaname, s.tablename, s.attname
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| NodeColumnStats {
            schema: r.get("schema_name"),
            table: r.get("table_name"),
            column: r.get("column_name"),
            stats: ColumnStats {
                null_frac: r.get::<Option<f64>, _>("null_frac"),
                n_distinct: r.get::<Option<f64>, _>("n_distinct"),
                most_common_vals: r.get("most_common_vals"),
                most_common_freqs: r.get("most_common_freqs"),
                histogram_bounds: r.get("histogram_bounds"),
                correlation: r.get::<Option<f64>, _>("correlation"),
            },
        })
        .collect())
}
