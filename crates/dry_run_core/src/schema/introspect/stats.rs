use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use super::super::snapshot::*;
use super::super::types::*;
use crate::error::Result;

pub(super) async fn fetch_named_column_stats(pool: &PgPool) -> Result<Vec<ColumnStatsEntry>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT s.schemaname              AS schema_name,
               s.tablename               AS table_name,
               s.attname                 AS column_name,
               s.null_frac::float8       AS null_frac,
               s.n_distinct::float8      AS n_distinct,
               s.most_common_vals::text  AS most_common_vals,
               s.most_common_freqs::text AS most_common_freqs,
               s.histogram_bounds::text  AS histogram_bounds,
               s.correlation::float8     AS correlation
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
        .map(|r| ColumnStatsEntry {
            table: QualifiedName::new(
                r.get::<String, _>("schema_name"),
                r.get::<String, _>("table_name"),
            ),
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

pub(super) async fn fetch_named_table_sizing(pool: &PgPool) -> Result<Vec<TableSizingEntry>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT n.nspname            AS schema_name,
               c.relname            AS table_name,
               c.reltuples::float8  AS reltuples,
               c.relpages::int8     AS relpages,
               pg_catalog.pg_relation_size(c.oid)::int8       AS table_size,
               pg_catalog.pg_total_relation_size(c.oid)::int8 AS total_size,
               pg_catalog.pg_indexes_size(c.oid)::int8        AS index_size
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind IN ('r', 'p')
           AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND n.nspname NOT LIKE 'pg_temp_%'
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| TableSizingEntry {
            table: QualifiedName::new(
                r.get::<String, _>("schema_name"),
                r.get::<String, _>("table_name"),
            ),
            sizing: TableSizing {
                reltuples: r.get("reltuples"),
                relpages: r.get("relpages"),
                table_size: r.get("table_size"),
                total_size: Some(r.get("total_size")),
                index_size: Some(r.get("index_size")),
            },
        })
        .collect())
}

pub(super) async fn fetch_named_table_activity(pool: &PgPool) -> Result<Vec<TableActivityEntry>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT n.nspname                   AS schema_name,
               c.relname                   AS table_name,
               COALESCE(s.seq_scan, 0)::int8        AS seq_scan,
               COALESCE(s.idx_scan, 0)::int8        AS idx_scan,
               COALESCE(s.n_live_tup, 0)::int8      AS n_live_tup,
               COALESCE(s.n_dead_tup, 0)::int8      AS n_dead_tup,
               s.last_vacuum,
               s.last_autovacuum,
               s.last_analyze,
               s.last_autoanalyze,
               COALESCE(s.vacuum_count, 0)::int8       AS vacuum_count,
               COALESCE(s.autovacuum_count, 0)::int8   AS autovacuum_count,
               COALESCE(s.analyze_count, 0)::int8      AS analyze_count,
               COALESCE(s.autoanalyze_count, 0)::int8  AS autoanalyze_count
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
        .map(|r| TableActivityEntry {
            table: QualifiedName::new(
                r.get::<String, _>("schema_name"),
                r.get::<String, _>("table_name"),
            ),
            activity: TableActivity {
                seq_scan: r.get("seq_scan"),
                idx_scan: r.get("idx_scan"),
                n_live_tup: r.get("n_live_tup"),
                n_dead_tup: r.get("n_dead_tup"),
                last_vacuum: r.get("last_vacuum"),
                last_autovacuum: r.get("last_autovacuum"),
                last_analyze: r.get("last_analyze"),
                last_autoanalyze: r.get("last_autoanalyze"),
                vacuum_count: r.get("vacuum_count"),
                autovacuum_count: r.get("autovacuum_count"),
                analyze_count: r.get("analyze_count"),
                autoanalyze_count: r.get("autoanalyze_count"),
            },
        })
        .collect())
}

pub(super) async fn fetch_named_index_sizing(pool: &PgPool) -> Result<Vec<IndexSizingEntry>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT n.nspname            AS schema_name,
               ci.relname            AS index_name,
               pg_catalog.pg_relation_size(ci.oid)::int8 AS index_size,
               ci.relpages::int8     AS relpages,
               ci.reltuples::float8  AS reltuples
          FROM pg_catalog.pg_class ci
          JOIN pg_catalog.pg_namespace n ON n.oid = ci.relnamespace
         WHERE ci.relkind = 'i'
           AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND n.nspname NOT LIKE 'pg_temp_%'
         ORDER BY n.nspname, ci.relname
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| IndexSizingEntry {
            index: QualifiedName::new(
                r.get::<String, _>("schema_name"),
                r.get::<String, _>("index_name"),
            ),
            sizing: IndexSizing {
                size: r.get("index_size"),
                relpages: r.get("relpages"),
                reltuples: r.get("reltuples"),
            },
        })
        .collect())
}

pub(super) async fn fetch_named_index_activity(pool: &PgPool) -> Result<Vec<IndexActivityEntry>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT n.nspname            AS schema_name,
               s.indexrelname        AS index_name,
               COALESCE(s.idx_scan, 0)::int8      AS idx_scan,
               COALESCE(s.idx_tup_read, 0)::int8  AS idx_tup_read,
               COALESCE(s.idx_tup_fetch, 0)::int8 AS idx_tup_fetch
          FROM pg_catalog.pg_stat_user_indexes s
          JOIN pg_catalog.pg_class ci ON ci.oid = s.indexrelid
          JOIN pg_catalog.pg_namespace n ON n.oid = ci.relnamespace
         WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND n.nspname NOT LIKE 'pg_temp_%'
         ORDER BY n.nspname, s.indexrelname
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| IndexActivityEntry {
            index: QualifiedName::new(
                r.get::<String, _>("schema_name"),
                r.get::<String, _>("index_name"),
            ),
            activity: IndexActivity {
                idx_scan: r.get("idx_scan"),
                idx_tup_read: r.get("idx_tup_read"),
                idx_tup_fetch: r.get("idx_tup_fetch"),
            },
        })
        .collect())
}
