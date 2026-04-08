use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use crate::error::Result;

use super::raw_types::*;

pub(super) async fn fetch_tables(pool: &PgPool) -> Result<Vec<RawTable>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT c.oid::int4      AS oid,
               n.nspname         AS schema_name,
               c.relname         AS table_name,
               c.relrowsecurity  AS rls_enabled,
               COALESCE(c.reloptions, '{}')  AS reloptions
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind IN ('r', 'p')
           AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND n.nspname NOT LIKE 'pg_temp_%'
         ORDER BY n.nspname, c.relname
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| RawTable {
            oid: r.get::<i32, _>("oid") as u32,
            schema: r.get("schema_name"),
            name: r.get("table_name"),
            rls_enabled: r.get("rls_enabled"),
            reloptions: r.get::<Vec<String>, _>("reloptions"),
        })
        .collect())
}

pub(super) async fn fetch_columns(pool: &PgPool) -> Result<Vec<RawColumn>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT a.attrelid::int4   AS table_oid,
               a.attname           AS column_name,
               a.attnum            AS ordinal,
               pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_name,
               NOT a.attnotnull    AS nullable,
               pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS default_expr,
               CASE a.attidentity
                   WHEN 'a' THEN 'always'
                   WHEN 'd' THEN 'by_default'
                   ELSE NULL
               END AS identity,
               NULLIF(a.attstattarget, -1)::int2 AS statistics_target,
               CASE a.attgenerated
                   WHEN 's' THEN 'stored'
                   ELSE NULL
               END AS generated
          FROM pg_catalog.pg_attribute a
          JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
         WHERE a.attnum > 0
           AND NOT a.attisdropped
           AND c.relkind IN ('r', 'p')
           AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND n.nspname NOT LIKE 'pg_temp_%'
         ORDER BY a.attrelid, a.attnum
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| RawColumn {
            table_oid: r.get::<i32, _>("table_oid") as u32,
            name: r.get("column_name"),
            ordinal: r.get("ordinal"),
            type_name: r.get("type_name"),
            nullable: r.get("nullable"),
            default: r.get("default_expr"),
            identity: r.get("identity"),
            generated: r.get("generated"),
            statistics_target: r.get("statistics_target"),
        })
        .collect())
}

pub(super) async fn fetch_constraints(pool: &PgPool) -> Result<Vec<RawConstraint>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT con.conrelid::int4     AS table_oid,
               con.conname             AS constraint_name,
               con.contype::text       AS contype,
               pg_catalog.pg_get_constraintdef(con.oid) AS definition,
               (SELECT array_agg(a.attname ORDER BY ord.n)
                  FROM unnest(con.conkey) WITH ORDINALITY AS ord(attnum, n)
                  JOIN pg_catalog.pg_attribute a
                    ON a.attrelid = con.conrelid AND a.attnum = ord.attnum
               ) AS col_names,
               CASE WHEN con.contype = 'f' THEN
                   (SELECT n2.nspname || '.' || c2.relname
                      FROM pg_catalog.pg_class c2
                      JOIN pg_catalog.pg_namespace n2 ON n2.oid = c2.relnamespace
                     WHERE c2.oid = con.confrelid)
               END AS fk_table,
               CASE WHEN con.contype = 'f' THEN
                   (SELECT array_agg(a.attname ORDER BY ord.n)
                      FROM unnest(con.confkey) WITH ORDINALITY AS ord(attnum, n)
                      JOIN pg_catalog.pg_attribute a
                        ON a.attrelid = con.confrelid AND a.attnum = ord.attnum
                   )
               END AS fk_col_names,
               ci.relname::text AS backing_index,
               d.description AS comment
          FROM pg_catalog.pg_constraint con
          JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          LEFT JOIN pg_catalog.pg_class ci
            ON ci.oid = con.conindid
          LEFT JOIN pg_catalog.pg_description d
            ON d.objoid = con.oid AND d.objsubid = 0
         WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND n.nspname NOT LIKE 'pg_temp_%'
           AND con.conislocal
         ORDER BY con.conrelid, con.conname
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| RawConstraint {
            table_oid: r.get::<i32, _>("table_oid") as u32,
            name: r.get("constraint_name"),
            contype: r.get("contype"),
            columns: r
                .get::<Option<Vec<String>>, _>("col_names")
                .unwrap_or_default(),
            definition: r.get("definition"),
            fk_table: r.get("fk_table"),
            fk_columns: r
                .get::<Option<Vec<String>>, _>("fk_col_names")
                .unwrap_or_default(),
            backing_index: r.get("backing_index"),
            comment: r.get("comment"),
        })
        .collect())
}

pub(super) async fn fetch_table_stats(pool: &PgPool) -> Result<Vec<RawTableStats>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT c.oid::int4            AS table_oid,
               c.reltuples::float8     AS reltuples,
               c.relpages::int8        AS relpages,
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
          LEFT JOIN pg_catalog.pg_stat_user_tables s
            ON s.relid = c.oid
         WHERE c.relkind IN ('r', 'p')
           AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND n.nspname NOT LIKE 'pg_temp_%'
        "#,
    )
    .fetch_all(pool)
    .await?;

    let stats: Vec<RawTableStats> = rows
        .iter()
        .map(|r| RawTableStats {
            table_oid: r.get::<i32, _>("table_oid") as u32,
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
        })
        .collect();

    tracing::info!(total = stats.len(), "table stats fetched");

    Ok(stats)
}

pub(super) async fn fetch_column_stats(pool: &PgPool) -> Result<Vec<RawColumnStats>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT c.oid::int4                    AS table_oid,
               s.attname                       AS column_name,
               s.null_frac::float8             AS null_frac,
               s.n_distinct::float8            AS n_distinct,
               s.most_common_vals::text        AS most_common_vals,
               s.most_common_freqs::text       AS most_common_freqs,
               s.histogram_bounds::text        AS histogram_bounds,
               s.correlation::float8           AS correlation
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          JOIN pg_catalog.pg_stats s
            ON s.schemaname = n.nspname AND s.tablename = c.relname
         WHERE c.relkind IN ('r', 'p')
           AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| RawColumnStats {
            table_oid: r.get::<i32, _>("table_oid") as u32,
            column_name: r.get("column_name"),
            null_frac: r.get::<Option<f64>, _>("null_frac"),
            n_distinct: r.get::<Option<f64>, _>("n_distinct"),
            most_common_vals: r.get("most_common_vals"),
            most_common_freqs: r.get("most_common_freqs"),
            histogram_bounds: r.get("histogram_bounds"),
            correlation: r.get::<Option<f64>, _>("correlation"),
        })
        .collect())
}
