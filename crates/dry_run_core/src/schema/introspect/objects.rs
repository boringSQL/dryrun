use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use super::super::types::*;
use crate::error::Result;

pub(super) async fn fetch_views(pool: &PgPool) -> Result<Vec<View>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT n.nspname        AS schema_name,
               c.relname         AS view_name,
               c.relkind = 'm'   AS is_materialized,
               pg_catalog.pg_get_viewdef(c.oid, true) AS definition,
               d.description     AS comment
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          LEFT JOIN pg_catalog.pg_description d
            ON d.objoid = c.oid AND d.objsubid = 0
         WHERE c.relkind IN ('v', 'm')
           AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND n.nspname NOT LIKE 'pg_temp_%'
         ORDER BY n.nspname, c.relname
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| View {
            schema: r.get("schema_name"),
            name: r.get("view_name"),
            definition: r.get::<Option<String>, _>("definition").unwrap_or_default(),
            is_materialized: r.get("is_materialized"),
            comment: r.get("comment"),
        })
        .collect())
}

pub(super) async fn fetch_functions(pool: &PgPool) -> Result<Vec<Function>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT n.nspname        AS schema_name,
               p.proname         AS func_name,
               pg_catalog.pg_get_function_identity_arguments(p.oid) AS identity_args,
               pg_catalog.pg_get_function_result(p.oid) AS return_type,
               l.lanname         AS language,
               p.provolatile::text AS volatility,
               p.prosecdef       AS security_definer,
               d.description     AS comment
          FROM pg_catalog.pg_proc p
          JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
          JOIN pg_catalog.pg_language l ON l.oid = p.prolang
          LEFT JOIN pg_catalog.pg_description d
            ON d.objoid = p.oid AND d.objsubid = 0
         WHERE p.prokind IN ('f', 'p')
           AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND n.nspname NOT LIKE 'pg_temp_%'
         ORDER BY n.nspname, p.proname
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| {
            let vol_str: String = r.get("volatility");
            Function {
                schema: r.get("schema_name"),
                name: r.get("func_name"),
                identity_args: r.get("identity_args"),
                return_type: r
                    .get::<Option<String>, _>("return_type")
                    .unwrap_or_default(),
                language: r.get("language"),
                volatility: Volatility::from_pg_provolatile(&vol_str)
                    .unwrap_or(Volatility::Volatile),
                security_definer: r.get("security_definer"),
                comment: r.get("comment"),
            }
        })
        .collect())
}

pub(super) async fn fetch_extensions(pool: &PgPool) -> Result<Vec<Extension>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT e.extname   AS ext_name,
               e.extversion AS ext_version,
               n.nspname    AS schema_name
          FROM pg_catalog.pg_extension e
          JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
         ORDER BY e.extname
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| Extension {
            name: r.get("ext_name"),
            version: r.get("ext_version"),
            schema: r.get("schema_name"),
        })
        .collect())
}

pub(super) async fn fetch_gucs(pool: &PgPool) -> Result<Vec<GucSetting>> {
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
