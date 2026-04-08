use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use crate::error::Result;

use super::raw_types::*;

pub(super) async fn fetch_policies(pool: &PgPool) -> Result<Vec<RawPolicy>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT pol.polrelid::int4    AS table_oid,
               pol.polname            AS policy_name,
               CASE pol.polcmd
                   WHEN 'r' THEN 'SELECT'
                   WHEN 'a' THEN 'INSERT'
                   WHEN 'w' THEN 'UPDATE'
                   WHEN 'd' THEN 'DELETE'
                   WHEN '*' THEN 'ALL'
                   ELSE pol.polcmd::text
               END AS command,
               pol.polpermissive       AS permissive,
               (SELECT array_agg(r.rolname)
                  FROM unnest(pol.polroles) AS rid(oid)
                  JOIN pg_catalog.pg_roles r ON r.oid = rid.oid
               ) AS roles,
               pg_catalog.pg_get_expr(pol.polqual, pol.polrelid) AS using_expr,
               pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid) AS with_check_expr
          FROM pg_catalog.pg_policy pol
          JOIN pg_catalog.pg_class c ON c.oid = pol.polrelid
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
         ORDER BY pol.polrelid, pol.polname
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| RawPolicy {
            table_oid: r.get::<i32, _>("table_oid") as u32,
            name: r.get("policy_name"),
            command: r.get("command"),
            permissive: r.get("permissive"),
            roles: r.get::<Option<Vec<String>>, _>("roles").unwrap_or_default(),
            using_expr: r.get("using_expr"),
            with_check_expr: r.get("with_check_expr"),
        })
        .collect())
}

pub(super) async fn fetch_triggers(pool: &PgPool) -> Result<Vec<RawTrigger>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT t.tgrelid::int4                AS table_oid,
               t.tgname                         AS trigger_name,
               pg_catalog.pg_get_triggerdef(t.oid) AS definition
          FROM pg_catalog.pg_trigger t
          JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE NOT t.tgisinternal
           AND t.tgparentid = 0
           AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
         ORDER BY t.tgrelid, t.tgname
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| RawTrigger {
            table_oid: r.get::<i32, _>("table_oid") as u32,
            name: r.get("trigger_name"),
            definition: r.get("definition"),
        })
        .collect())
}
