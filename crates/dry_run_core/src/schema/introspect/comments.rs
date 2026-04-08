use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use crate::error::Result;

use super::raw_types::*;

pub(super) async fn fetch_table_comments(pool: &PgPool) -> Result<Vec<RawTableComment>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT d.objoid::int4 AS table_oid,
               d.description   AS comment
          FROM pg_catalog.pg_description d
          JOIN pg_catalog.pg_class c ON c.oid = d.objoid
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE d.objsubid = 0
           AND c.relkind IN ('r', 'p')
           AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND n.nspname NOT LIKE 'pg_temp_%'
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| RawTableComment {
            table_oid: r.get::<i32, _>("table_oid") as u32,
            comment: r.get("comment"),
        })
        .collect())
}

pub(super) async fn fetch_column_comments(pool: &PgPool) -> Result<Vec<RawColumnComment>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT d.objoid::int4 AS table_oid,
               a.attname       AS column_name,
               d.description   AS comment
          FROM pg_catalog.pg_description d
          JOIN pg_catalog.pg_class c ON c.oid = d.objoid
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          JOIN pg_catalog.pg_attribute a
            ON a.attrelid = d.objoid AND a.attnum = d.objsubid
         WHERE d.objsubid > 0
           AND c.relkind IN ('r', 'p')
           AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND n.nspname NOT LIKE 'pg_temp_%'
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| RawColumnComment {
            table_oid: r.get::<i32, _>("table_oid") as u32,
            column_name: r.get("column_name"),
            comment: r.get("comment"),
        })
        .collect())
}
