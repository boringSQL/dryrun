use std::collections::HashMap;

use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use super::super::types::*;
use crate::error::Result;

pub(super) async fn fetch_enums(pool: &PgPool) -> Result<Vec<EnumType>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT n.nspname AS schema_name,
               t.typname  AS type_name,
               (SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder)
                  FROM pg_catalog.pg_enum e
                 WHERE e.enumtypid = t.oid
               ) AS labels
          FROM pg_catalog.pg_type t
          JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
         WHERE t.typtype = 'e'
           AND n.nspname NOT IN ('pg_catalog', 'information_schema')
         ORDER BY n.nspname, t.typname
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| EnumType {
            schema: r.get("schema_name"),
            name: r.get("type_name"),
            labels: r
                .get::<Option<Vec<String>>, _>("labels")
                .unwrap_or_default(),
        })
        .collect())
}

pub(super) async fn fetch_domains(pool: &PgPool) -> Result<Vec<DomainType>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT n.nspname AS schema_name,
               t.typname  AS type_name,
               pg_catalog.format_type(t.typbasetype, t.typtypmod) AS base_type,
               t.typnotnull AS notnull,
               pg_catalog.pg_get_expr(t.typdefaultbin, 0) AS default_expr,
               (SELECT array_agg(pg_catalog.pg_get_constraintdef(con.oid) ORDER BY con.conname)
                  FROM pg_catalog.pg_constraint con
                 WHERE con.contypid = t.oid
               ) AS check_constraints
          FROM pg_catalog.pg_type t
          JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
         WHERE t.typtype = 'd'
           AND n.nspname NOT IN ('pg_catalog', 'information_schema')
         ORDER BY n.nspname, t.typname
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| DomainType {
            schema: r.get("schema_name"),
            name: r.get("type_name"),
            base_type: r.get("base_type"),
            nullable: !r.get::<bool, _>("notnull"),
            default: r.get("default_expr"),
            check_constraints: r
                .get::<Option<Vec<String>>, _>("check_constraints")
                .unwrap_or_default(),
        })
        .collect())
}

pub(super) async fn fetch_composites(pool: &PgPool) -> Result<Vec<CompositeType>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT n.nspname   AS schema_name,
               t.typname    AS type_name,
               a.attname    AS field_name,
               pg_catalog.format_type(a.atttypid, a.atttypmod) AS field_type
          FROM pg_catalog.pg_type t
          JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
          JOIN pg_catalog.pg_class c ON c.oid = t.typrelid
          JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
         WHERE t.typtype = 'c'
           AND c.relkind = 'c'
           AND a.attnum > 0
           AND NOT a.attisdropped
           AND n.nspname NOT IN ('pg_catalog', 'information_schema')
         ORDER BY n.nspname, t.typname, a.attnum
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut map: HashMap<(String, String), Vec<CompositeField>> = HashMap::new();
    for r in &rows {
        let key = (
            r.get::<String, _>("schema_name"),
            r.get::<String, _>("type_name"),
        );
        map.entry(key).or_default().push(CompositeField {
            name: r.get("field_name"),
            type_name: r.get("field_type"),
        });
    }

    let mut composites: Vec<CompositeType> = map
        .into_iter()
        .map(|((schema, name), fields)| CompositeType {
            schema,
            name,
            fields,
        })
        .collect();
    composites.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    Ok(composites)
}
