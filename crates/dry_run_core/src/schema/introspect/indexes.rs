use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use crate::error::Result;

use super::raw_types::*;

pub(super) async fn fetch_indexes(pool: &PgPool) -> Result<Vec<RawIndex>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT i.indrelid::int4      AS table_oid,
               ci.relname             AS index_name,
               am.amname              AS index_type,
               i.indisunique          AS is_unique,
               i.indisprimary         AS is_primary,
               pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS predicate,
               pg_catalog.pg_get_indexdef(i.indexrelid) AS definition,
               i.indisvalid           AS is_valid,
               i.indnkeyatts          AS n_key_atts,
               -- check when index backs a UNIQUE/PK/EXCLUSION constraint
               EXISTS (
                   SELECT 1 FROM pg_catalog.pg_constraint con
                    WHERE con.conindid = i.indexrelid
               ) AS backs_constraint,
               -- All column names (key + include)
               (SELECT array_agg(a.attname ORDER BY ord.n)
                  FROM unnest(i.indkey) WITH ORDINALITY AS ord(attnum, n)
                  JOIN pg_catalog.pg_attribute a
                    ON a.attrelid = i.indrelid AND a.attnum = ord.attnum
                 WHERE ord.attnum > 0
               ) AS all_col_names,
               array_length(i.indkey, 1) AS total_cols
          FROM pg_catalog.pg_index i
          JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
          JOIN pg_catalog.pg_class ct ON ct.oid = i.indrelid
          JOIN pg_catalog.pg_namespace n ON n.oid = ct.relnamespace
          JOIN pg_catalog.pg_am am ON am.oid = ci.relam
         WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND n.nspname NOT LIKE 'pg_temp_%'
           AND NOT EXISTS (SELECT 1 FROM pg_inherits inh WHERE inh.inhrelid = i.indexrelid)
         ORDER BY i.indrelid, ci.relname
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| {
            let all_cols: Vec<String> = r
                .get::<Option<Vec<String>>, _>("all_col_names")
                .unwrap_or_default();
            let n_key_atts = r.get::<i16, _>("n_key_atts") as usize;
            let (key_cols, include_cols) = if n_key_atts > 0 && n_key_atts <= all_cols.len() {
                (
                    all_cols[..n_key_atts].to_vec(),
                    all_cols[n_key_atts..].to_vec(),
                )
            } else {
                (all_cols, vec![])
            };

            RawIndex {
                table_oid: r.get::<i32, _>("table_oid") as u32,
                name: r.get("index_name"),
                columns: key_cols,
                include_columns: include_cols,
                index_type: r.get("index_type"),
                is_unique: r.get("is_unique"),
                is_primary: r.get("is_primary"),
                predicate: r.get("predicate"),
                definition: r.get("definition"),
                is_valid: r.get("is_valid"),
                backs_constraint: r.get("backs_constraint"),
            }
        })
        .collect())
}

pub(super) async fn fetch_index_stats(pool: &PgPool) -> Result<Vec<RawIndexStats>> {
    let rows: Vec<PgRow> = sqlx::query(
        r#"
        SELECT s.relid::int4        AS table_oid,
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
         ORDER BY s.relid, s.indexrelname
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| RawIndexStats {
            table_oid: r.get::<i32, _>("table_oid") as u32,
            index_name: r.get("index_name"),
            idx_scan: r.get("idx_scan"),
            idx_tup_read: r.get("idx_tup_read"),
            idx_tup_fetch: r.get("idx_tup_fetch"),
            size: r.get("index_size"),
            relpages: r.get("index_relpages"),
            reltuples: r.get("index_reltuples"),
        })
        .collect())
}
