use sqlx::PgPool;
use tracing::info;

use crate::error::{Error, Result};
use crate::schema::types::{
    ColumnStats, IndexStats, NodeStats, SchemaSnapshot, TableStats,
};

#[derive(Debug)]
pub struct ApplyResult {
    pub tables_updated: usize,
    pub indexes_updated: usize,
    pub columns_injected: usize,
    pub skipped: Vec<String>,
    pub regresql_loaded: bool,
}

/// Resolved stats ready for injection — flat lists with schema-qualified names.
#[derive(Debug)]
struct ResolvedStats {
    tables: Vec<(String, String, TableStats)>,
    indexes: Vec<(String, String, String, IndexStats)>,
    columns: Vec<(String, String, String, String, ColumnStats)>, // schema, table, column, type_name, stats
}

/// Column metadata from the target database.
struct ColumnMeta {
    attrelid: i64,
    attnum: i16,
    type_name: String,
    eq_opr: Option<i64>,
    lt_opr: Option<i64>,
}

/// Apply captured statistics from a SchemaSnapshot to a target PostgreSQL database.
///
/// When `node` is Some, uses that specific node's stats from node_stats.
/// When None, uses single node_stats entry or falls back to inline stats.
pub async fn apply_stats(
    pool: &PgPool,
    snapshot: &SchemaSnapshot,
    node: Option<&str>,
) -> Result<ApplyResult> {
    check_inject_privileges(pool).await?;
    let regresql_loaded = check_regresql(pool).await;
    let resolved = resolve_stats(snapshot, node)?;

    let mut result = ApplyResult {
        tables_updated: 0,
        indexes_updated: 0,
        columns_injected: 0,
        skipped: Vec::new(),
        regresql_loaded,
    };

    let mut tx = pool.begin().await.map_err(|e| {
        Error::StatsInjection(format!("failed to begin transaction: {e}"))
    })?;

    // phase 1: pg_class for tables
    for (schema, table, stats) in &resolved.tables {
        match update_pg_class(&mut tx, schema, table, "r", stats.reltuples, stats.relpages).await {
            Ok(true) => result.tables_updated += 1,
            Ok(false) => {
                result.skipped.push(format!("{schema}.{table}: not found on target"));
            }
            Err(e) => {
                result.skipped.push(format!("{schema}.{table}: {e}"));
            }
        }
    }

    // phase 2: pg_class for indexes
    for (schema, _table, index_name, stats) in &resolved.indexes {
        match update_pg_class(&mut tx, schema, index_name, "i", stats.reltuples, stats.relpages)
            .await
        {
            Ok(true) => result.indexes_updated += 1,
            Ok(false) => {
                result.skipped.push(format!("index {schema}.{index_name}: not found on target"));
            }
            Err(e) => {
                result.skipped.push(format!("index {schema}.{index_name}: {e}"));
            }
        }
    }

    // phase 3: pg_statistic for columns
    for (schema, table, column, type_name, stats) in &resolved.columns {
        let meta = match lookup_column_meta(&mut tx, schema, table, column).await {
            Ok(Some(m)) => m,
            Ok(None) => {
                result
                    .skipped
                    .push(format!("{schema}.{table}.{column}: column not found on target"));
                continue;
            }
            Err(e) => {
                result
                    .skipped
                    .push(format!("{schema}.{table}.{column}: {e}"));
                continue;
            }
        };

        // validate the type can be used for casting on target
        let resolved_type = match validate_type_name(&mut tx, type_name).await {
            Ok(Some(t)) => t,
            Ok(None) => {
                result.skipped.push(format!(
                    "{schema}.{table}.{column}: type '{type_name}' not recognized on target"
                ));
                continue;
            }
            Err(e) => {
                result
                    .skipped
                    .push(format!("{schema}.{table}.{column}: type validation failed: {e}"));
                continue;
            }
        };

        let meta = ColumnMeta {
            type_name: resolved_type,
            ..meta
        };

        match inject_column_stats(&mut tx, &meta, stats).await {
            Ok(true) => result.columns_injected += 1,
            Ok(false) => {
                result
                    .skipped
                    .push(format!("{schema}.{table}.{column}: no stats to inject"));
            }
            Err(e) => {
                result
                    .skipped
                    .push(format!("{schema}.{table}.{column}: {e}"));
            }
        }
    }

    tx.commit().await.map_err(|e| {
        Error::StatsInjection(format!("failed to commit: {e}"))
    })?;

    info!(
        tables = result.tables_updated,
        indexes = result.indexes_updated,
        columns = result.columns_injected,
        skipped = result.skipped.len(),
        "stats injection complete"
    );

    Ok(result)
}

/// Check whether pg_regresql extension is loaded. Returns true if loaded.
async fn check_regresql(pool: &PgPool) -> bool {
    // check if the extension is available at all
    let available: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_regresql')",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(false);

    if !available {
        // maybe it's loaded directly (not installed as extension but via LOAD)
        // check shared_preload_libraries
        let spl: String = sqlx::query_scalar("SHOW shared_preload_libraries")
            .fetch_one(pool)
            .await
            .unwrap_or_default();

        if spl.contains("pg_regresql") {
            return true;
        }

        return false;
    }

    // check if actually loaded — the extension registers a hook, which we can detect
    // by checking if it appears in shared_preload_libraries or was LOADed
    let spl: String = sqlx::query_scalar("SHOW shared_preload_libraries")
        .fetch_one(pool)
        .await
        .unwrap_or_default();

    if spl.contains("pg_regresql") {
        return true;
    }

    // also check if it's been CREATE EXTENSION'd (it might auto-load via session_preload_libraries)
    let created: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_regresql')",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(false);

    created
}

fn resolve_stats(snapshot: &SchemaSnapshot, node: Option<&str>) -> Result<ResolvedStats> {
    if let Some(node_name) = node {
        // explicit node selection
        let ns = snapshot
            .node_stats
            .iter()
            .find(|n| n.source == node_name)
            .ok_or_else(|| {
                let available: Vec<&str> = snapshot.node_stats.iter().map(|n| n.source.as_str()).collect();
                Error::StatsInjection(format!(
                    "node '{}' not found. Available: {}",
                    node_name,
                    if available.is_empty() {
                        "(none)".to_string()
                    } else {
                        available.join(", ")
                    }
                ))
            })?;
        return Ok(resolve_from_node_stats(ns, snapshot));
    }

    if snapshot.node_stats.len() == 1 {
        return Ok(resolve_from_node_stats(&snapshot.node_stats[0], snapshot));
    }

    if !snapshot.node_stats.is_empty() {
        let available: Vec<&str> = snapshot.node_stats.iter().map(|n| n.source.as_str()).collect();
        return Err(Error::StatsInjection(format!(
            "multiple node stats found ({}). Use --node to select one: {}",
            snapshot.node_stats.len(),
            available.join(", ")
        )));
    }

    // fallback: inline stats from tables/indexes/columns
    Ok(resolve_from_inline(snapshot))
}

fn resolve_from_node_stats(ns: &NodeStats, snapshot: &SchemaSnapshot) -> ResolvedStats {
    let tables: Vec<_> = ns
        .table_stats
        .iter()
        .map(|t| (t.schema.clone(), t.table.clone(), t.stats.clone()))
        .collect();

    let indexes: Vec<_> = ns
        .index_stats
        .iter()
        .map(|i| {
            (
                i.schema.clone(),
                i.table.clone(),
                i.index_name.clone(),
                i.stats.clone(),
            )
        })
        .collect();

    // for column stats from node_stats, we need type_name from the snapshot
    let columns: Vec<_> = ns
        .column_stats
        .iter()
        .filter_map(|cs| {
            let type_name = find_column_type(snapshot, &cs.schema, &cs.table, &cs.column)?;
            Some((
                cs.schema.clone(),
                cs.table.clone(),
                cs.column.clone(),
                type_name,
                cs.stats.clone(),
            ))
        })
        .collect();

    ResolvedStats {
        tables,
        indexes,
        columns,
    }
}

fn resolve_from_inline(snapshot: &SchemaSnapshot) -> ResolvedStats {
    let mut tables = Vec::new();
    let mut indexes = Vec::new();
    let mut columns = Vec::new();

    for table in &snapshot.tables {
        if let Some(ref ts) = table.stats {
            tables.push((table.schema.clone(), table.name.clone(), ts.clone()));
        }
        for idx in &table.indexes {
            if let Some(ref is) = idx.stats {
                indexes.push((
                    table.schema.clone(),
                    table.name.clone(),
                    idx.name.clone(),
                    is.clone(),
                ));
            }
        }
        for col in &table.columns {
            if let Some(ref cs) = col.stats {
                columns.push((
                    table.schema.clone(),
                    table.name.clone(),
                    col.name.clone(),
                    col.type_name.clone(),
                    cs.clone(),
                ));
            }
        }
    }

    ResolvedStats {
        tables,
        indexes,
        columns,
    }
}

fn find_column_type(snapshot: &SchemaSnapshot, schema: &str, table: &str, column: &str) -> Option<String> {
    snapshot
        .tables
        .iter()
        .find(|t| t.schema == schema && t.name == table)?
        .columns
        .iter()
        .find(|c| c.name == column)
        .map(|c| c.type_name.clone())
}

async fn check_inject_privileges(pool: &PgPool) -> Result<()> {
    let has_privs: bool = sqlx::query_scalar(
        "SELECT has_table_privilege(current_user, 'pg_catalog.pg_statistic', 'INSERT') \
         AND has_table_privilege(current_user, 'pg_catalog.pg_class', 'UPDATE')",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| Error::StatsInjection(format!("privilege check failed: {e}")))?;

    if !has_privs {
        return Err(Error::Privilege(
            "need INSERT on pg_statistic and UPDATE on pg_class (requires superuser or table owner)"
                .to_string(),
        ));
    }

    Ok(())
}

async fn update_pg_class(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema: &str,
    name: &str,
    relkind: &str,
    reltuples: f64,
    relpages: i64,
) -> Result<bool> {
    let reltuples_f32 = reltuples as f32;
    let relpages_i32 = relpages as i32;

    let result = sqlx::query(
        "UPDATE pg_catalog.pg_class \
            SET reltuples = $1::real, relpages = $2::int \
          WHERE oid = ( \
            SELECT c.oid FROM pg_catalog.pg_class c \
              JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $3 AND c.relname = $4 \
               AND c.relkind = ANY(CASE WHEN $5 = 'i' THEN ARRAY['i'] ELSE ARRAY['r','p'] END) \
          )",
    )
    .bind(reltuples_f32)
    .bind(relpages_i32)
    .bind(schema)
    .bind(name)
    .bind(relkind)
    .execute(&mut **tx)
    .await?;

    Ok(result.rows_affected() > 0)
}

async fn lookup_column_meta(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema: &str,
    table: &str,
    column: &str,
) -> Result<Option<ColumnMeta>> {
    type ColumnMetaRow = (i64, i16, i64, Option<i64>, Option<i64>);
    let row: Option<ColumnMetaRow> = sqlx::query_as(
        "SELECT a.attrelid::bigint, \
                a.attnum::smallint, \
                a.atttypid::bigint, \
                (SELECT o.oid::bigint FROM pg_catalog.pg_operator o \
                  WHERE o.oprname = '=' AND o.oprleft = a.atttypid AND o.oprright = a.atttypid \
                  AND o.oprnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog') \
                  LIMIT 1), \
                (SELECT o.oid::bigint FROM pg_catalog.pg_operator o \
                  WHERE o.oprname = '<' AND o.oprleft = a.atttypid AND o.oprright = a.atttypid \
                  AND o.oprnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog') \
                  LIMIT 1) \
           FROM pg_catalog.pg_attribute a \
           JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
           JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
          WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3 \
            AND a.attnum > 0 AND NOT a.attisdropped",
    )
    .bind(schema)
    .bind(table)
    .bind(column)
    .fetch_optional(&mut **tx)
    .await?;

    Ok(row.map(|(attrelid, attnum, _atttypid, eq_opr, lt_opr)| ColumnMeta {
        attrelid,
        attnum,
        type_name: String::new(), // filled in by caller after validate_type_name
        eq_opr,
        lt_opr,
    }))
}

/// Validate a type name against the target database, returning normalized form.
async fn validate_type_name(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    type_name: &str,
) -> Result<Option<String>> {
    let result: Option<(String,)> = sqlx::query_as(&format!(
        "SELECT '{}'::regtype::text",
        type_name.replace('\'', "''")
    ))
    .fetch_optional(&mut **tx)
    .await
    .ok()
    .flatten();

    Ok(result.map(|(t,)| t))
}

/// Inject column statistics into pg_statistic. Returns true if a row was inserted.
async fn inject_column_stats(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    meta: &ColumnMeta,
    stats: &ColumnStats,
) -> Result<bool> {
    // need at least something to inject
    let has_anything = stats.null_frac.is_some()
        || stats.n_distinct.is_some()
        || stats.most_common_vals.is_some()
        || stats.histogram_bounds.is_some()
        || stats.correlation.is_some();

    if !has_anything {
        return Ok(false);
    }

    // delete existing row
    sqlx::query(
        "DELETE FROM pg_catalog.pg_statistic \
         WHERE starelid = $1::oid AND staattnum = $2::smallint AND stainherit = false",
    )
    .bind(meta.attrelid as i32)
    .bind(meta.attnum)
    .execute(&mut **tx)
    .await?;

    let null_frac = stats.null_frac.unwrap_or(0.0) as f32;
    let n_distinct = stats.n_distinct.unwrap_or(0.0) as f32;

    // build slot assignments
    let mut slot_kinds = [0i16; 5];
    let mut slot_ops = [0i64; 5];
    let mut slot_numbers: [Option<String>; 5] = [None, None, None, None, None];
    let mut slot_values: [Option<String>; 5] = [None, None, None, None, None];

    let mut slot_idx = 0;

    // MCV slot (stakind = 1)
    if let (Some(mcv_vals), Some(mcv_freqs)) =
        (&stats.most_common_vals, &stats.most_common_freqs)
        && let Some(eq_op) = meta.eq_opr {
            slot_kinds[slot_idx] = 1;
            slot_ops[slot_idx] = eq_op;
            slot_numbers[slot_idx] = Some(mcv_freqs.clone());
            slot_values[slot_idx] = Some(mcv_vals.clone());
            slot_idx += 1;
        }

    // Histogram slot (stakind = 2)
    if let Some(ref hist) = stats.histogram_bounds
        && let Some(lt_op) = meta.lt_opr {
            slot_kinds[slot_idx] = 2;
            slot_ops[slot_idx] = lt_op;
            slot_values[slot_idx] = Some(hist.clone());
            slot_idx += 1;
        }

    // Correlation slot (stakind = 3)
    if let Some(corr) = stats.correlation
        && let Some(lt_op) = meta.lt_opr {
            slot_kinds[slot_idx] = 3;
            slot_ops[slot_idx] = lt_op;
            slot_numbers[slot_idx] = Some(format!("{{{corr}}}"));
            // no stavalues for correlation
        }

    // Build dynamic INSERT — we need dynamic SQL because stavalues is anyarray
    // and we need to cast to the actual column type
    let type_name = &meta.type_name;

    // construct the values expressions for each slot
    let mut value_exprs = Vec::new();
    for i in 0..5 {
        let numbers_expr = match &slot_numbers[i] {
            Some(n) => format!("'{n}'::real[]"),
            None => "NULL".to_string(),
        };
        let values_expr = match &slot_values[i] {
            Some(v) => {
                // the values from pg_stats are already in PG array literal format
                let escaped = v.replace('\'', "''");
                format!("'{escaped}'::{type_name}[]")
            }
            None => "NULL".to_string(),
        };
        value_exprs.push((slot_kinds[i], slot_ops[i], numbers_expr, values_expr));
    }

    let sql = format!(
        "INSERT INTO pg_catalog.pg_statistic ( \
            starelid, staattnum, stainherit, stanullfrac, stawidth, stadistinct, \
            stakind1, staop1, stanumbers1, stavalues1, \
            stakind2, staop2, stanumbers2, stavalues2, \
            stakind3, staop3, stanumbers3, stavalues3, \
            stakind4, staop4, stanumbers4, stavalues4, \
            stakind5, staop5, stanumbers5, stavalues5 \
         ) VALUES ( \
            {relid}::oid, {attnum}::smallint, false, {null_frac}::real, 0::int, {n_distinct}::real, \
            {k1}::smallint, {o1}::oid, {n1}, {v1}, \
            {k2}::smallint, {o2}::oid, {n2}, {v2}, \
            {k3}::smallint, {o3}::oid, {n3}, {v3}, \
            {k4}::smallint, {o4}::oid, {n4}, {v4}, \
            {k5}::smallint, {o5}::oid, {n5}, {v5} \
         )",
        relid = meta.attrelid,
        attnum = meta.attnum,
        null_frac = null_frac,
        n_distinct = n_distinct,
        k1 = value_exprs[0].0,
        o1 = value_exprs[0].1,
        n1 = value_exprs[0].2,
        v1 = value_exprs[0].3,
        k2 = value_exprs[1].0,
        o2 = value_exprs[1].1,
        n2 = value_exprs[1].2,
        v2 = value_exprs[1].3,
        k3 = value_exprs[2].0,
        o3 = value_exprs[2].1,
        n3 = value_exprs[2].2,
        v3 = value_exprs[2].3,
        k4 = value_exprs[3].0,
        o4 = value_exprs[3].1,
        n4 = value_exprs[3].2,
        v4 = value_exprs[3].3,
        k5 = value_exprs[4].0,
        o5 = value_exprs[4].1,
        n5 = value_exprs[4].2,
        v5 = value_exprs[4].3,
    );

    sqlx::query(&sql).execute(&mut **tx).await.map_err(|e| {
        Error::StatsInjection(format!(
            "pg_statistic insert for attrelid={} attnum={}: {e}",
            meta.attrelid, meta.attnum
        ))
    })?;

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_inline_stats() {
        use crate::schema::types::*;
        use chrono::Utc;

        let snapshot = SchemaSnapshot {
            pg_version: "16.0".to_string(),
            database: "test".to_string(),
            timestamp: Utc::now(),
            content_hash: String::new(),
            source: None,
            tables: vec![Table {
                oid: 1,
                schema: "public".to_string(),
                name: "users".to_string(),
                columns: vec![Column {
                    name: "id".to_string(),
                    ordinal: 1,
                    type_name: "integer".to_string(),
                    nullable: false,
                    default: None,
                    identity: None,
                    generated: None,
                    comment: None,
                    statistics_target: None,
                    stats: Some(ColumnStats {
                        null_frac: Some(0.0),
                        n_distinct: Some(-1.0),
                        most_common_vals: None,
                        most_common_freqs: None,
                        histogram_bounds: Some("{1,100,200}".to_string()),
                        correlation: Some(1.0),
                    }),
                }],
                constraints: vec![],
                indexes: vec![],
                comment: None,
                stats: Some(TableStats {
                    reltuples: 1000.0,
                    relpages: 10,
                    dead_tuples: 0,
                    last_vacuum: None,
                    last_autovacuum: None,
                    last_analyze: None,
                    last_autoanalyze: None,
                    seq_scan: 50,
                    idx_scan: 100,
                    table_size: 81920,
                }),
                partition_info: None,
                policies: vec![],
                triggers: vec![],
                reloptions: vec![],
                rls_enabled: false,
            }],
            enums: vec![],
            domains: vec![],
            composites: vec![],
            views: vec![],
            functions: vec![],
            extensions: vec![],
            gucs: vec![],
            node_stats: vec![],
        };

        let resolved = resolve_stats(&snapshot, None).unwrap();
        assert_eq!(resolved.tables.len(), 1);
        assert_eq!(resolved.tables[0].0, "public");
        assert_eq!(resolved.tables[0].1, "users");
        assert_eq!(resolved.columns.len(), 1);
        assert_eq!(resolved.columns[0].3, "integer"); // type_name
    }

    #[test]
    fn test_resolve_node_stats_not_found() {
        use chrono::Utc;

        let snapshot = SchemaSnapshot {
            pg_version: "16.0".to_string(),
            database: "test".to_string(),
            timestamp: Utc::now(),
            content_hash: String::new(),
            source: None,
            tables: vec![],
            enums: vec![],
            domains: vec![],
            composites: vec![],
            views: vec![],
            functions: vec![],
            extensions: vec![],
            gucs: vec![],
            node_stats: vec![NodeStats {
                source: "prod-1".to_string(),
                timestamp: Utc::now(),
                is_standby: false,
                table_stats: vec![],
                index_stats: vec![],
                column_stats: vec![],
            }],
        };

        let err = resolve_stats(&snapshot, Some("prod-2")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("prod-2"));
        assert!(msg.contains("prod-1"));
    }

    #[test]
    fn test_resolve_multiple_nodes_requires_selection() {
        use chrono::Utc;

        let snapshot = SchemaSnapshot {
            pg_version: "16.0".to_string(),
            database: "test".to_string(),
            timestamp: Utc::now(),
            content_hash: String::new(),
            source: None,
            tables: vec![],
            enums: vec![],
            domains: vec![],
            composites: vec![],
            views: vec![],
            functions: vec![],
            extensions: vec![],
            gucs: vec![],
            node_stats: vec![
                NodeStats {
                    source: "prod-1".to_string(),
                    timestamp: Utc::now(),
                    is_standby: false,
                    table_stats: vec![],
                    index_stats: vec![],
                    column_stats: vec![],
                },
                NodeStats {
                    source: "prod-2".to_string(),
                    timestamp: Utc::now(),
                    is_standby: true,
                    table_stats: vec![],
                    index_stats: vec![],
                    column_stats: vec![],
                },
            ],
        };

        let err = resolve_stats(&snapshot, None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("multiple"));
        assert!(msg.contains("prod-1"));
        assert!(msg.contains("prod-2"));
    }
}
