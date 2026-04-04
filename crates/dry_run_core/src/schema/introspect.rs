use std::collections::HashMap;

use chrono::{DateTime, Utc};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use tracing::info;

use super::hash::{compute_content_hash, HashInput};
use super::types::*;
use crate::error::Result;

pub async fn introspect_schema(pool: &PgPool) -> Result<SchemaSnapshot> {
    let pg_version: String = sqlx::query_scalar("SELECT version()")
        .fetch_one(pool)
        .await?;

    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await?;

    // Group 1: table-centric data.
    let (
        raw_tables,
        raw_columns,
        raw_constraints,
        table_comments,
        column_comments,
        raw_indexes,
        raw_table_stats,
        raw_column_stats,
        raw_partitions,
        raw_partition_children,
        raw_policies,
        raw_triggers,
        raw_index_stats,
    ) = tokio::try_join!(
        fetch_tables(pool),
        fetch_columns(pool),
        fetch_constraints(pool),
        fetch_table_comments(pool),
        fetch_column_comments(pool),
        fetch_indexes(pool),
        fetch_table_stats(pool),
        fetch_column_stats(pool),
        fetch_partition_info(pool),
        fetch_partition_children(pool),
        fetch_policies(pool),
        fetch_triggers(pool),
        fetch_index_stats(pool),
    )?;

    // Group 2: top-level objects.
    let (enums, domains, composites, views, functions, extensions, gucs, is_standby) =
        tokio::try_join!(
            fetch_enums(pool),
            fetch_domains(pool),
            fetch_composites(pool),
            fetch_views(pool),
            fetch_functions(pool),
            fetch_extensions(pool),
            fetch_gucs(pool),
            fetch_is_standby(pool),
        )?;

    let with_vacuum = raw_table_stats.iter().filter(|s| s.last_autovacuum.is_some()).count();
    if with_vacuum == 0 && !raw_table_stats.is_empty() {
        if is_standby {
            info!("all vacuum timestamps are null;expected on standby");
        } else {
            tracing::warn!(
                "all vacuum/analyze timestamps are null on primary! \
                 check that the role has pg_read_all_stats privilege"
            );
        }
    }

    let tables = assemble_tables(
        raw_tables,
        raw_columns,
        raw_constraints,
        table_comments,
        column_comments,
        raw_indexes,
        raw_table_stats,
        raw_column_stats,
        raw_partitions,
        raw_partition_children,
        raw_policies,
        raw_triggers,
        raw_index_stats,
    );

    let content_hash = compute_content_hash(&HashInput {
        pg_version: &pg_version,
        tables: &tables,
        enums: &enums,
        domains: &domains,
        composites: &composites,
        views: &views,
        functions: &functions,
        extensions: &extensions,
    });

    let snapshot = SchemaSnapshot {
        pg_version,
        database,
        timestamp: Utc::now(),
        content_hash,
        source: None,
        tables,
        enums,
        domains,
        composites,
        views,
        functions,
        extensions,
        gucs,
        node_stats: vec![],
    };

    info!(
        tables = snapshot.tables.len(),
        enums = snapshot.enums.len(),
        domains = snapshot.domains.len(),
        composites = snapshot.composites.len(),
        views = snapshot.views.len(),
        functions = snapshot.functions.len(),
        extensions = snapshot.extensions.len(),
        hash = %snapshot.content_hash,
        "schema introspection complete"
    );

    Ok(snapshot)
}

pub async fn fetch_stats_only(pool: &PgPool, source: &str) -> Result<NodeStats> {
    let (raw_table_stats, raw_index_stats, raw_column_stats, is_standby) = tokio::try_join!(
        fetch_named_table_stats(pool),
        fetch_named_index_stats(pool),
        fetch_named_column_stats(pool),
        fetch_is_standby(pool),
    )?;

    Ok(NodeStats {
        source: source.to_string(),
        timestamp: Utc::now(),
        is_standby,
        table_stats: raw_table_stats,
        index_stats: raw_index_stats,
        column_stats: raw_column_stats,
    })
}

pub async fn fetch_is_standby(pool: &PgPool) -> Result<bool> {
    let row: PgRow = sqlx::query("SELECT pg_catalog.pg_is_in_recovery() AS is_standby")
        .fetch_one(pool)
        .await?;
    Ok(row.get("is_standby"))
}

async fn fetch_named_table_stats(pool: &PgPool) -> Result<Vec<NodeTableStats>> {
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

async fn fetch_named_index_stats(pool: &PgPool) -> Result<Vec<NodeIndexStats>> {
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

async fn fetch_named_column_stats(pool: &PgPool) -> Result<Vec<NodeColumnStats>> {
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

// ---------------------------------------------------------------------------
// Raw row structs for intermediate grouping
// ---------------------------------------------------------------------------

struct RawTable {
    oid: u32,
    schema: String,
    name: String,
    rls_enabled: bool,
    reloptions: Vec<String>,
}

struct RawColumn {
    table_oid: u32,
    name: String,
    ordinal: i16,
    type_name: String,
    nullable: bool,
    default: Option<String>,
    identity: Option<String>,
}

struct RawConstraint {
    table_oid: u32,
    name: String,
    contype: String,
    columns: Vec<String>,
    definition: Option<String>,
    fk_table: Option<String>,
    fk_columns: Vec<String>,
    comment: Option<String>,
}

struct RawTableComment {
    table_oid: u32,
    comment: String,
}

struct RawColumnComment {
    table_oid: u32,
    column_name: String,
    comment: String,
}

struct RawIndex {
    table_oid: u32,
    name: String,
    columns: Vec<String>,
    include_columns: Vec<String>,
    index_type: String,
    is_unique: bool,
    is_primary: bool,
    predicate: Option<String>,
    definition: String,
    is_valid: bool,
    backs_constraint: bool,
}

struct RawTableStats {
    table_oid: u32,
    reltuples: f64,
    relpages: i64,
    dead_tuples: i64,
    last_vacuum: Option<DateTime<Utc>>,
    last_autovacuum: Option<DateTime<Utc>>,
    last_analyze: Option<DateTime<Utc>>,
    last_autoanalyze: Option<DateTime<Utc>>,
    seq_scan: i64,
    idx_scan: i64,
    table_size: i64,
}

struct RawColumnStats {
    table_oid: u32,
    column_name: String,
    null_frac: Option<f64>,
    n_distinct: Option<f64>,
    most_common_vals: Option<String>,
    most_common_freqs: Option<String>,
    histogram_bounds: Option<String>,
    correlation: Option<f64>,
}

struct RawPartitionInfo {
    table_oid: u32,
    strategy: String,
    key: String,
}

struct RawPartitionChild {
    parent_oid: u32,
    schema: String,
    name: String,
    bound: String,
}

struct RawPolicy {
    table_oid: u32,
    name: String,
    command: String,
    permissive: bool,
    roles: Vec<String>,
    using_expr: Option<String>,
    with_check_expr: Option<String>,
}

struct RawTrigger {
    table_oid: u32,
    name: String,
    definition: String,
}

struct RawIndexStats {
    table_oid: u32,
    index_name: String,
    idx_scan: i64,
    idx_tup_read: i64,
    idx_tup_fetch: i64,
    size: i64,
    relpages: i64,
    reltuples: f64,
}

// ---------------------------------------------------------------------------
// Query 1: Tables (relkind IN ('r','p') — regular and partitioned)
// ---------------------------------------------------------------------------

async fn fetch_tables(pool: &PgPool) -> Result<Vec<RawTable>> {
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

// ---------------------------------------------------------------------------
// Query 2: Columns
// ---------------------------------------------------------------------------

async fn fetch_columns(pool: &PgPool) -> Result<Vec<RawColumn>> {
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
               END AS identity
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
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Query 3: Constraints
// ---------------------------------------------------------------------------

async fn fetch_constraints(pool: &PgPool) -> Result<Vec<RawConstraint>> {
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
               d.description AS comment
          FROM pg_catalog.pg_constraint con
          JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
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
            comment: r.get("comment"),
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Query 4: Table comments (objsubid = 0)
// ---------------------------------------------------------------------------

async fn fetch_table_comments(pool: &PgPool) -> Result<Vec<RawTableComment>> {
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

// ---------------------------------------------------------------------------
// Query 5: Column comments (objsubid > 0)
// ---------------------------------------------------------------------------

async fn fetch_column_comments(pool: &PgPool) -> Result<Vec<RawColumnComment>> {
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

// ---------------------------------------------------------------------------
// Query 6: Enum types
// ---------------------------------------------------------------------------

async fn fetch_enums(pool: &PgPool) -> Result<Vec<EnumType>> {
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

// ---------------------------------------------------------------------------
// Query 7: Domain types
// ---------------------------------------------------------------------------

async fn fetch_domains(pool: &PgPool) -> Result<Vec<DomainType>> {
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

// ---------------------------------------------------------------------------
// Query 8: Composite types
// ---------------------------------------------------------------------------

async fn fetch_composites(pool: &PgPool) -> Result<Vec<CompositeType>> {
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

// ---------------------------------------------------------------------------
// Query 9: Indexes
// ---------------------------------------------------------------------------

async fn fetch_indexes(pool: &PgPool) -> Result<Vec<RawIndex>> {
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

// ---------------------------------------------------------------------------
// Query 10: Table statistics
// ---------------------------------------------------------------------------

async fn fetch_table_stats(pool: &PgPool) -> Result<Vec<RawTableStats>> {
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

    info!(total = stats.len(), "table stats fetched");

    Ok(stats)
}

// ---------------------------------------------------------------------------
// Query 11: Column statistics
// ---------------------------------------------------------------------------

async fn fetch_column_stats(pool: &PgPool) -> Result<Vec<RawColumnStats>> {
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

// ---------------------------------------------------------------------------
// Query 12: Partition info
// ---------------------------------------------------------------------------

async fn fetch_partition_info(pool: &PgPool) -> Result<Vec<RawPartitionInfo>> {
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

// ---------------------------------------------------------------------------
// Query 13: Partition children
// ---------------------------------------------------------------------------

async fn fetch_partition_children(pool: &PgPool) -> Result<Vec<RawPartitionChild>> {
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

// ---------------------------------------------------------------------------
// Query 14: RLS policies
// ---------------------------------------------------------------------------

async fn fetch_policies(pool: &PgPool) -> Result<Vec<RawPolicy>> {
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

// ---------------------------------------------------------------------------
// Query 15: Triggers (non-internal only)
// ---------------------------------------------------------------------------

async fn fetch_triggers(pool: &PgPool) -> Result<Vec<RawTrigger>> {
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

// ---------------------------------------------------------------------------
// Query 16: Index statistics from pg_stat_user_indexes
// ---------------------------------------------------------------------------

async fn fetch_index_stats(pool: &PgPool) -> Result<Vec<RawIndexStats>> {
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

// ---------------------------------------------------------------------------
// Query 17: Views and materialized views
// ---------------------------------------------------------------------------

async fn fetch_views(pool: &PgPool) -> Result<Vec<View>> {
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

// ---------------------------------------------------------------------------
// Query 17: Functions and procedures
// ---------------------------------------------------------------------------

async fn fetch_functions(pool: &PgPool) -> Result<Vec<Function>> {
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

// ---------------------------------------------------------------------------
// Query 18: Extensions
// ---------------------------------------------------------------------------

async fn fetch_extensions(pool: &PgPool) -> Result<Vec<Extension>> {
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

// ---------------------------------------------------------------------------
// Query 19: Relevant GUC settings
// ---------------------------------------------------------------------------

async fn fetch_gucs(pool: &PgPool) -> Result<Vec<GucSetting>> {
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

// ---------------------------------------------------------------------------
// Assembly: merge parts into Table structs
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn assemble_tables(
    raw_tables: Vec<RawTable>,
    raw_columns: Vec<RawColumn>,
    raw_constraints: Vec<RawConstraint>,
    table_comments: Vec<RawTableComment>,
    column_comments: Vec<RawColumnComment>,
    raw_indexes: Vec<RawIndex>,
    raw_table_stats: Vec<RawTableStats>,
    raw_column_stats: Vec<RawColumnStats>,
    raw_partitions: Vec<RawPartitionInfo>,
    raw_partition_children: Vec<RawPartitionChild>,
    raw_policies: Vec<RawPolicy>,
    raw_triggers: Vec<RawTrigger>,
    raw_index_stats: Vec<RawIndexStats>,
) -> Vec<Table> {
    // --- Columns ---
    let mut columns_by_oid: HashMap<u32, Vec<Column>> = HashMap::new();
    for rc in raw_columns {
        columns_by_oid
            .entry(rc.table_oid)
            .or_default()
            .push(Column {
                name: rc.name,
                ordinal: rc.ordinal,
                type_name: rc.type_name,
                nullable: rc.nullable,
                default: rc.default,
                identity: rc.identity,
                comment: None,
                stats: None,
            });
    }

    // --- Constraints ---
    let mut constraints_by_oid: HashMap<u32, Vec<Constraint>> = HashMap::new();
    for rc in raw_constraints {
        let kind = match ConstraintKind::from_pg_contype(&rc.contype) {
            Some(k) => k,
            None => continue,
        };
        constraints_by_oid
            .entry(rc.table_oid)
            .or_default()
            .push(Constraint {
                name: rc.name,
                kind,
                columns: rc.columns,
                definition: rc.definition,
                fk_table: rc.fk_table,
                fk_columns: rc.fk_columns,
                comment: rc.comment,
            });
    }

    // --- Table comments ---
    let table_comment_map: HashMap<u32, String> = table_comments
        .into_iter()
        .map(|tc| (tc.table_oid, tc.comment))
        .collect();

    // --- Column comments ---
    let col_comment_map: HashMap<(u32, String), String> = column_comments
        .into_iter()
        .map(|cc| ((cc.table_oid, cc.column_name), cc.comment))
        .collect();

    for (oid, cols) in &mut columns_by_oid {
        for col in cols.iter_mut() {
            if let Some(comment) = col_comment_map.get(&(*oid, col.name.clone())) {
                col.comment = Some(comment.clone());
            }
        }
    }

    // --- Column stats ---
    let mut col_stats_map: HashMap<(u32, String), ColumnStats> = HashMap::new();
    for cs in raw_column_stats {
        col_stats_map.insert(
            (cs.table_oid, cs.column_name),
            ColumnStats {
                null_frac: cs.null_frac,
                n_distinct: cs.n_distinct,
                most_common_vals: cs.most_common_vals,
                most_common_freqs: cs.most_common_freqs,
                histogram_bounds: cs.histogram_bounds,
                correlation: cs.correlation,
            },
        );
    }

    for (oid, cols) in &mut columns_by_oid {
        for col in cols.iter_mut() {
            if let Some(stats) = col_stats_map.remove(&(*oid, col.name.clone())) {
                col.stats = Some(stats);
            }
        }
    }

    // --- Index stats ---
    let mut idx_stats_map: HashMap<(u32, String), IndexStats> = HashMap::new();
    for ris in raw_index_stats {
        idx_stats_map.insert(
            (ris.table_oid, ris.index_name),
            IndexStats {
                idx_scan: ris.idx_scan,
                idx_tup_read: ris.idx_tup_read,
                idx_tup_fetch: ris.idx_tup_fetch,
                size: ris.size,
                relpages: ris.relpages,
                reltuples: ris.reltuples,
            },
        );
    }

    // --- Indexes ---
    let mut indexes_by_oid: HashMap<u32, Vec<Index>> = HashMap::new();
    for ri in raw_indexes {
        let stats = idx_stats_map.remove(&(ri.table_oid, ri.name.clone()));
        indexes_by_oid.entry(ri.table_oid).or_default().push(Index {
            name: ri.name,
            columns: ri.columns,
            include_columns: ri.include_columns,
            index_type: ri.index_type,
            is_unique: ri.is_unique,
            is_primary: ri.is_primary,
            predicate: ri.predicate,
            definition: ri.definition,
            is_valid: ri.is_valid,
            backs_constraint: ri.backs_constraint,
            stats,
        });
    }

    // --- Table stats ---
    let stats_by_oid: HashMap<u32, TableStats> = raw_table_stats
        .into_iter()
        .map(|s| {
            (
                s.table_oid,
                TableStats {
                    reltuples: s.reltuples,
                    relpages: s.relpages,
                    dead_tuples: s.dead_tuples,
                    last_vacuum: s.last_vacuum,
                    last_autovacuum: s.last_autovacuum,
                    last_analyze: s.last_analyze,
                    last_autoanalyze: s.last_autoanalyze,
                    seq_scan: s.seq_scan,
                    idx_scan: s.idx_scan,
                    table_size: s.table_size,
                },
            )
        })
        .collect();

    // --- Partition info ---
    let mut children_by_parent: HashMap<u32, Vec<PartitionChild>> = HashMap::new();
    for pc in raw_partition_children {
        children_by_parent
            .entry(pc.parent_oid)
            .or_default()
            .push(PartitionChild {
                schema: pc.schema,
                name: pc.name,
                bound: pc.bound,
            });
    }

    let partition_info_by_oid: HashMap<u32, PartitionInfo> = raw_partitions
        .into_iter()
        .filter_map(|rp| {
            let strategy = PartitionStrategy::from_pg_partstrat(&rp.strategy)?;
            Some((
                rp.table_oid,
                PartitionInfo {
                    strategy,
                    key: rp.key,
                    children: children_by_parent.remove(&rp.table_oid).unwrap_or_default(),
                },
            ))
        })
        .collect();

    // --- Policies ---
    let mut policies_by_oid: HashMap<u32, Vec<RlsPolicy>> = HashMap::new();
    for rp in raw_policies {
        policies_by_oid
            .entry(rp.table_oid)
            .or_default()
            .push(RlsPolicy {
                name: rp.name,
                command: rp.command,
                permissive: rp.permissive,
                roles: rp.roles,
                using_expr: rp.using_expr,
                with_check_expr: rp.with_check_expr,
            });
    }

    // --- Triggers ---
    let mut triggers_by_oid: HashMap<u32, Vec<Trigger>> = HashMap::new();
    for rt in raw_triggers {
        triggers_by_oid
            .entry(rt.table_oid)
            .or_default()
            .push(Trigger {
                name: rt.name,
                definition: rt.definition,
            });
    }

    // --- Assemble ---
    raw_tables
        .into_iter()
        .map(|rt| Table {
            oid: rt.oid,
            schema: rt.schema,
            name: rt.name,
            columns: columns_by_oid.remove(&rt.oid).unwrap_or_default(),
            constraints: constraints_by_oid.remove(&rt.oid).unwrap_or_default(),
            indexes: indexes_by_oid.remove(&rt.oid).unwrap_or_default(),
            comment: table_comment_map.get(&rt.oid).cloned(),
            stats: stats_by_oid.get(&rt.oid).cloned(),
            partition_info: partition_info_by_oid.get(&rt.oid).cloned(),
            policies: policies_by_oid.remove(&rt.oid).unwrap_or_default(),
            triggers: triggers_by_oid.remove(&rt.oid).unwrap_or_default(),
            reloptions: rt.reloptions,
            rls_enabled: rt.rls_enabled,
        })
        .collect()
}
