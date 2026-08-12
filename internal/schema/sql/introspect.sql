-- name: fetch-tables
SELECT c.oid::int4      AS oid,
       n.nspname         AS schema_name,
       c.relname         AS table_name,
       c.relrowsecurity  AS rls_enabled,
       c.reloptions      AS reloptions
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relkind IN ('r', 'p')
   AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
   AND n.nspname NOT LIKE 'pg_temp_%'
 ORDER BY n.nspname, c.relname

-- name: fetch-columns
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

-- name: fetch-constraints
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
   -- keep local constraints plus a partition's own copy of an inherited
   -- constraint (parent on a different conrelid); skip same-relation clnes
   AND (con.conislocal
        OR (con.conparentid <> 0
            AND con.conrelid <> (SELECT p.conrelid FROM pg_catalog.pg_constraint p
                                  WHERE p.oid = con.conparentid)))
 ORDER BY con.conrelid, con.conname

-- name: fetch-table-comments
SELECT d.objoid::int4 AS table_oid,
       d.description   AS comment
  FROM pg_catalog.pg_description d
  JOIN pg_catalog.pg_class c ON c.oid = d.objoid
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
 WHERE d.objsubid = 0
   AND c.relkind IN ('r', 'p')
   AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
   AND n.nspname NOT LIKE 'pg_temp_%'

-- name: fetch-column-comments
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

-- name: fetch-enums
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

-- name: fetch-domains
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

-- name: fetch-composites
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

-- name: fetch-indexes
SELECT i.indrelid::int4      AS table_oid,
       i.indexrelid::int4    AS index_oid,
       ci.relname             AS index_name,
       am.amname              AS index_type,
       i.indisunique          AS is_unique,
       i.indisprimary         AS is_primary,
       pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS predicate,
       pg_catalog.pg_get_indexdef(i.indexrelid) AS definition,
       i.indnkeyatts          AS n_key_atts,
       i.indisvalid           AS is_valid,
       i.indisready           AS is_ready,
       -- check when index backs a UNIQUE/PK/EXCLUSION constraint
       EXISTS (
           SELECT 1 FROM pg_catalog.pg_constraint con
            WHERE con.conindid = i.indexrelid
       ) AS backs_constraint,
       (i.indexprs IS NOT NULL) AS has_expressions,
       -- All entries (key + include) in indkey order; expressions become deparsed text
       (SELECT array_agg(
                 COALESCE(a.attname,
                          btrim(regexp_replace(
                              pg_catalog.pg_get_indexdef(i.indexrelid, ord.n::int, true),
                              '\s+', ' ', 'g')),
                          '')
                 ORDER BY ord.n)
          FROM unnest(i.indkey) WITH ORDINALITY AS ord(attnum, n)
          LEFT JOIN pg_catalog.pg_attribute a
            ON a.attrelid = i.indrelid AND a.attnum = ord.attnum AND ord.attnum > 0
       ) AS all_col_names
  FROM pg_catalog.pg_index i
  JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
  JOIN pg_catalog.pg_class ct ON ct.oid = i.indrelid
  JOIN pg_catalog.pg_namespace n ON n.oid = ct.relnamespace
  JOIN pg_catalog.pg_am am ON am.oid = ci.relam
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
   AND n.nspname NOT LIKE 'pg_temp_%'
 ORDER BY i.indrelid, ci.relname

-- name: fetch-partition-info
SELECT pt.partrelid::int4       AS table_oid,
       pt.partstrat::text        AS strategy,
       pg_catalog.pg_get_partkeydef(pt.partrelid) AS part_key
  FROM pg_catalog.pg_partitioned_table pt
  JOIN pg_catalog.pg_class c ON c.oid = pt.partrelid
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')

-- name: fetch-partition-children
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

-- name: fetch-partition-index-children
SELECT inh.inhparent::int4     AS parent_index_oid,
       n.nspname                AS schema_name,
       ct.relname                AS table_name,
       ci.relname                AS index_name
  FROM pg_catalog.pg_inherits inh
  JOIN pg_catalog.pg_class ci ON ci.oid = inh.inhrelid
  JOIN pg_catalog.pg_index pgi ON pgi.indexrelid = ci.oid
  JOIN pg_catalog.pg_class ct ON ct.oid = pgi.indrelid
  JOIN pg_catalog.pg_namespace n ON n.oid = ct.relnamespace
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
   AND n.nspname NOT LIKE 'pg_temp_%'
 ORDER BY inh.inhparent, ci.relname

-- name: fetch-policies
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

-- name: fetch-triggers
SELECT t.tgrelid::int4                AS table_oid,
       t.tgname                         AS trigger_name,
       pg_catalog.pg_get_triggerdef(t.oid) AS definition
  FROM pg_catalog.pg_trigger t
  JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
 WHERE NOT t.tgisinternal
   AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
   AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d WHERE d.objid = t.oid AND d.deptype = 'i')
 ORDER BY t.tgrelid, t.tgname

-- name: fetch-views
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

-- name: fetch-functions
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

-- name: fetch-extensions
SELECT e.extname   AS ext_name,
       e.extversion AS ext_version,
       n.nspname    AS schema_name
  FROM pg_catalog.pg_extension e
  JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
 ORDER BY e.extname

-- name: fetch-gucs
SELECT name, setting, unit
  FROM pg_catalog.pg_settings
 WHERE name IN (
       'work_mem', 'effective_cache_size', 'random_page_cost',
       'seq_page_cost', 'effective_io_concurrency', 'shared_buffers',
       'maintenance_work_mem', 'default_statistics_target',
       'cpu_tuple_cost', 'cpu_index_tuple_cost', 'cpu_operator_cost',
       'hash_mem_multiplier',
       'max_parallel_workers_per_gather', 'max_parallel_workers',
       'max_worker_processes', 'parallel_setup_cost',
       'parallel_tuple_cost', 'min_parallel_table_scan_size',
       'min_parallel_index_scan_size', 'parallel_leader_participation',
       'jit', 'jit_above_cost', 'jit_inline_above_cost',
       'jit_optimize_above_cost',
       'autovacuum', 'autovacuum_vacuum_threshold',
       'autovacuum_vacuum_scale_factor', 'autovacuum_analyze_threshold',
       'autovacuum_analyze_scale_factor', 'autovacuum_vacuum_cost_delay',
       'autovacuum_vacuum_cost_limit', 'autovacuum_freeze_max_age',
       'autovacuum_multixact_freeze_max_age'
 )
    OR name LIKE 'enable\_%'
 ORDER BY name
