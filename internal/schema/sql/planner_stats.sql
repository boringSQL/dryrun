-- name: fetch-planner-table-sizing
-- pg_class.reltuples + on-disk footprint (heap, total, indexes, toast)
SELECT n.nspname                                       AS schema_name,
       c.relname                                       AS table_name,
       c.reltuples::float8                             AS reltuples,
       c.relpages::int8                                AS relpages,
       pg_catalog.pg_relation_size(c.oid)::int8        AS table_size,
       pg_catalog.pg_total_relation_size(c.oid)::int8  AS total_relation_size,
       pg_catalog.pg_indexes_size(c.oid)::int8         AS indexes_size,
       COALESCE(pg_catalog.pg_total_relation_size(c.reltoastrelid), 0)::int8 AS toast_size
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relkind IN ('r', 'p')
   AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
   AND n.nspname NOT LIKE 'pg_temp_%'
 ORDER BY n.nspname, c.relname

-- name: fetch-planner-index-sizing
SELECT n.nspname                                  AS schema_name,
       ct.relname                                  AS table_name,
       ci.relname                                  AS index_name,
       ci.relpages::int8                           AS relpages,
       ci.reltuples::float8                        AS reltuples,
       pg_catalog.pg_relation_size(ci.oid)::int8   AS size
  FROM pg_catalog.pg_index i
  JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
  JOIN pg_catalog.pg_class ct ON ct.oid = i.indrelid
  JOIN pg_catalog.pg_namespace n ON n.oid = ct.relnamespace
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
   AND n.nspname NOT LIKE 'pg_temp_%'
 ORDER BY n.nspname, ct.relname, ci.relname

-- name: fetch-planner-column-stats
-- per-column pg_stats; mcv/histogram lists kept as text to avoid type juggling
SELECT s.schemaname               AS schema_name,
       s.tablename                AS table_name,
       s.attname                  AS column_name,
       s.null_frac::float8        AS null_frac,
       s.n_distinct::float8       AS n_distinct,
       s.most_common_vals::text   AS most_common_vals,
       s.most_common_freqs::text  AS most_common_freqs,
       s.histogram_bounds::text   AS histogram_bounds,
       s.correlation::float8      AS correlation
  FROM pg_catalog.pg_stats s
 WHERE s.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
 ORDER BY s.schemaname, s.tablename, s.attname
