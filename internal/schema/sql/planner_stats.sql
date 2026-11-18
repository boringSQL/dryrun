-- name: fetch-planner-table-sizing
-- pg_class.reltuples + on-disk footprint + freeze xids (0 for partitioned parents);
-- ages derived offline against database_xid/database_mxid to keep rows dedup-stable.
SELECT n.nspname                                       AS schema_name,
       c.relname                                       AS table_name,
       c.reltuples::float8                             AS reltuples,
       c.relpages::int8                                AS relpages,
       c.relallvisible::int8                           AS relallvisible,
       pg_catalog.pg_relation_size(c.oid)::int8        AS table_size,
       pg_catalog.pg_total_relation_size(c.oid)::int8  AS total_relation_size,
       pg_catalog.pg_indexes_size(c.oid)::int8         AS indexes_size,
       COALESCE(pg_catalog.pg_total_relation_size(c.reltoastrelid), 0)::int8 AS toast_size,
       c.relfrozenxid::text::int8                      AS relfrozenxid,
       c.relminmxid::text::int8                        AS relminmxid
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

-- name: fetch-planner-extended-stats
-- n_distinct/dependencies: PG19 uses JSON arrays, older versions need trasnlation
-- via pg_dump.
WITH stats AS (
    SELECT s.oid,
           s.stxrelid,
           s.stxkeys,
           cn.nspname AS table_schema,
           c.relname  AS table_name,
           sn.nspname AS stats_schema,
           s.stxname  AS stats_name
      FROM pg_catalog.pg_statistic_ext s
      JOIN pg_catalog.pg_class c ON c.oid = s.stxrelid
      JOIN pg_catalog.pg_namespace cn ON cn.oid = c.relnamespace
      JOIN pg_catalog.pg_namespace sn ON sn.oid = s.stxnamespace
     WHERE cn.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
       AND cn.nspname NOT LIKE 'pg_temp_%'
), cols AS (
    SELECT st.oid,
           st.stxrelid,
           st.stxkeys,
           st.table_schema,
           st.table_name,
           st.stats_schema,
           st.stats_name,
           (SELECT array_agg(x.attname ORDER BY x.attnum)
              FROM (SELECT DISTINCT a.attnum, a.attname::text AS attname
                      FROM pg_catalog.pg_depend d
                      JOIN pg_catalog.pg_attribute a
                        ON a.attrelid = st.stxrelid AND a.attnum = d.refobjsubid
                     WHERE d.classid = 'pg_catalog.pg_statistic_ext'::regclass
                       AND d.objid = st.oid
                       AND d.refclassid = 'pg_catalog.pg_class'::regclass
                       AND d.refobjid = st.stxrelid
                       AND d.refobjsubid > 0) x) AS columns
      FROM stats st
), resolved AS (
    SELECT c.*,
           EXISTS (
               SELECT 1
                 FROM pg_catalog.pg_attribute a
                 JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
                WHERE a.attrelid = c.stxrelid
                  AND a.attname::text = ANY(c.columns)
                  AND (t.typname = 'jsonb'
                       OR t.typname = '_jsonb'
                       OR (t.typtype = 'd' AND pg_catalog.format_type(t.typbasetype, NULL) LIKE '%jsonb%'))
           ) AS has_jsonb
      FROM cols c
)
SELECT r.table_schema,
       r.table_name,
       r.stats_schema,
       r.stats_name,
       r.columns,
       pg_catalog.pg_get_statisticsobjdef_columns(r.oid) AS definition,
       pg_catalog.string_to_array(r.stxkeys::text, ' ')::int2[] AS stxkeys,
       sd.stxdinherit,
       __NDISTINCT__ AS n_distinct,
       __DEPENDENCIES__ AS dependencies,
       CASE WHEN r.has_jsonb THEN NULL ELSE m.mcv_values END      AS mcv_values,
       CASE WHEN r.has_jsonb THEN NULL ELSE m.mcv_freqs END       AS mcv_freqs,
       CASE WHEN r.has_jsonb THEN NULL ELSE m.mcv_base_freqs END  AS mcv_base_freqs
  FROM resolved r
  JOIN pg_catalog.pg_statistic_ext_data sd ON sd.stxoid = r.oid
  LEFT JOIN LATERAL (
      SELECT array_agg(mcv.values ORDER BY mcv."index")::text          AS mcv_values,
             array_agg(mcv.frequency ORDER BY mcv."index")::text       AS mcv_freqs,
             array_agg(mcv.base_frequency ORDER BY mcv."index")::text  AS mcv_base_freqs
        FROM pg_catalog.pg_mcv_list_items(sd.stxdmcv) AS mcv
  ) m ON sd.stxdmcv IS NOT NULL
 ORDER BY r.table_schema, r.table_name, r.stats_schema, r.stats_name, sd.stxdinherit

-- name: fetch-planner-column-stats
-- mcv/histogram as text to avoid type juggling; jsonb MCV stripped to keep row payloads out of the artifact
-- inheritance parents have two rows per column (f/t); prefer the own-heap row
SELECT DISTINCT ON (s.schemaname, s.tablename, s.attname)
       s.schemaname               AS schema_name,
       s.tablename                AS table_name,
       s.attname                  AS column_name,
       s.null_frac::float8        AS null_frac,
       s.n_distinct::float8       AS n_distinct,
       CASE WHEN t.typname = 'jsonb'
                 OR t.typname = '_jsonb'
                 OR (t.typtype = 'd' AND pg_catalog.format_type(t.typbasetype, NULL) LIKE '%jsonb%')
            THEN NULL
            ELSE s.most_common_vals::text END   AS most_common_vals,
       CASE WHEN t.typname = 'jsonb'
                 OR t.typname = '_jsonb'
                 OR (t.typtype = 'd' AND pg_catalog.format_type(t.typbasetype, NULL) LIKE '%jsonb%')
            THEN NULL
            ELSE s.most_common_freqs::text END  AS most_common_freqs,
       s.histogram_bounds::text   AS histogram_bounds,
       s.correlation::float8      AS correlation,
       s.avg_width::int           AS avg_width,
       s.inherited                AS inherited
  FROM pg_catalog.pg_stats s
  JOIN pg_catalog.pg_class     c ON c.relname = s.tablename
  JOIN pg_catalog.pg_namespace  n ON n.oid = c.relnamespace AND n.nspname = s.schemaname
  JOIN pg_catalog.pg_attribute  a ON a.attrelid = c.oid AND a.attname = s.attname
  JOIN pg_catalog.pg_type       t ON t.oid = a.atttypid
 WHERE s.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
 ORDER BY s.schemaname, s.tablename, s.attname, s.inherited
