-- name: lookup-column-meta
SELECT a.attname,
       c.oid,
       a.attnum,
       a.atttypid,
       format_type(a.atttypid, a.atttypmod),
       COALESCE((SELECT o.oid FROM pg_operator o
                  WHERE o.oprname = '=' AND o.oprleft = a.atttypid AND o.oprright = a.atttypid
                  LIMIT 1), 0)
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_attribute a ON a.attrelid = c.oid
 WHERE n.nspname = $1
   AND c.relname = $2
   AND a.attname = ANY($3)
   AND a.attnum > 0
   AND NOT a.attisdropped

-- name: restore-relation-stats-pg18
SELECT pg_restore_relation_stats(
    'version', $1::int, 'schemaname', $2::text, 'relname', $3::text,
    'relpages', $4::integer, 'reltuples', $5::real,
    'relallvisible', $6::integer
)

-- name: update-relation-stats-legacy
-- NULL $5 (pre-relallvisible snapshot) keeps the target's value
UPDATE pg_catalog.pg_class
   SET reltuples = $1, relpages = $2,
       relallvisible = COALESCE($5::int4, relallvisible)
 WHERE relname = $3
   AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $4)

-- name: delete-column-stats-legacy
DELETE FROM pg_statistic
 WHERE starelid = $1
   AND staattnum = $2
   AND stainherit = $3

-- name: probe-pg-regresql
SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_regresql')
    OR current_setting('shared_preload_libraries')  LIKE '%pg_regresql%'
    OR current_setting('session_preload_libraries') LIKE '%pg_regresql%'
    OR current_setting('local_preload_libraries')   LIKE '%pg_regresql%'

-- name: probe-extended-stats-restore
SELECT EXISTS (
    SELECT 1
      FROM pg_catalog.pg_proc p
      JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
     WHERE n.nspname = 'pg_catalog'
       AND p.proname = 'pg_restore_extended_stats'
)

-- name: lookup-extended-stats-layout
-- One row only when the table and statistics object both exist; pgx.ErrNoRows
-- then means the target is missing the object.
SELECT s.oid,
       pg_catalog.pg_get_statisticsobjdef_columns(s.oid),
       pg_catalog.string_to_array(s.stxkeys::text, ' ')::int2[]
  FROM pg_catalog.pg_statistic_ext s
  JOIN pg_catalog.pg_namespace sn ON sn.oid = s.stxnamespace
  JOIN pg_catalog.pg_class c ON c.oid = s.stxrelid
  JOIN pg_catalog.pg_namespace tn ON tn.oid = c.relnamespace
 WHERE sn.nspname = $1
   AND s.stxname = $2
   AND tn.nspname = $3
   AND c.relname = $4

-- name: clear-extended-stats
SELECT pg_clear_extended_stats($1::name, $2::name, $3::name, $4::name, $5::boolean)

-- name: read-extended-stats-kinds
SELECT sd.stxdndistinct IS NOT NULL,
       sd.stxddependencies IS NOT NULL,
       sd.stxdmcv IS NOT NULL
  FROM pg_catalog.pg_statistic_ext_data sd
 WHERE sd.stxoid = $1
   AND sd.stxdinherit = $2
