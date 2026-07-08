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
    'relpages', $4::integer, 'reltuples', $5::real
)

-- name: update-relation-stats-legacy
UPDATE pg_catalog.pg_class
   SET reltuples = $1, relpages = $2
 WHERE relname = $3
   AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $4)

-- name: delete-column-stats-legacy
DELETE FROM pg_statistic
 WHERE starelid = $1
   AND staattnum = $2
   AND NOT stainherit

-- name: probe-pg-regresql
SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_regresql')
    OR current_setting('shared_preload_libraries')  LIKE '%pg_regresql%'
    OR current_setting('session_preload_libraries') LIKE '%pg_regresql%'
    OR current_setting('local_preload_libraries')   LIKE '%pg_regresql%'
