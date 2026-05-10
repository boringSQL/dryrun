-- name: fetch-activity-tables
SELECT s.schemaname                          AS schema_name,
       s.relname                             AS table_name,
       COALESCE(s.seq_scan, 0)::int8         AS seq_scan,
       COALESCE(s.seq_tup_read, 0)::int8     AS seq_tup_read,
       COALESCE(s.idx_scan, 0)::int8         AS idx_scan,
       COALESCE(s.idx_tup_fetch, 0)::int8    AS idx_tup_fetch,
       COALESCE(s.n_tup_ins, 0)::int8        AS n_tup_ins,
       COALESCE(s.n_tup_upd, 0)::int8        AS n_tup_upd,
       COALESCE(s.n_tup_del, 0)::int8        AS n_tup_del,
       COALESCE(s.n_tup_hot_upd, 0)::int8    AS n_tup_hot_upd,
       COALESCE(s.n_live_tup, 0)::int8       AS n_live_tup,
       COALESCE(s.n_dead_tup, 0)::int8       AS n_dead_tup,
       s.last_vacuum                          AS last_vacuum,
       s.last_autovacuum                      AS last_autovacuum,
       s.last_analyze                         AS last_analyze,
       s.last_autoanalyze                     AS last_autoanalyze,
       COALESCE(s.vacuum_count, 0)::int8      AS vacuum_count,
       COALESCE(s.autovacuum_count, 0)::int8  AS autovacuum_count,
       COALESCE(s.analyze_count, 0)::int8     AS analyze_count,
       COALESCE(s.autoanalyze_count, 0)::int8 AS autoanalyze_count
  FROM pg_catalog.pg_stat_user_tables s
 WHERE s.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
 ORDER BY s.schemaname, s.relname

-- name: fetch-activity-indexes
SELECT s.schemaname                       AS schema_name,
       s.relname                          AS table_name,
       s.indexrelname                     AS index_name,
       COALESCE(s.idx_scan, 0)::int8      AS idx_scan,
       COALESCE(s.idx_tup_read, 0)::int8  AS idx_tup_read,
       COALESCE(s.idx_tup_fetch, 0)::int8 AS idx_tup_fetch
  FROM pg_catalog.pg_stat_user_indexes s
 WHERE s.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
 ORDER BY s.schemaname, s.relname, s.indexrelname

-- name: fetch-node-identity
-- pg_stat_replication is primary-side; pg_is_in_recovery() distinguishes role
SELECT pg_catalog.pg_is_in_recovery() AS is_standby,
       version()                       AS pg_version
