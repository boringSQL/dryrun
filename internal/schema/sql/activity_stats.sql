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
       COALESCE(s.n_mod_since_analyze, 0)::int8 AS n_mod_since_analyze,
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

-- name: fetch-database-stats
SELECT d.deadlocks,
       d.temp_files,
       d.temp_bytes,
       d.xact_commit,
       d.xact_rollback,
       d.blks_hit,
       d.blks_read,
       d.conflicts,
       d.checksum_failures,
       d.stats_reset
  FROM pg_catalog.pg_stat_database d
 WHERE d.datname = current_database()

-- name: fetch-has-wal-status
-- wal_status/safe_wal_size are PG13+; a pre-13 primary has neither column.
SELECT current_setting('server_version_num')::int >= 130000

-- name: fetch-replication-slots
SELECT slot_name, slot_type, active, wal_status, safe_wal_size
  FROM pg_catalog.pg_replication_slots
 ORDER BY slot_name

-- name: fetch-replication-slots-no-wal-status
SELECT slot_name, slot_type, active
  FROM pg_catalog.pg_replication_slots
 ORDER BY slot_name

-- name: fetch-has-pg-stat-checkpointer
-- PG17 split checkpoint counters out of pg_stat_bgwriter into their own view.
SELECT to_regclass('pg_catalog.pg_stat_checkpointer') IS NOT NULL

-- name: fetch-checkpointer-stats-pg17
SELECT num_timed, num_requested, stats_reset
  FROM pg_catalog.pg_stat_checkpointer

-- name: fetch-checkpointer-stats-legacy
SELECT checkpoints_timed, checkpoints_req, stats_reset
  FROM pg_catalog.pg_stat_bgwriter
