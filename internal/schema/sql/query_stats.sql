-- name: fetch-pg-stat-statements-installed
-- *_exec_time columns require PG13+. The info view is pgss 1.9+, which ships with
-- PG14 but stays at the old version across pg_upgrade until ALTER EXTENSION UPDATE,
-- so probe it here: selecting from a missing view aborts the capture transaction.
SELECT to_regclass('pg_stat_statements') IS NOT NULL
       AND current_setting('server_version_num')::int >= 130000,
       to_regclass('pg_stat_statements_info') IS NOT NULL

-- name: fetch-query-stats
SELECT s.queryid, s.calls, s.query,
       s.total_exec_time, s.mean_exec_time, s.rows
  FROM pg_stat_statements s
 WHERE s.dbid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
   AND s.queryid IS NOT NULL
   AND s.query <> '<insufficient privilege>'
   -- utility-statement literals aren't $N-substituted; whitelist DML instead of blacklisting
   AND s.query ~* '^\s*(with|select|insert|update|delete|merge|table|values)\M'
 ORDER BY s.total_exec_time DESC
 LIMIT 500

-- name: fetch-pgss-info
-- PG14+; the view doesn't exist on PG13, so failure means absent, not zero
SELECT stats_reset, dealloc FROM pg_stat_statements_info

-- name: fetch-pgss-max
-- placeholder GUC, no row (or no privilege) means absent
SELECT setting::int FROM pg_catalog.pg_settings WHERE name = 'pg_stat_statements.max'
