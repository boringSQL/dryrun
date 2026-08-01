-- name: fetch-pg-stat-statements-installed
SELECT to_regclass('pg_stat_statements') IS NOT NULL
       AND current_setting('server_version_num')::int >= 130000,
       to_regclass('pg_stat_statements_info') IS NOT NULL,
       EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                WHERE attrelid = to_regclass('pg_stat_statements')
                  AND attname = 'toplevel' AND attnum > 0 AND NOT attisdropped)

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

SELECT s.queryid, s.calls, s.query,
       s.total_exec_time, s.mean_exec_time, s.rows, s.toplevel
  FROM pg_stat_statements s
 WHERE s.dbid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
   AND s.queryid IS NOT NULL
   AND s.query <> '<insufficient privilege>'
   AND s.query ~* '^\s*(with|select|insert|update|delete|merge|table|values)\M'
 ORDER BY s.total_exec_time DESC
 LIMIT 500

-- name: fetch-pgss-info
-- PG14+; the view doesn't exist on PG13, so failure means absent, not zero
SELECT stats_reset, dealloc FROM pg_stat_statements_info

-- name: fetch-pgss-max
-- placeholder GUC, no row (or no privilege) means absent
SELECT setting::int FROM pg_catalog.pg_settings WHERE name = 'pg_stat_statements.max'

SELECT setting FROM pg_catalog.pg_settings WHERE name = 'pg_stat_statements.track'
