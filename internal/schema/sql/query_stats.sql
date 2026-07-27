-- name: fetch-pg-stat-statements-installed
-- *_exec_time columns require PG13+
SELECT to_regclass('pg_stat_statements') IS NOT NULL
       AND current_setting('server_version_num')::int >= 130000

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
