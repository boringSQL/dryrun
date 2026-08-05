-- name: fetch-pg-stat-statements-installed
SELECT to_regclass('pg_stat_statements') IS NOT NULL
       AND current_setting('server_version_num')::int >= 130000,
       to_regclass('pg_stat_statements_info') IS NOT NULL,
       EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                WHERE attrelid = to_regclass('pg_stat_statements')
                  AND attname = 'toplevel' AND attnum > 0 AND NOT attisdropped),
       EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                WHERE attrelid = to_regclass('pg_stat_statements')
                  AND attname = 'shared_blk_read_time' AND attnum > 0 AND NOT attisdropped)

-- name: fetch-query-stats
SELECT s.queryid, s.calls, s.query,
       s.total_exec_time, s.stddev_exec_time, s.rows,
       -- temp blocks: the sorts and hashes that spilled out of work_mem. Present in
       -- every pg_stat_statements version we support, with no configuration gate,
       -- unlike the shared block TIMINGS that need track_io_timing.
       s.temp_blks_read, s.temp_blks_written,
       s.shared_blks_hit, s.shared_blks_read, s.shared_blks_dirtied, s.shared_blks_written,
       s.__READ_TIME__, s.__WRITE_TIME__
  FROM pg_stat_statements s
 WHERE s.dbid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
   AND s.queryid IS NOT NULL
   AND s.query <> '<insufficient privilege>'
   -- utility-statement literals aren't $N-substituted; whitelist DML instead of blacklisting.
   -- COPY admitted for bulk-load visibility; dropUnsafeCopy keeps only literal-free forms.
   AND s.query ~* '^\s*(with|select|insert|update|delete|merge|table|values|copy)\M'
 ORDER BY s.total_exec_time DESC
 LIMIT 500

-- name: fetch-query-stats-toplevel
-- pgss 1.9+ only; separate query because an older pgss (PG13, or one left
-- below 1.9 by pg_upgrade) has no toplevel column and this would not compile. pgss also counts nested statements' time inside
-- the caller's total, so the filter avoids double counting and keeps churning
-- nested rows out of the top 500. Consequence: function/trigger work is
-- invisible even under track = 'all', exactly as under track = 'top'.
SELECT s.queryid, s.calls, s.query,
       s.total_exec_time, s.stddev_exec_time, s.rows,
       -- temp blocks: the sorts and hashes that spilled out of work_mem. Present in
       -- every pg_stat_statements version we support, with no configuration gate,
       -- unlike the shared block TIMINGS that need track_io_timing.
       s.temp_blks_read, s.temp_blks_written,
       s.shared_blks_hit, s.shared_blks_read, s.shared_blks_dirtied, s.shared_blks_written,
       s.__READ_TIME__, s.__WRITE_TIME__
  FROM pg_stat_statements s
 WHERE s.dbid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
   AND s.queryid IS NOT NULL
   AND s.query <> '<insufficient privilege>'
   AND s.toplevel
   -- utility-statement literals aren't $N-substituted; whitelist DML instead of blacklisting.
   -- COPY admitted for bulk-load visibility; dropUnsafeCopy keeps only literal-free forms.
   AND s.query ~* '^\s*(with|select|insert|update|delete|merge|table|values|copy)\M'
 ORDER BY s.total_exec_time DESC
 LIMIT 500

-- name: fetch-pgss-info
-- PG14+; the view doesn't exist on PG13, so failure means absent, not zero
SELECT stats_reset, dealloc FROM pg_stat_statements_info

-- name: fetch-pgss-max
-- placeholder GUC, no row (or no privilege) means absent
SELECT setting::int FROM pg_catalog.pg_settings WHERE name = 'pg_stat_statements.max'

-- name: fetch-pgss-track
-- 'top' (the default) records only statements issued directly by a client, so
-- work inside functions and triggers never appears as a query shape at all.
SELECT setting FROM pg_catalog.pg_settings WHERE name = 'pg_stat_statements.track'

-- name: fetch-block-size
-- The compile-time block size, which is what turns a temp-block count into the bytes an
-- operator can act on. Almost always 8192; assuming it would be a silent 2x error on the
-- clusters where it is not, so it rides the capture instead.
SELECT current_setting('block_size')::int

-- name: fetch-track-io-timing
SELECT setting = 'on' FROM pg_catalog.pg_settings WHERE name = 'track_io_timing'

-- name: fetch-pgss-track-planning
-- pgss 1.8+; a missing row means the GUC is absent, not off
SELECT setting = 'on' FROM pg_catalog.pg_settings WHERE name = 'pg_stat_statements.track_planning'

-- name: fetch-track-activity-query-size
SELECT setting::int FROM pg_catalog.pg_settings WHERE name = 'track_activity_query_size'
