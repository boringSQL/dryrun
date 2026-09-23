-- name: fetch-pg-stat-statements-installed
-- No server_version_num gate: the hazard is the EXTENSION version, not the
-- server major (pg_upgrade leaves old pgss behind on new servers).
SELECT to_regclass('pg_stat_statements') IS NOT NULL
       -- total_exec_time is pgss 1.8+; without it the projection cannot
       -- compile, so an older pgss counts as not installed.
       AND EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                    WHERE attrelid = to_regclass('pg_stat_statements')
                      AND attname = 'total_exec_time' AND attnum > 0 AND NOT attisdropped),
       to_regclass('pg_stat_statements_info') IS NOT NULL,
       EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                WHERE attrelid = to_regclass('pg_stat_statements')
                  AND attname = 'toplevel' AND attnum > 0 AND NOT attisdropped),
       EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                WHERE attrelid = to_regclass('pg_stat_statements')
                  AND attname = 'shared_blk_read_time' AND attnum > 0 AND NOT attisdropped)

-- name: fetch-query-stats
-- fold pgss's per-(userid,queryid) rows BEFORE the cap, so a queryid is
-- captured whole rather than one role's row crossing the cap alone
SELECT s.queryid, sum(s.calls)::bigint,
       -- smallest text, matching qshape's member sort so owner/tag attribution is stable
       min(s.query COLLATE "C"),
       sum(s.total_exec_time),
       -- pooled population stddev; a lone row keeps its own value, greatest() absorbs NULL (calls=0 under track_planning)
       CASE WHEN count(*) = 1 THEN max(s.stddev_exec_time)
            ELSE sqrt(greatest(
              sum(s.calls * (s.stddev_exec_time ^ 2 + (s.total_exec_time / nullif(s.calls, 0)) ^ 2))
                / nullif(sum(s.calls), 0)
              - (sum(s.total_exec_time) / nullif(sum(s.calls), 0)) ^ 2, 0))
       END,
       sum(s.rows)::bigint,
       -- temp blocks: the sorts and hashes that spilled out of work_mem. Present in
       -- every pg_stat_statements version we support, with no configuration gate,
       -- unlike the shared block TIMINGS that need track_io_timing.
       sum(s.temp_blks_read)::bigint, sum(s.temp_blks_written)::bigint,
       sum(s.shared_blks_hit)::bigint, sum(s.shared_blks_read)::bigint,
       sum(s.shared_blks_dirtied)::bigint, sum(s.shared_blks_written)::bigint,
       sum(s.__READ_TIME__), sum(s.__WRITE_TIME__)
  FROM pg_stat_statements s
 WHERE s.dbid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
   AND s.queryid IS NOT NULL
   AND s.query <> '<insufficient privilege>'
   -- utility-statement literals aren't $N-substituted; whitelist DML instead of blacklisting.
   -- COPY admitted for bulk-load visibility; dropUnsafeCopy keeps only literal-free forms.
   -- Leading `-- comment` lines (sqlc-style annotations) stripped before matching.
   AND regexp_replace(s.query, '^(\s*--[^\n]*\n)*\s*', '')
         ~* '^(with|select|insert|update|delete|merge|table|values|copy)\M'
 GROUP BY s.queryid
 ORDER BY sum(s.total_exec_time) DESC, s.queryid
 LIMIT $1

-- name: fetch-query-stats-toplevel
-- pgss 1.9+ only; an older pgss has no toplevel column and this would not
-- compile. The filter avoids double counting (pgss counts nested time inside
-- the caller's total). Consequence: function/trigger work is invisible even
-- under track = 'all', exactly as under track = 'top'.
-- fold pgss's per-(userid,queryid) rows BEFORE the cap, so a queryid is
-- captured whole rather than one role's row crossing the cap alone
SELECT s.queryid, sum(s.calls)::bigint,
       -- smallest text, matching qshape's member sort so owner/tag attribution is stable
       min(s.query COLLATE "C"),
       sum(s.total_exec_time),
       -- pooled population stddev; a lone row keeps its own value, greatest() absorbs NULL (calls=0 under track_planning)
       CASE WHEN count(*) = 1 THEN max(s.stddev_exec_time)
            ELSE sqrt(greatest(
              sum(s.calls * (s.stddev_exec_time ^ 2 + (s.total_exec_time / nullif(s.calls, 0)) ^ 2))
                / nullif(sum(s.calls), 0)
              - (sum(s.total_exec_time) / nullif(sum(s.calls), 0)) ^ 2, 0))
       END,
       sum(s.rows)::bigint,
       -- temp blocks: the sorts and hashes that spilled out of work_mem. Present in
       -- every pg_stat_statements version we support, with no configuration gate,
       -- unlike the shared block TIMINGS that need track_io_timing.
       sum(s.temp_blks_read)::bigint, sum(s.temp_blks_written)::bigint,
       sum(s.shared_blks_hit)::bigint, sum(s.shared_blks_read)::bigint,
       sum(s.shared_blks_dirtied)::bigint, sum(s.shared_blks_written)::bigint,
       sum(s.__READ_TIME__), sum(s.__WRITE_TIME__)
  FROM pg_stat_statements s
 WHERE s.dbid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
   AND s.queryid IS NOT NULL
   AND s.query <> '<insufficient privilege>'
   AND s.toplevel
   -- utility-statement literals aren't $N-substituted; whitelist DML instead of blacklisting.
   -- COPY admitted for bulk-load visibility; dropUnsafeCopy keeps only literal-free forms.
   -- Leading `-- comment` lines (sqlc-style annotations) stripped before matching.
   AND regexp_replace(s.query, '^(\s*--[^\n]*\n)*\s*', '')
         ~* '^(with|select|insert|update|delete|merge|table|values|copy)\M'
 GROUP BY s.queryid
 ORDER BY sum(s.total_exec_time) DESC, s.queryid
 LIMIT $1

-- name: fetch-pgss-info
-- pgss 1.9+; a lower pgss has no such view, so failure means absent, not zero
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
