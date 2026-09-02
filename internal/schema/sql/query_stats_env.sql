-- name: fetch-pgss-environment
SELECT (SELECT setting::int FROM pg_catalog.pg_settings WHERE name = 'pg_stat_statements.max'),
       (SELECT setting FROM pg_catalog.pg_settings WHERE name = 'pg_stat_statements.track'),
       (SELECT setting = 'on' FROM pg_catalog.pg_settings WHERE name = 'pg_stat_statements.track_planning'),
       (SELECT setting = 'on' FROM pg_catalog.pg_settings WHERE name = 'track_io_timing'),
       (SELECT setting::int FROM pg_catalog.pg_settings WHERE name = 'track_activity_query_size'),
       (SELECT setting::int FROM pg_catalog.pg_settings WHERE name = 'block_size')
