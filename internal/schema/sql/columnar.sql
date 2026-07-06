-- name: columnar-engine-enabled
SELECT current_setting('google_columnar_engine.enabled', true) = 'on'

-- name: columnar-columns
SELECT schema_name,
       relation_name,
       column_name,
       column_type,
       status,
       size_in_bytes,
       num_times_accessed,
       last_accessed_time
  FROM g_columnar_columns
 ORDER BY size_in_bytes DESC

-- name: columnar-relations
SELECT schema_name,
       relation_name,
       status,
       size,
       invalid_block_count,
       total_block_count,
       auto_refresh_failure_count
  FROM g_columnar_relations
 ORDER BY size DESC
