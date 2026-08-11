-- dryrun-readonly-role.sql
--
-- Creates a read-only role for use with dryrun's dump-schema functionality.
-- Works with PostgreSQL 14+.
--
-- Usage:
--   psql -v db_name=mydb -v dryrun_password="'s3cret'" -f dryrun-readonly-role.sql

-- 1. Create the role
CREATE ROLE dryrun_readonly NOLOGIN;

-- 2. Allow connecting to the target database
GRANT CONNECT ON DATABASE :db_name TO dryrun_readonly;

-- 3. pg_read_all_data covers SELECT on all tables, views, sequences across all schemas (PG14+)
GRANT pg_read_all_data TO dryrun_readonly;

-- 4. pg_read_all_stats exposes pg_stat_replication peers and full pg_replication_slots
-- visibility. Without it both sections may appear empty rather than failing.
GRANT pg_read_all_stats TO dryrun_readonly;

-- 5. Create a login user that inherits the role
CREATE ROLE dryrun_user LOGIN PASSWORD :dryrun_password IN ROLE dryrun_readonly;
