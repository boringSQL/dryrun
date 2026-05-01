use std::fmt;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Entry
// ---------------------------------------------------------------------------

#[must_use]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub status: String,
    pub reason: String,
    pub fix: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "STATUS: {}\nREASON: {}\nFIX:\n{}",
            self.status, self.reason, self.fix
        )?;
        if let Some(note) = &self.note {
            write!(f, "\nNOTE: {note}")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

/// Returns the part after the last dot, or the whole string.
#[must_use]
pub fn strip_schema(qualified: &str) -> &str {
    match qualified.rfind('.') {
        Some(pos) => &qualified[pos + 1..],
        None => qualified,
    }
}

// ---------------------------------------------------------------------------
// Migration safety
// ---------------------------------------------------------------------------

pub fn add_column_volatile_default(
    table: &str,
    col: &str,
    col_type: &str,
    default_expr: &str,
) -> Entry {
    Entry {
        status: "unsafe".into(),
        reason: format!(
            "Adding column `{col}` ({col_type}) to `{table}` with volatile default `{default_expr}` rewrites the entire table while holding an ACCESS EXCLUSIVE lock."
        ),
        fix: format!(
            "ALTER TABLE {table} ADD COLUMN {col} {col_type};\n\
             -- backfill in batches, then:\n\
             ALTER TABLE {table} ALTER COLUMN {col} SET DEFAULT {default_expr};"
        ),
        note: Some("Volatile defaults (e.g. clock_timestamp(), random()) cannot use the PG 11+ fast-path; every row must be physically rewritten.".into()),
    }
}

pub fn add_column_pre_pg11(table: &str, col: &str, col_type: &str, default_expr: &str) -> Entry {
    Entry {
        status: "unsafe".into(),
        reason: format!(
            "Adding column `{col}` ({col_type}) to `{table}` with DEFAULT `{default_expr}` rewrites the entire table on PostgreSQL < 11."
        ),
        fix: format!(
            "ALTER TABLE {table} ADD COLUMN {col} {col_type};\n\
             ALTER TABLE {table} ALTER COLUMN {col} SET DEFAULT {default_expr};\n\
             -- backfill existing rows in batches"
        ),
        note: Some("PostgreSQL 11+ can add columns with a non-volatile default without a table rewrite. Upgrade or use the three-step pattern.".into()),
    }
}

pub fn alter_column_type(table: &str, col: &str, new_type: &str) -> Entry {
    Entry {
        status: "unsafe".into(),
        reason: format!(
            "Changing the type of `{table}.{col}` to `{new_type}` rewrites the table and holds an ACCESS EXCLUSIVE lock for the duration."
        ),
        fix: format!(
            "-- 1. Add a new column with the desired type\n\
             ALTER TABLE {table} ADD COLUMN {col}_new {new_type};\n\
             -- 2. Backfill in batches\n\
             UPDATE {table} SET {col}_new = {col}::{new_type} WHERE {col}_new IS NULL;\n\
             -- 3. Swap inside a short lock\n\
             ALTER TABLE {table} RENAME COLUMN {col} TO {col}_old;\n\
             ALTER TABLE {table} RENAME COLUMN {col}_new TO {col};\n\
             ALTER TABLE {table} DROP COLUMN {col}_old;"
        ),
        note: None,
    }
}

pub fn set_not_null(table: &str, col: &str, pg_major: u32) -> Entry {
    if pg_major >= 12 {
        Entry {
            status: "safe-with-pattern".into(),
            reason: format!(
                "SET NOT NULL on `{table}.{col}` scans the entire table to verify no NULLs exist, holding an ACCESS EXCLUSIVE lock."
            ),
            fix: format!(
                "-- PG 12+: add a CHECK constraint NOT VALID, then validate separately\n\
                 ALTER TABLE {table} ADD CONSTRAINT {table}_{col}_not_null CHECK ({col} IS NOT NULL) NOT VALID;\n\
                 ALTER TABLE {table} VALIDATE CONSTRAINT {table}_{col}_not_null;\n\
                 -- once validated, the NOT NULL can be added instantly:\n\
                 ALTER TABLE {table} ALTER COLUMN {col} SET NOT NULL;\n\
                 ALTER TABLE {table} DROP CONSTRAINT {table}_{col}_not_null;"
            ),
            note: Some("On PG 12+ the planner recognises a validated CHECK (col IS NOT NULL) and skips the full-table scan when SET NOT NULL is applied.".into()),
        }
    } else {
        Entry {
            status: "unsafe".into(),
            reason: format!(
                "SET NOT NULL on `{table}.{col}` performs a full-table scan under an ACCESS EXCLUSIVE lock. PG < 12 has no fast-path."
            ),
            fix: format!(
                "-- Add a CHECK constraint NOT VALID and validate in a separate transaction\n\
                 ALTER TABLE {table} ADD CONSTRAINT {table}_{col}_not_null CHECK ({col} IS NOT NULL) NOT VALID;\n\
                 ALTER TABLE {table} VALIDATE CONSTRAINT {table}_{col}_not_null;\n\
                 -- NOTE: on PG < 12 you cannot then use SET NOT NULL without a scan;\n\
                 -- keep the CHECK constraint as the enforcement mechanism."
            ),
            note: Some("Upgrade to PostgreSQL 12+ to get the fast SET NOT NULL path after CHECK validation.".into()),
        }
    }
}

pub fn add_foreign_key_unsafe(table: &str, col: &str, ref_table: &str, ref_col: &str) -> Entry {
    Entry {
        status: "unsafe".into(),
        reason: format!(
            "Adding a foreign key `{table}.{col}` → `{ref_table}.{ref_col}` validates the entire table while holding a SHARE ROW EXCLUSIVE lock on both tables."
        ),
        fix: format!(
            "ALTER TABLE {table} ADD CONSTRAINT {table}_{col}_fkey\n\
             \x20 FOREIGN KEY ({col}) REFERENCES {ref_table}({ref_col}) NOT VALID;\n\
             ALTER TABLE {table} VALIDATE CONSTRAINT {table}_{col}_fkey;"
        ),
        note: Some("NOT VALID takes only a brief lock; VALIDATE then checks rows with a weaker ROW SHARE lock.".into()),
    }
}

pub fn add_check_constraint_unsafe(table: &str, constraint_expr: &str) -> Entry {
    Entry {
        status: "unsafe".into(),
        reason: format!(
            "Adding CHECK ({constraint_expr}) on `{table}` validates every row under an ACCESS EXCLUSIVE lock."
        ),
        fix: format!(
            "ALTER TABLE {table} ADD CONSTRAINT {table}_check\n\
             \x20 CHECK ({constraint_expr}) NOT VALID;\n\
             ALTER TABLE {table} VALIDATE CONSTRAINT {table}_check;"
        ),
        note: None,
    }
}

pub fn create_index_blocking(table: &str, idx_name: &str, method: &str, columns: &str) -> Entry {
    Entry {
        status: "unsafe".into(),
        reason: format!(
            "CREATE INDEX `{idx_name}` on `{table}` blocks all writes (INSERT/UPDATE/DELETE) for the duration of the build."
        ),
        fix: format!(
            "CREATE INDEX CONCURRENTLY {idx_name} ON {table} USING {method} ({columns});"
        ),
        note: Some("CONCURRENTLY builds the index without holding a long write lock. It takes longer but does not block DML.".into()),
    }
}

pub fn rename(old_name: &str, new_name: &str) -> Entry {
    Entry {
        status: "unsafe".into(),
        reason: format!(
            "Renaming `{old_name}` to `{new_name}` breaks every query, view, function, and ORM mapping that references the old name."
        ),
        fix: format!(
            "-- 1. Create a view/alias with the new name pointing to the old\n\
             CREATE VIEW {new_name} AS SELECT * FROM {old_name};\n\
             -- 2. Migrate all application code to use `{new_name}`\n\
             -- 3. Once no references to `{old_name}` remain, drop the view and rename"
        ),
        note: None,
    }
}

// ---------------------------------------------------------------------------
// Plan warnings
// ---------------------------------------------------------------------------

pub fn cte_materialized(cte_name: &str, rows: i64) -> Entry {
    Entry {
        status: "warning".into(),
        reason: format!(
            "CTE `{cte_name}` is materialized ({rows} rows). The planner cannot push predicates into it, which may cause a full scan of the intermediate result."
        ),
        fix: format!(
            "-- Option A: rewrite as a sub-query or JOIN so the planner can push filters down\n\
             -- Option B (PG 12+): mark it AS NOT MATERIALIZED to allow predicate push-down\n\
             WITH {cte_name} AS NOT MATERIALIZED (\n\
             \x20 ... original query ...\n\
             )"
        ),
        note: Some("Before PG 12, all CTEs were forced-materialized. If you need PG < 12 support, rewrite as a subquery.".into()),
    }
}

pub fn cte_over_partitioned_table(cte_name: &str, table: &str) -> Entry {
    Entry {
        status: "warning".into(),
        reason: format!(
            "CTE `{cte_name}` reads partitioned table `{table}`. Materialisation prevents partition pruning; all partitions are scanned."
        ),
        fix: format!(
            "-- Move the filter inside the CTE, or rewrite as a subquery:\n\
             WITH {cte_name} AS NOT MATERIALIZED (\n\
             \x20 SELECT ... FROM {table} WHERE <partition_key filter>\n\
             )"
        ),
        note: None,
    }
}

pub fn no_partition_pruning(
    table: &str,
    partition_key: &str,
    scanned: usize,
    total: usize,
) -> Entry {
    Entry {
        status: "warning".into(),
        reason: format!(
            "Query scans {scanned}/{total} partitions of `{table}`: no pruning on `{partition_key}`."
        ),
        fix: format!(
            "-- Add a WHERE clause (or JOIN condition) on the partition key `{partition_key}`\n\
             -- to let the planner eliminate unneeded partitions.\n\
             SELECT ... FROM {table} WHERE {partition_key} = $1;"
        ),
        note: None,
    }
}

// ---------------------------------------------------------------------------
// Index advice
// ---------------------------------------------------------------------------

pub fn suggest_gin(table: &str, col: &str, col_type: &str) -> Entry {
    Entry {
        status: "advice".into(),
        reason: format!(
            "Column `{table}.{col}` ({col_type}) would benefit from a GIN index for containment and existence queries."
        ),
        fix: format!("CREATE INDEX CONCURRENTLY ON {table} USING gin ({col});"),
        note: Some("GIN indexes are ideal for JSONB, arrays, and full-text search columns.".into()),
    }
}

pub fn suggest_gist(table: &str, col: &str, col_type: &str) -> Entry {
    Entry {
        status: "advice".into(),
        reason: format!(
            "Column `{table}.{col}` ({col_type}) would benefit from a GiST index for range or spatial queries."
        ),
        fix: format!("CREATE INDEX CONCURRENTLY ON {table} USING gist ({col});"),
        note: Some(
            "GiST indexes are ideal for range types, geometric types, and inet/cidr.".into(),
        ),
    }
}

pub fn suggest_partial_index(table: &str, col: &str, predicate: &str) -> Entry {
    Entry {
        status: "advice".into(),
        reason: format!(
            "Column `{table}.{col}` is mostly filtered with `{predicate}`. A partial index avoids indexing irrelevant rows."
        ),
        fix: format!("CREATE INDEX CONCURRENTLY ON {table} ({col}) WHERE {predicate};"),
        note: None,
    }
}

// ---------------------------------------------------------------------------
// Lint fixes
// ---------------------------------------------------------------------------

pub fn missing_primary_key(table: &str) -> Entry {
    Entry {
        status: "lint".into(),
        reason: format!(
            "Table `{table}` has no primary key. This breaks logical replication, many ORMs, and makes UPDATE/DELETE without a scan impossible."
        ),
        fix: format!(
            "-- If a natural key exists:\n\
             ALTER TABLE {table} ADD PRIMARY KEY (id);\n\
             -- Otherwise add a surrogate:\n\
             ALTER TABLE {table} ADD COLUMN {table}_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY;"
        ),
        note: None,
    }
}

pub fn text_over_varchar(table: &str, col: &str) -> Entry {
    Entry {
        status: "lint".into(),
        reason: format!(
            "Column `{table}.{col}` uses VARCHAR(n). In PostgreSQL there is no performance difference; VARCHAR just adds a hidden CHECK constraint."
        ),
        fix: format!(
            "ALTER TABLE {table} ALTER COLUMN {col} TYPE TEXT;"
        ),
        note: Some("If you need a length limit, use an explicit CHECK (length(col) <= N) so the constraint name is visible.".into()),
    }
}

pub fn timestamp_to_timestamptz(table: &str, col: &str) -> Entry {
    Entry {
        status: "lint".into(),
        reason: format!(
            "Column `{table}.{col}` uses TIMESTAMP WITHOUT TIME ZONE. This silently drops timezone information and causes bugs across timezones."
        ),
        fix: format!(
            "ALTER TABLE {table} ALTER COLUMN {col} TYPE TIMESTAMPTZ USING {col} AT TIME ZONE 'UTC';"
        ),
        note: None,
    }
}

pub fn missing_timestamp(table: &str, col_name: &str) -> Entry {
    Entry {
        status: "lint".into(),
        reason: format!(
            "Table `{table}` is missing a `{col_name}` column. Without it you lose auditability and cannot do incremental extracts."
        ),
        fix: format!(
            "ALTER TABLE {table} ADD COLUMN {col_name} TIMESTAMPTZ NOT NULL DEFAULT now();"
        ),
        note: None,
    }
}

pub fn partition_too_many_children(table: &str, count: usize) -> Entry {
    Entry {
        status: "lint".into(),
        reason: format!(
            "Partitioned table `{table}` has {count} child partitions. Planning time grows linearly; beyond ~100 partitions it becomes noticeable."
        ),
        fix: "-- Consider sub-partitioning or coarser partition boundaries to reduce the child count.".into(),
        note: None,
    }
}

pub fn partition_range_gap(parent: &str, from_bound: &str, to_bound: &str) -> Entry {
    Entry {
        status: "lint".into(),
        reason: format!(
            "Range partition `{parent}` has a gap between `{from_bound}` and `{to_bound}`. Inserts into the gap will fail unless a DEFAULT partition exists."
        ),
        fix: format!(
            "-- Create a partition covering the gap:\n\
             CREATE TABLE {parent}_fill PARTITION OF {parent}\n\
             \x20 FOR VALUES FROM ('{from_bound}') TO ('{to_bound}');"
        ),
        note: None,
    }
}

pub fn partition_no_default(parent: &str) -> Entry {
    Entry {
        status: "lint".into(),
        reason: format!(
            "Partitioned table `{parent}` has no DEFAULT partition. Rows that don't match any partition boundary will be rejected."
        ),
        fix: format!("CREATE TABLE {parent}_default PARTITION OF {parent} DEFAULT;"),
        note: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_schema_with_dot() {
        assert_eq!(strip_schema("public.users"), "users");
    }

    #[test]
    fn test_strip_schema_without_dot() {
        assert_eq!(strip_schema("users"), "users");
    }

    #[test]
    fn test_strip_schema_multiple_dots() {
        assert_eq!(strip_schema("my_db.public.users"), "users");
    }

    #[test]
    fn test_entry_display_without_note() {
        let e = alter_column_type("orders", "total", "NUMERIC(12,2)");
        let s = e.to_string();
        assert!(s.starts_with("STATUS: unsafe"));
        assert!(s.contains("REASON:"));
        assert!(s.contains("FIX:"));
        assert!(!s.contains("NOTE:"));
    }

    #[test]
    fn test_entry_display_with_note() {
        let e = add_column_volatile_default("events", "ts", "TIMESTAMPTZ", "clock_timestamp()");
        let s = e.to_string();
        assert!(s.contains("NOTE:"));
    }

    #[test]
    fn test_set_not_null_pg12_plus() {
        let e = set_not_null("users", "email", 14);
        assert_eq!(e.status, "safe-with-pattern");
        assert!(e.fix.contains("VALIDATE CONSTRAINT"));
    }

    #[test]
    fn test_set_not_null_pre_pg12() {
        let e = set_not_null("users", "email", 11);
        assert_eq!(e.status, "unsafe");
        assert!(e.fix.contains("keep the CHECK constraint"));
    }

    #[test]
    fn test_create_index_blocking_fix_is_concurrent() {
        let e = create_index_blocking("orders", "idx_orders_user", "btree", "user_id");
        assert!(e.fix.contains("CONCURRENTLY"));
    }

    #[test]
    fn test_entry_serialization_skips_none_note() {
        let e = rename("old_tbl", "new_tbl");
        let json = serde_json::to_string(&e).unwrap();
        assert!(!json.contains("note"));
    }

    #[test]
    fn test_entry_serialization_includes_some_note() {
        let e = suggest_gin("docs", "metadata", "JSONB");
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains("\"note\""));
    }
}
