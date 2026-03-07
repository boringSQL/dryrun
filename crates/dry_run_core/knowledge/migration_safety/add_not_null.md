---
title: ADD NOT NULL Safety
keywords: add not null, set not null, alter column set not null, not null
min_pg_version: 12
safety: caution
---

## ADD NOT NULL Constraint

### Quick Decision

- **PG 12+: Add CHECK constraint first, then SET NOT NULL?** → Safe. PG 12+ skips the scan if a valid CHECK exists.
- **Direct `SET NOT NULL`?** → Dangerous on large tables (PG 11 and below always scans; PG 12+ scans unless a valid CHECK exists).

### Version Behavior

| PG Version | Behavior of `SET NOT NULL` |
|------------|---------------------------|
| PG 12–18 | Skips scan if a valid `CHECK (col IS NOT NULL)` constraint exists |
| PG 11 and below | Always scans entire table under ACCESS EXCLUSIVE |

### The Trick (PG 12+)

Here's the clever bit. PG 12+ is smart enough to look at your existing CHECK constraints. If it finds a validated `CHECK (col IS NOT NULL)`, it says: "Someone already proved no NULLs exist. I'll just flip the catalog flag."

So you give it what it wants:

```sql
-- Step 1: Add a CHECK constraint NOT VALID (brief lock)
ALTER TABLE orders ADD CONSTRAINT chk_status_not_null
    CHECK (status IS NOT NULL) NOT VALID;

-- Step 2: Validate the constraint (weaker lock, concurrent DML allowed)
ALTER TABLE orders VALIDATE CONSTRAINT chk_status_not_null;

-- Step 3: Set NOT NULL (PG 12+ sees the valid CHECK and skips the scan)
ALTER TABLE orders ALTER COLUMN status SET NOT NULL;

-- Step 4: Optionally drop the now-redundant CHECK
ALTER TABLE orders DROP CONSTRAINT chk_status_not_null;
```

Four steps instead of one. But Step 3 is instant, just a catalog update.

### The Dangerous Way

```sql
-- Scans entire table under ACCESS EXCLUSIVE (may block for minutes on large tables)
ALTER TABLE orders ALTER COLUMN status SET NOT NULL;
```

Looks innocent, doesn't it? One line, clean, simple. But behind the scenes, Postgres acquires ACCESS EXCLUSIVE and scans every single row to make sure there are no NULLs. On a 100M-row table, you could be blocking all queries for minutes.

### Why This Matters

The lock is the problem. ACCESS EXCLUSIVE means no reads, no writes, nothing. Every query on that table queues up behind your `SET NOT NULL`. And it won't let go until it has checked every row.

On PG 11 and below, there's no shortcut; you always pay this price. On PG 12+, the CHECK trick gives you an escape hatch.

### Check Before Running

```sql
-- Verify no NULL values exist
SELECT count(*) FROM your_table WHERE your_column IS NULL;

-- Check table size
SELECT pg_size_pretty(pg_total_relation_size('your_table'));

-- Check PG version (the safe pattern only works on PG 12+)
SELECT version();
```

If you find NULLs, you'll need to backfill them before the `VALIDATE` step will succeed. Do that in batches. Don't `UPDATE ... SET col = default WHERE col IS NULL` on a 200M-row table in one go.
