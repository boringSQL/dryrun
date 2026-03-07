---
title: ALTER COLUMN TYPE Safety
keywords: alter column type, alter table alter column type, set data type, change type, using, cast, int to bigint, varchar to text, json to jsonb, enum, citext
min_pg_version: 12
safety: dangerous
---

## ALTER COLUMN TYPE

### Quick Decision

- **Most type changes** → Dangerous. Full table rewrite under ACCESS EXCLUSIVE lock.
- **Some safe casts** → Metadata-only. See the safe list below.

### Safe Type Changes (No Rewrite)

Not all type changes are created equal. Some are just catalog updates, because the on-disk format doesn't actually change:

| From | To | Safe? | Why |
|------|----|-------|-----|
| `varchar(N)` | `varchar(M)` where M > N | Yes | Just loosening a constraint |
| `varchar(N)` | `text` | Yes | Removing a constraint, same storage |
| `varchar(N)` | `varchar(M)` where M < N | **No** | Needs to verify every row fits |
| `numeric(P,S)` | `numeric(P2,S)` where P2 > P | Yes | Widening precision |
| `numeric(P,S)` | `numeric(P2,S)` where P2 < P | **No** | Needs to verify and possibly truncate |
| `numeric` (unconstrained) | `numeric(P,S)` | **No** | Needs to verify all values fit |
| `int` | `bigint` | **No** | 4 bytes vs 8 bytes, full rewrite |
| `bigint` | `int` | **No** | Narrowing, full rewrite + possible overflow |
| `smallint` | `int` | **No** | 2 bytes vs 4 bytes, still a rewrite |
| `timestamp` | `timestamptz` | **No** | Values change (timezone adjustment) |
| `timestamptz` | `timestamp` | **No** | Same, values get truncated |
| `json` | `jsonb` | **No** | Completely different on-disk format |
| `text` | `citext` | **No** | Different type OID, rewrite |
| `citext` | `text` | **No** | Different type OID, rewrite |

The pattern: widening a constraint (bigger varchar, more numeric precision) is safe because the on-disk bytes don't change. Changing the actual storage format, even if the values are "compatible," is not.

`int` to `bigint` seems like just "bigger number," but on disk it's 4 bytes vs 8 bytes. Every row gets rewritten. And `timestamp` to `timestamptz`? Postgres needs to adjust values based on the session timezone, so it can't just relabel them.

### The USING Clause

When you change types, PostgreSQL needs to know how to convert existing values. By default it tries an implicit cast. If that doesn't exist, you need `USING`:

```sql
-- text column that holds integers, convert to actual int
ALTER TABLE events ALTER COLUMN priority TYPE integer USING priority::integer;

-- extract a field from jsonb into a typed column
ALTER TABLE orders ALTER COLUMN total TYPE numeric(12,2)
    USING (metadata->>'total')::numeric(12,2);

-- timestamp to timestamptz with explicit timezone
ALTER TABLE logs ALTER COLUMN created_at TYPE timestamptz
    USING created_at AT TIME ZONE 'UTC';
```

The `USING` expression runs for every row, so this is always a full table rewrite. But it's essential when there's no implicit cast, or when the default cast would do the wrong thing (like `timestamp` to `timestamptz` using the session timezone instead of UTC).

**Common trap:** forgetting `USING` when going from `text` to `integer`. If any row contains non-numeric text, the whole ALTER fails partway through and rolls back. Check your data first:

```sql
SELECT count(*) FROM events WHERE priority !~ '^\d+$';
```

### What Breaks When You Change a Type

Changing a column type doesn't just rewrite the table. It cascades through dependent objects:

**Indexes** on the column get automatically rebuilt. This adds to the rewrite time and lock duration. A table with 5 indexes on the changed column means 5 index rebuilds while holding ACCESS EXCLUSIVE.

**Views** that reference the column will break if the new type is incompatible with the view's output. PostgreSQL won't let you change the type if a view depends on it, unless you use `CASCADE`.

**Defaults** that are incompatible with the new type get dropped. If you had `DEFAULT 'pending'` on a column you're changing to `integer`, that default silently disappears.

**Foreign keys** referencing or referenced by the column will block the type change. You need to drop the FK, change the type on both sides, and re-add the FK.

**Generated columns** and **expression indexes** that reference the column will also block the change.

```sql
-- Check what depends on this column before you touch it
SELECT
    d.classid::regclass AS object_type,
    pg_describe_object(d.classid, d.objid, d.objsubid) AS dependent_object
FROM pg_depend d
JOIN pg_attribute a ON d.refobjid = a.attrelid AND d.refobjsubid = a.attnum
WHERE a.attrelid = 'your_table'::regclass
  AND a.attname = 'your_column';
```

### Dangerous: Full Table Rewrite

```sql
-- DANGEROUS: rewrites entire table, ACCESS EXCLUSIVE for duration
ALTER TABLE users ALTER COLUMN id TYPE bigint;
```

On a 200M-row table, this locks you out for potentially hours. Your application is dead in the water.

### Safe Alternative: The Expand-Then-Swap

For `int` → `bigint` on large tables, you don't have to suffer. Use this pattern:

```sql
-- 1. Add new column (metadata-only, instant)
ALTER TABLE users ADD COLUMN id_new bigint;

-- 2. Backfill in batches (no long locks)
UPDATE users SET id_new = id WHERE id_new IS NULL AND id BETWEEN ... AND ...;

-- 3. Add constraints
ALTER TABLE users ADD CONSTRAINT chk_id_new_not_null CHECK (id_new IS NOT NULL) NOT VALID;
ALTER TABLE users VALIDATE CONSTRAINT chk_id_new_not_null;

-- 4. Swap in a transaction (brief lock)
BEGIN;
ALTER TABLE users DROP CONSTRAINT chk_id_new_not_null;
ALTER TABLE users RENAME COLUMN id TO id_old;
ALTER TABLE users RENAME COLUMN id_new TO id;
-- Update sequences, FKs, etc.
COMMIT;

-- 5. Drop old column later
ALTER TABLE users DROP COLUMN id_old;
```

More steps, yes. But your app stays up the whole time. Step 4 is the only moment you hold ACCESS EXCLUSIVE, and it's just catalog updates, milliseconds at most.

This pattern works for any type change, not just `int` to `bigint`. JSON to JSONB, `text` to `citext`, `timestamp` to `timestamptz` with explicit timezone handling. The principle is the same: add new column, backfill, swap.

### JSON to JSONB

This is a common one. `json` and `jsonb` have completely different on-disk formats (`json` stores the raw text, `jsonb` stores a parsed binary representation), so a direct `ALTER COLUMN TYPE jsonb` triggers a full rewrite.

For small tables, just do it:

```sql
ALTER TABLE config ALTER COLUMN data TYPE jsonb USING data::jsonb;
```

For large tables, use the expand-then-swap pattern. One extra thing to watch: if you have GIN indexes on the `json` column, they need to be recreated for `jsonb` (different operator class).

### Enum Type Modifications

Enums have their own rules. Adding a new value is relatively safe, but the details matter:

```sql
-- Safe: appends to the end of the enum (PG 9.1+)
ALTER TYPE order_status ADD VALUE 'cancelled';

-- Safe: insert at specific position (PG 9.1+)
ALTER TYPE order_status ADD VALUE 'processing' BEFORE 'shipped';

-- IMPORTANT: ADD VALUE cannot run inside a transaction block (PG < 12)
-- PG 12+ allows it inside a transaction, but the value isn't visible
-- until the transaction commits

-- Renaming a value (PG 10+)
ALTER TYPE order_status RENAME VALUE 'pending' TO 'awaiting_payment';
```

Removing enum values is only possible in PG 14+ and requires that no row, default, or function references the value:

```sql
-- PG 14+ only
ALTER TYPE order_status DROP VALUE 'obsolete';
-- Fails if any row still uses this value
```

Before PG 14, removing an enum value means recreating the entire type. This is painful: create new type, add new column, backfill, swap, drop old. If you find yourself doing this often, consider a lookup table instead of an enum.

### When a Direct Rewrite Is Acceptable

Not every table needs the expand-then-swap dance. For small tables (under ~100MB), the rewrite finishes in seconds and the lock is brief. The question is always: how long will my application be blocked?

Rough guidelines for direct `ALTER COLUMN TYPE`:
- **Under 100 MB**: Just do it. The lock is subsecond to a few seconds.
- **100 MB to 1 GB**: Risky during peak traffic. Schedule it during a low-traffic window, or use a brief maintenance page.
- **Over 1 GB**: Use the expand-then-swap pattern. No exceptions.

These numbers assume decent hardware and no other long-running transactions holding conflicting locks. If you have a transaction sitting open for hours (a common culprit: an idle `pgAdmin` session with `BEGIN` and no `COMMIT`), even a small table rewrite will hang waiting for that lock.

```sql
-- Check for long-running transactions that would block your ALTER
SELECT pid, state, age(now(), xact_start) AS duration, query
FROM pg_stat_activity
WHERE state != 'idle'
  AND xact_start IS NOT NULL
ORDER BY xact_start;
```

### Batched Rollout with Expand-Then-Swap

The expand-then-swap pattern shown above works, but the backfill step needs care on large tables. A single `UPDATE ... WHERE id_new IS NULL` on 500M rows will generate massive WAL, bloat the table, and possibly run out of disk.

Break it into batches:

```sql
-- Backfill in chunks of 10,000 rows
-- Adjust batch size based on your table's row width and available I/O
DO $$
DECLARE
    batch_size int := 10000;
    min_id bigint;
    max_id bigint;
    current_id bigint;
BEGIN
    SELECT min(id), max(id) INTO min_id, max_id FROM users;
    current_id := min_id;

    WHILE current_id <= max_id LOOP
        UPDATE users
        SET id_new = id
        WHERE id BETWEEN current_id AND current_id + batch_size - 1
          AND id_new IS NULL;

        current_id := current_id + batch_size;
        COMMIT;
    END LOOP;
END $$;
```

A few things to keep in mind:

**Batch size matters.** Too small and you're spending all your time on transaction overhead. Too large and you generate big WAL segments and hold row locks longer. 10,000 to 50,000 rows per batch is a reasonable starting point, but test on your actual data.

**Run VACUUM between batches on very large tables.** The backfill creates dead tuples (old row versions). If you're updating 500M rows in 10K batches, that's 500M dead tuples that autovacuum may not keep up with. Explicit `VACUUM` calls every N batches help.

**Monitor replication lag.** If you have replicas, each batch generates WAL. Large backfills can cause replicas to fall behind. Watch `pg_stat_replication` and throttle if needed.

**Handle the gap between backfill and swap.** New rows inserted after your backfill starts won't have `id_new` set. You need a trigger or application-level logic to keep the new column in sync during the migration window:

```sql
-- Keep new column in sync during migration
CREATE OR REPLACE FUNCTION sync_id_new()
RETURNS TRIGGER AS $$
BEGIN
    NEW.id_new := NEW.id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sync_id_new
    BEFORE INSERT OR UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION sync_id_new();

-- Drop this trigger after the swap is complete
```

**The full sequence for a production rollout:**

1. Add new column (instant, metadata-only)
2. Add sync trigger (so new writes populate both columns)
3. Backfill existing rows in batches
4. Add NOT NULL constraint via CHECK (NOT VALID, then VALIDATE)
5. Swap columns in a transaction (brief ACCESS EXCLUSIVE)
6. Drop sync trigger
7. Drop old column
8. Update any sequences, FKs, indexes pointing to the old column

Steps 1-4 can happen over days if needed. The actual downtime is only step 5, which takes milliseconds.

### Lock Duration

ACCESS EXCLUSIVE lock held for the entire rewrite. For large tables this can be minutes to hours. All queries, reads and writes, are blocked. There is no "just wait a bit" when you have million of users hitting the table.

### Check Before Running

```sql
-- Check table size (rewrite time roughly proportional)
SELECT pg_size_pretty(pg_total_relation_size('your_table'));

-- Check dependent objects (views, indexes, FKs, generated columns)
SELECT
    d.classid::regclass AS object_type,
    pg_describe_object(d.classid, d.objid, d.objsubid) AS dependent_object
FROM pg_depend d
JOIN pg_attribute a ON d.refobjid = a.attrelid AND d.refobjsubid = a.attnum
WHERE a.attrelid = 'your_table'::regclass
  AND a.attname = 'your_column';

-- For text-to-integer conversions, check for non-castable values
SELECT count(*) FROM your_table WHERE your_column !~ '^\d+$';

-- For narrowing changes (bigint to int), check for overflow
SELECT count(*) FROM your_table
WHERE your_column > 2147483647 OR your_column < -2147483648;
```

If the table is over a few GB, don't even think about a direct type change in production. Use the expand-then-swap pattern.
