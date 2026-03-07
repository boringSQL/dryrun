---
title: CREATE INDEX Safety
keywords: create index, index concurrently
min_pg_version: 12
safety: caution
---

## CREATE INDEX

### Quick Decision

- **`CREATE INDEX CONCURRENTLY`?** → Safe. Does not block reads or writes.
- **`CREATE INDEX` (without CONCURRENTLY)?** → Dangerous on large tables. Blocks writes for the entire build.
- **`CREATE UNIQUE INDEX CONCURRENTLY`?** → Safe, but may fail if duplicates exist.

### Lock Behavior

| Command | Lock | Blocks Writes? | Blocks Reads? |
|---------|------|---------------|---------------|
| `CREATE INDEX` | SHARE lock | Yes | No |
| `CREATE INDEX CONCURRENTLY` | SHARE UPDATE EXCLUSIVE | No | No |

That SHARE lock on a regular `CREATE INDEX` blocks all INSERT, UPDATE, and DELETE on the table. For a large table, the index build can take minutes or longer. Your app is effectively read-only during that time.

### How CONCURRENTLY Works

`CREATE INDEX CONCURRENTLY` builds the index in three phases:
1. Scan the table, build index entries (concurrent with writes, so your app keeps working)
2. Validate entries against any concurrent changes that happened during phase 1
3. Mark the index as valid

**Tradeoffs:**
- Takes ~2-3x longer than a regular `CREATE INDEX`
- Cannot run inside a transaction block
- Two table scans required instead of one

### When CONCURRENTLY Fails

Here's something that bites people: **if `CREATE INDEX CONCURRENTLY` fails partway through, it leaves behind an INVALID index.** Postgres won't use it for queries, but it still exists.

You **must** drop it before retrying:

```sql
-- Check for invalid indexes
SELECT indexrelid::regclass, indisvalid
FROM pg_index WHERE NOT indisvalid;

-- Drop the broken one
DROP INDEX CONCURRENTLY idx_orders_customer_id;

-- Then try again
CREATE INDEX CONCURRENTLY idx_orders_customer_id ON orders(customer_id);
```

Don't just re-run the `CREATE INDEX CONCURRENTLY` without dropping first. You'll get a "relation already exists" error. Or worse, you name it something slightly different and now you have an invalid index hanging around, wasting space and slowing down writes.

### Rebuilding Bloated Indexes

Over time, indexes accumulate bloat (dead tuples, fragmentation). Before PG 12, rebuilding meant `DROP` + `CREATE`, which was painful.

PG 12 introduced `REINDEX CONCURRENTLY`, and it's great:

```sql
-- Rebuild a bloated index without blocking queries (PG 12+)
REINDEX INDEX CONCURRENTLY idx_orders_customer_id;
```

This builds a new index alongside the old one, then swaps them atomically. No downtime. In real-world cases, the results can be dramatic. We've seen indexes shrink from 2.2 MB to 456 KB. That's 79% reduction, which makes a real difference for buffer cache efficiency.

### Version-Specific Notes

| PG Version | Notable Behavior |
|------------|-----------------|
| PG 14 | `REINDEX CONCURRENTLY` improvements |
| PG 13 | B-tree deduplication (smaller indexes by default) |
| PG 12 | `REINDEX CONCURRENTLY` introduced |

### Safe Pattern

```sql
-- Always use CONCURRENTLY for production tables
CREATE INDEX CONCURRENTLY idx_orders_customer_id ON orders(customer_id);

-- Check for invalid indexes after
SELECT indexrelid::regclass, indisvalid
FROM pg_index WHERE NOT indisvalid;
```

**Always check for invalid indexes after running CONCURRENTLY.**

### Dangerous Pattern

```sql
-- Blocks writes on the table for the entire index build
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
```

### Check Before Running

```sql
-- Estimate index build time (rough: ~1 minute per GB of table)
SELECT pg_size_pretty(pg_total_relation_size('your_table'));

-- Check for duplicate values if creating UNIQUE index
SELECT your_column, count(*)
FROM your_table
GROUP BY your_column
HAVING count(*) > 1
LIMIT 10;
```

If duplicates exist and you're creating a UNIQUE index, `CONCURRENTLY` will fail, and you'll have an invalid index to clean up. Check first.
