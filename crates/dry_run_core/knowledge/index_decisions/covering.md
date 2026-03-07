---
title: Covering Index Decision (INCLUDE)
keywords: covering index, include, index only scan, heap lookup
min_pg_version: 11
safety: safe
---

## Covering Index (INCLUDE): Skip the Heap Trip

PG 11+ lets you store extra columns in the index leaf pages without making them part of the key. The payoff? Index-only scans. Your query gets everything it needs from the index and never touches the heap.

### When to Use

- Your SELECT list references columns not in the index key
- You see "Index Scan" in EXPLAIN but want "Index Only Scan"
- The extra columns are small (don't go stuffing TEXT blobs in there)

### How It Works

```sql
-- Without INCLUDE: Index Scan + heap lookup for 'name' and 'email'
CREATE INDEX idx_users_id ON users(id);
-- Query: SELECT name, email FROM users WHERE id = 42;
-- → Index Scan on idx_users_id + heap fetch

-- With INCLUDE: Index Only Scan (no heap lookup)
CREATE INDEX idx_users_id_covering ON users(id) INCLUDE (name, email);
-- → Index Only Scan on idx_users_id_covering
```

The difference can be dramatic. Instead of bouncing between index and heap for every row, PostgreSQL reads just the index. On large tables, this turns random I/O into a much tighter access pattern.

### INCLUDE vs Composite Key

| Approach | Key Columns | Use |
|----------|-------------|-----|
| `CREATE INDEX ON t(a, b)` | a, b | Both used for search + ordering |
| `CREATE INDEX ON t(a) INCLUDE (b)` | a | Only a for search; b is payload |

INCLUDE columns:
- **Cannot** be used for search or ordering
- Don't affect index key size limits
- Perfect for "tag-along" columns you just need in your SELECT

Think of it this way: key columns are for finding rows. INCLUDE columns are for avoiding the trip back to the table once you've found them.

### Decision Checklist

1. Does EXPLAIN show "Index Scan" when "Index Only Scan" would be faster?
2. Are the INCLUDE columns small (< a few hundred bytes total)?
3. Is the visibility map mostly up-to-date? (`VACUUM` must have run; check `n_dead_tup`)

### Caveat: The Visibility Map Gotcha

Here's what catches people off guard. Index-only scans only work if the visibility map says the page is all-visible. If many rows have been modified since the last VACUUM, PostgreSQL falls back to regular index scan + heap fetch anyway.

So you create this beautiful covering index, EXPLAIN shows "Index Only Scan" and you celebrate. Then in production, with constant writes, it's doing heap fetches on every other page. **Make sure autovacuum is keeping up.**

```sql
-- Check dead tuple ratio (high = VACUUM needed for index-only scans)
SELECT relname, n_dead_tup, n_live_tup,
       round(100.0 * n_dead_tup / nullif(n_live_tup + n_dead_tup, 0), 1) as dead_pct
FROM pg_stat_user_tables
WHERE relname = 'your_table';
```

### Example

```sql
-- Covering index for a common query pattern
-- Query: SELECT order_id, total FROM orders WHERE customer_id = ? ORDER BY created_at DESC
CREATE INDEX CONCURRENTLY idx_orders_cust_covering
    ON orders(customer_id, created_at DESC)
    INCLUDE (order_id, total);
```
