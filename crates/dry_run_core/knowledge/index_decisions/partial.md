---
title: Partial Index Decision
keywords: partial index, where clause index, filtered index, conditional index
min_pg_version: 12
safety: safe
---

## Partial Index: Index Only What Matters

An index with a WHERE clause. Instead of indexing every single row, you index just the ones you actually care about.

### When to Use

- Your queries always filter on a known condition: `WHERE status = 'active'`
- Skewed data: 99% of rows are 'completed', you only ever query 'pending' orders
- Unique constraint on a subset: "only one active email per user"

### Why It's So Good

1. **Smaller**: Only indexes matching rows → less disk, more of it fits in RAM
2. **Faster writes**: Inserts/updates to non-matching rows skip index maintenance entirely
3. **Faster reads**: Smaller index = faster lookups

You've got a 100 million row orders table, 95% are completed, and your app only queries pending ones? That's a partial index on `WHERE status = 'pending'`. Your index just went from millions of entries to thousands.

### Decision Checklist

1. Does your query always include a fixed predicate (e.g. `WHERE active = true`)?
2. Does the predicate filter out >50% of rows? (If not, a full index may be simpler)
3. Does the query predicate match the index predicate exactly? (Must match for planner to use it)

### Important: Predicate Matching

This is the part that bites you. PostgreSQL will only use a partial index if the query's WHERE clause **implies** the index predicate. And the match must be syntactically recognizable, because the planner doesn't do deep logical reasoning here:

```sql
-- Index predicate: WHERE status = 'active'
-- Query: WHERE status = 'active' AND age > 30   ← USES the index ✓
-- Query: WHERE status IN ('active', 'pending')   ← DOES NOT use the index ✗
```

That second query logically includes `status = 'active'`, but PostgreSQL can't figure that out from the syntax. The query predicate must be a superset of the index predicate in a way the planner can recognize.

### Check Queries

```sql
-- Check value distribution to see if partial index makes sense
SELECT status, count(*), round(100.0 * count(*) / sum(count(*)) over (), 1) as pct
FROM your_table
GROUP BY status
ORDER BY count(*) DESC;
```

### Example

```sql
-- Only index active orders (if 95% are completed)
CREATE INDEX CONCURRENTLY idx_orders_active
    ON orders(customer_id, created_at)
    WHERE status = 'pending';

-- Unique constraint on active rows only
CREATE UNIQUE INDEX CONCURRENTLY idx_users_active_email
    ON users(email)
    WHERE deleted_at IS NULL;
```
