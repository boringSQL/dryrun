---
title: Composite Index Decision
keywords: composite index, multi-column, column order, compound index
min_pg_version: 12
safety: safe
---

## Composite (Multi-Column) Index: Order Is Everything

A single B-tree index on two or more columns. But here's the thing: **column order matters more than almost any other indexing decision you'll make.**

Get it right, and one index serves multiple query patterns. Get it wrong, and PostgreSQL ignores your carefully crafted index entirely.

### When to Use

- Queries filter on multiple columns: `WHERE status = 'active' AND customer_id = 42`
- Queries combine equality + range: `WHERE customer_id = 42 AND created_at > '2024-01-01'`
- Queries filter + sort: `WHERE status = 'pending' ORDER BY created_at`

### Column Order Rules

This is the part you can't afford to get wrong. The B-tree is sorted left-to-right, and that dictates everything:

1. **Equality columns first**: Columns compared with `=` go leftmost
2. **Most selective equality column first** (among equals): Reduces the search space fastest
3. **Range/sort column last**: `>`, `<`, `BETWEEN`, `ORDER BY` go rightmost
4. **Only the leftmost prefix is usable**: `(a, b, c)` supports `WHERE a = ?` and `WHERE a = ? AND b = ?` but NOT `WHERE b = ?` alone

That last rule trips people up constantly. You create an index on `(status, customer_id, created_at)` and then wonder why your `WHERE customer_id = 42` query doesn't use it. Well, now you know.

### Examples

```sql
-- Query: WHERE status = 'pending' AND customer_id = 42 ORDER BY created_at DESC
-- Optimal: equality columns first, sort column last
CREATE INDEX ON orders(status, customer_id, created_at DESC);

-- Query: WHERE customer_id = 42 AND created_at > '2024-01-01'
-- Optimal: equality first, range second
CREATE INDEX ON orders(customer_id, created_at);

-- WRONG order for the same query:
CREATE INDEX ON orders(created_at, customer_id);
-- ↑ This can only use the index for created_at range, then filter customer_id
```

### vs Multiple Single-Column Indexes

But wait, you say, can't PostgreSQL combine multiple indexes?

Yes, it can merge them via Bitmap Index Scan. But a composite index is still better:
- **Faster**: Single index lookup vs bitmap merge
- **More space efficient**: One index vs two
- **Required** for multi-column sort (bitmap scans can't produce ordered output)

### Decision Checklist

1. List the columns in your WHERE/ORDER BY
2. Separate into equality (`=`) and range/sort (`>`, `<`, `ORDER BY`)
3. Order: equality (most selective first) → range/sort
4. Check: does the query always use the leftmost column(s)?

### Check Queries

```sql
-- Check selectivity to determine column order
SELECT attname, n_distinct
FROM pg_stats
WHERE tablename = 'your_table' AND attname IN ('col_a', 'col_b')
ORDER BY n_distinct DESC;  -- higher absolute value = more selective

-- Check if existing indexes already cover this
SELECT indexdef FROM pg_indexes WHERE tablename = 'your_table';
```
