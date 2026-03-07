---
title: B-tree Index Decision
keywords: btree, b-tree, index, equality, range, order by, sort
min_pg_version: 12
safety: safe
---

## B-tree Index: The Workhorse You Already Know

B-tree is the default. When you write `CREATE INDEX` without specifying a type, you get a B-tree. It handles equality (`=`), range (`<`, `>`, `BETWEEN`), `ORDER BY`, `IS NULL`, and prefix `LIKE 'foo%'`.

If your query is "give me rows where X equals something" or "sort by Y", B-tree is almost certainly the right call.

### When to Reach for B-tree

- Equality lookups: `WHERE id = 42`
- Range scans: `WHERE created_at > '2024-01-01'`
- Sorting: `ORDER BY created_at DESC`
- Uniqueness enforcement (automatically created for PK/UNIQUE constraints)

### When B-tree Won't Help You

- Full-text search → use GIN
- Array/JSONB containment → use GIN
- Geometric/range overlap queries → use GiST
- Very large tables with low-selectivity sequential access → consider BRIN
- Pattern matching `LIKE '%foo%'` → B-tree can't do this (but `LIKE 'foo%'` works fine)
- **Array columns** → B-tree on an array column only helps with whole-array equality. As in, `WHERE tags = ARRAY['rust', 'postgres']` exactly. That's almost never what you actually want. For real-world element queries like "find rows where tags contain 'rust'", you need GIN.

### Decision Checklist

1. **Selectivity**: Does the WHERE clause filter to <10-15% of rows? If not, the planner may just seq scan anyway. And honestly, it's probably right to.
2. **Write ratio**: High-write tables pay index maintenance cost on every INSERT/UPDATE/DELETE. Only index columns you actually query on.
3. **Column ordering** (composite): Put the most selective column first. Equality columns before range columns.
4. **Covering** (PG 11+): Use `INCLUDE` to avoid heap lookups for index-only scans.

### Version Notes

| PG Version | B-tree Feature |
|------------|---------------|
| PG 13 | Deduplication, which means significantly smaller indexes for low-cardinality columns |
| PG 12 | Improved space utilization |
| PG 11 | INCLUDE columns for covering indexes |

### Check Queries

```sql
-- Check column selectivity (n_distinct)
SELECT attname, n_distinct, correlation
FROM pg_stats
WHERE tablename = 'your_table' AND attname = 'your_column';

-- Check if an existing index already covers this query
SELECT indexdef FROM pg_indexes WHERE tablename = 'your_table';

-- Estimate index size
SELECT pg_size_pretty(
    pg_relation_size('your_table') *
    (SELECT avg_width FROM pg_stats WHERE tablename = 'your_table' AND attname = 'your_column')::numeric
    / (SELECT avg_width FROM pg_stats WHERE tablename = 'your_table' LIMIT 1)::numeric
);
```

### Example

```sql
-- Simple B-tree
CREATE INDEX CONCURRENTLY idx_orders_customer_id ON orders(customer_id);

-- Composite with optimal column order
CREATE INDEX CONCURRENTLY idx_orders_status_date ON orders(status, created_at);

-- Covering index (PG 11+)
CREATE INDEX CONCURRENTLY idx_orders_covering ON orders(customer_id) INCLUDE (status, total);
```
