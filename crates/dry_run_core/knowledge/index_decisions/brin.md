---
title: BRIN Index Decision
keywords: brin, block range, time series, append only, correlation, sequential
min_pg_version: 12
safety: safe
---

## BRIN (Block Range Index): The Tiny Index for Ordered Data

BRIN is the index that barely exists. Instead of tracking every single row, it stores just the min/max values per block range. The result? An index that's orders of magnitude smaller than B-tree, but only useful if your data plays nice.

### When to Use

- Time-series tables where rows are inserted in order: `WHERE created_at > '2024-01-01'`
- Append-only tables with high physical correlation between column value and row position
- Very large tables where B-tree index size is becoming a problem

### When NOT to Use

- Tables with random inserts/updates (low correlation) → B-tree
- Point lookups (`WHERE id = 42`) → B-tree. BRIN can't tell you which row, only which block range. Too imprecise.
- Small tables → the overhead isn't worth it, just use B-tree

### The Key Concept: Correlation

BRIN works because adjacent rows on disk have similar values. If your `created_at` column always increases as you insert rows, the physical order on disk matches the logical order. BRIN exploits this.

But if you update rows, delete and reinsert, or load data out of order, that correlation falls apart and BRIN becomes useless.

Check correlation:

```sql
SELECT attname, correlation
FROM pg_stats
WHERE tablename = 'your_table' AND attname = 'your_column';
```

- `correlation` near **1.0 or -1.0** → BRIN is effective
- `correlation` near **0.0** → BRIN is useless, don't bother

### The Size Advantage Is Absurd

BRIN indexes are comically small compared to B-tree:
- 1 billion row table: B-tree ~20 GB, BRIN ~1 MB

That's not a typo. The trade-off is precision: BRIN might say "your rows are somewhere in these 128 pages", and PostgreSQL has to scan all of them. But when the alternative is a 20 GB index that doesn't even fit in memory, BRIN starts looking real attractive.

### Parameters

| Parameter | Default | Effect |
|-----------|---------|--------|
| `pages_per_range` | 128 | Lower = more granular but larger index |
| `autosummarize` | on (PG 14+) | Auto-summarize new block ranges |

Lower `pages_per_range` gives you better precision at the cost of a bigger (but still tiny) index. For most use cases, the default of 128 is fine.

### Check Queries

```sql
-- Check correlation (must be close to ±1.0 for BRIN)
SELECT attname, correlation, n_distinct
FROM pg_stats
WHERE tablename = 'events' AND attname = 'created_at';

-- Compare BRIN vs B-tree size estimate
SELECT pg_size_pretty(pg_total_relation_size('your_table'));
```

### Example

```sql
-- Time-series with high correlation
CREATE INDEX CONCURRENTLY idx_events_created_brin ON events USING brin(created_at);

-- Custom pages_per_range for finer granularity
CREATE INDEX CONCURRENTLY idx_events_brin_fine ON events
    USING brin(created_at) WITH (pages_per_range = 32);
```
