---
title: Common Table Expressions
keywords: cte, common table expression, with, materialized, not materialized, recursive, writable cte, optimization fence, inline, inlining
min_pg_version: 12
safety: info
---

## CTEs Are Not Optimization Fences Anymore

Before PostgreSQL 12, every CTE was materialized — the planner stored the result in a temporary tuplestore regardless of complexity, preventing predicate pushdown and index usage. That era is over. Since PG 12, non-recursive, side-effect-free CTEs referenced once are automatically inlined, optimized identically to regular subqueries.

If someone tells you "CTEs are slow," they're running pre-12 advice. The planner is smarter now.

## When Does a CTE Get Materialized?

**Inlined (optimized like subqueries):**
- Single reference, pure SELECT
- STABLE functions like `now()` (contrary to common misconception)

**Materialized (stored in a tuplestore):**
- Referenced more than once (computed once, reused)
- Contains VOLATILE functions (`random()`, `nextval()`, `clock_timestamp()`)
- Recursive CTEs
- Data-modifying operations (INSERT/UPDATE/DELETE with RETURNING)
- Row-locking clauses (FOR UPDATE, FOR SHARE)

You can override the planner's decision:
- `MATERIALIZED` forces materialization
- `NOT MATERIALIZED` forces inlining

Use these deliberately, not as cargo cult.

## The Statistics Problem

This is the real cost of materialization that nobody talks about. Materialized CTEs have **no statistics**. None. The planner guesses row counts using hardcoded defaults — 0.3333 selectivity for range comparisons, for example. That cascades into bad join order decisions and wrong memory estimates.

A materialized CTE over 100,000 rows with a range predicate might estimate 5,290 rows when the actual distribution is completely different. The inlined equivalent reads real histograms from `pg_statistic`. PG 17 improved this with column statistics propagation, but inlined CTEs remain strictly better for estimation accuracy.

## Bad Pattern: Aggregate Then Filter

This is the most common CTE misuse. You aggregate everything, then filter the result:

```sql
-- BAD: aggregates all orders, then throws most away
WITH order_metadata AS (
    SELECT
        o.id,
        bool_or(oa.id IS NOT NULL) AS was_archived,
        count(o2.id) AS related_count
    FROM orders o
    LEFT JOIN orders_archive oa ON o.id = oa.id
    LEFT JOIN orders o2 ON o.customer_id = o2.customer_id
                       AND o2.id != o.id
    GROUP BY o.id
)
SELECT * FROM order_metadata
WHERE was_archived = false AND related_count > 0;
```

The `GROUP BY` acts as an optimization barrier. The planner cannot push filters past aggregation, so it joins and aggregates every row before discarding most of them.

```sql
-- GOOD: filter first with EXISTS, short-circuit early
SELECT o.*
FROM orders o
WHERE o.created_at > '2024-01-01'
  AND NOT EXISTS (
    SELECT 1 FROM orders_archive oa WHERE oa.id = o.id
  )
  AND EXISTS (
    SELECT 1 FROM orders o2
    WHERE o2.customer_id = o.customer_id AND o2.id != o.id
  );
```

`EXISTS` short-circuits after the first matching row, allows filter pushdown, and enables early termination. The planner can use indexes on both subqueries.

## When Materialization Actually Helps

Don't avoid materialization dogmatically. It's the right call when:

1. **Multiple references** — computing an expensive aggregation once and reusing it beats recalculating per reference
2. **Expensive VOLATILE functions** — materialization ensures the function executes once with consistent results
3. **Data-modifying operations** — atomicity depends on it

If you're referencing the same CTE three times and it does a heavy aggregation, materialization is saving you work. The problem is when it happens by accident on something that should have been inlined.

## Writable CTEs: Powerful but Subtle

Data-modifying CTEs let you do atomic multi-statement operations in a single query:

```sql
WITH deleted AS (
    DELETE FROM orders
    WHERE status = 'cancelled' AND created_at < '2023-01-01'
    RETURNING *
),
archived AS (
    INSERT INTO orders_archive
    SELECT * FROM deleted
    RETURNING id
)
SELECT count(*) FROM archived;
```

The critical trap: **all sub-statements see the same snapshot**. Modifications in one CTE are NOT visible to reads of the same table in another — only through `RETURNING` data:

```sql
WITH ins AS (
    INSERT INTO orders (customer_id, amount, status, created_at)
    VALUES (1, 100.00, 'pending', CURRENT_DATE)
    RETURNING id
)
-- This returns the PRE-INSERT count, not including the new row
SELECT count(*) FROM orders WHERE customer_id = 1;
```

Also worth knowing: data-modifying CTEs disable parallel query for the entire statement.

## Recursive CTEs

Recursive CTEs use an iterative working-table mechanism, not actual recursion:

1. Execute the non-recursive seed term
2. Execute the recursive term against the working table
3. Repeat until no new rows
4. Union all iterations

**UNION vs UNION ALL matters here.** `UNION ALL` is faster but dangerous in cyclic graphs — infinite loop territory. `UNION` deduplicates at each iteration, preventing that. PG 14 added SQL-standard `SEARCH` and `CYCLE` clauses for cleaner cycle detection.

## Partition Pruning and CTEs

A materialized CTE over a partitioned table scans **all partitions** to build the result, even when predicates would normally eliminate most of them. If you see an Append node touching every partition inside a CTE, add `NOT MATERIALIZED` to preserve pruning. Or better yet, restructure so the CTE isn't needed.

## CTE vs Alternatives Decision Tree

| Scenario | Use | Why |
|----------|-----|-----|
| Readability, single reference | CTE (auto-inlined, zero cost) | Planner handles it |
| Compute once, reuse multiple times (small result) | CTE MATERIALIZED | Avoids recomputation |
| Compute once, reuse multiple times (large result) | Temporary table | Gets indexes and statistics |
| Atomic multi-statement DML | Writable CTE | No alternative for single-statement atomicity |
| Hierarchy / graph traversal | Recursive CTE | Only pure-SQL option |
| Need indexes on intermediate data | Temporary table | Planner has full statistics |

## Diagnostic Tips

- `EXPLAIN (VERBOSE)` shows whether a CTE was inlined or materialized — look for `CTE Scan` nodes
- In PG 18+, `EXPLAIN ANALYZE` reports memory and disk usage for CTE materialization
- If a materialized CTE exceeds `work_mem`, it silently spills to disk. Monitor with `log_temp_files = 0`
- Watch for generic plans in prepared statements — they may inline CTEs differently than custom plans, causing unexpected plan shifts after 5+ executions
