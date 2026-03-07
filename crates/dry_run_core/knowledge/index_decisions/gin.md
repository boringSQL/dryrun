---
title: GIN Index Decision
keywords: gin, jsonb, array, full-text, tsvector, containment, @>, ?
min_pg_version: 12
safety: safe
---

## GIN (Generalized Inverted Index): The Multi-Value Champion

GIN is your go-to for data where a single column holds multiple values: arrays, JSONB documents, full-text search vectors. It inverts the relationship: instead of "row → values", it builds "value → rows".

### When to Use

- JSONB containment: `WHERE data @> '{"key": "value"}'`
- JSONB key existence: `WHERE data ? 'key'`
- Array containment: `WHERE tags @> ARRAY['rust']`
- Full-text search: `WHERE tsv @@ to_tsquery('search')`
- Trigram similarity: `WHERE name % 'fuzzy'` (with `pg_trgm`)

### When NOT to Use

- Simple equality/range on scalar columns → B-tree is faster and cheaper
- Geometric/spatial data → GiST
- Very write-heavy tables with infrequent reads (GIN write cost will hurt)

### The `ANY` Trap

This one catches almost everyone. You write:

```sql
WHERE 'feature' = ANY(tags)
```

Looks like it should use GIN on `tags`, right? **It doesn't.** This is scalar equality in a loop. PostgreSQL evaluates `'feature' = tags[1]`, then `'feature' = tags[2]`, and so on. GIN index sits there unused.

Rewrite it as:

```sql
WHERE tags @> ARRAY['feature']
```

Same result, but now GIN can do its job. The `@>` operator is what GIN actually understands.

### Write Performance: The Real Cost

GIN indexes are expensive to maintain on writes, and here's why: **write multiplication**. One row with an array of 10 elements means 10 index entries. Insert a JSONB document with 50 keys? That's 50 index entries. Your insert throughput can tank if you're not expecting this.

By default, GIN uses `fastupdate = on`, which batches inserts into a pending list. This amortizes write cost, but there's a trade-off. **Reads must scan both the organized index and the messy pending list.** For read-heavy workloads, consider setting `fastupdate = off` so the index is always fully merged. You'll pay more on writes, but reads stay fast.

Run `gin_clean_pending_list('index_name')` during maintenance windows if you keep fastupdate on.

### Operator Classes

| Data Type | Operator Class | Supports |
|-----------|---------------|----------|
| `jsonb` | `jsonb_ops` (default) | `@>`, `?`, `?&`, `?\|` |
| `jsonb` | `jsonb_path_ops` | `@>` only, but smaller and faster |
| `tsvector` | `tsvector_ops` | `@@` |
| `anyarray` | `array_ops` | `@>`, `<@`, `&&`, `=` |
| `text` | `gin_trgm_ops` | `%`, `LIKE`, `ILIKE`, `~` (requires pg_trgm) |

### Check Queries

```sql
-- Check if you're querying JSONB containment
EXPLAIN (FORMAT TEXT) SELECT * FROM your_table WHERE data @> '{"status": "active"}';

-- Check GIN index size (tends to be larger than B-tree)
SELECT pg_size_pretty(pg_relation_size('your_index'));
```

### Example

```sql
-- JSONB containment
CREATE INDEX CONCURRENTLY idx_events_data ON events USING gin(data);

-- JSONB with smaller, faster path_ops
CREATE INDEX CONCURRENTLY idx_events_data_path ON events USING gin(data jsonb_path_ops);

-- Full-text search
CREATE INDEX CONCURRENTLY idx_articles_tsv ON articles USING gin(tsv);

-- Trigram for fuzzy search (requires pg_trgm extension)
CREATE INDEX CONCURRENTLY idx_users_name_trgm ON users USING gin(name gin_trgm_ops);
```
