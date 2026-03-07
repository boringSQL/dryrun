---
title: Array Conventions
keywords: array, gin, unnest, toast, intarray, junction table, gin index, array operators, lz4, mvcc
safety: info
---

## When Arrays Make Sense

Arrays in PostgreSQL are tempting. You see a one-to-many relationship and think: "Why create a whole junction table when I can just stuff the IDs into an array column?" The data stays local, queries look simpler, and you skip the join.

But that locality comes with a cost. Arrays are the right tool in specific situations, and a trap in others.

**Use arrays when the data shares the parent row's lifecycle.** Tags on a blog post. A list of permissions for a role. Phone numbers for a contact. Data that gets read and written together, always as a unit. If you'd never query "find all rows where this specific element appears" or need referential integrity on the elements, arrays work great.

**Don't use arrays for relationships across tables.** No foreign keys exist for array elements. No `ON DELETE CASCADE`. Deleting a referenced row leaves orphaned IDs permanently sitting in your arrays, silently pointing at nothing. If you need referential integrity, use junction tables.

## The Document Model Temptation

It's tempting to treat arrays like a mini-document store and keep everything together for read performance. And sometimes that's valid! But PostgreSQL isn't MongoDB. Its MVCC model means that modifying a single array element rewrites the entire row. Every element. Even the ones you didn't touch.

For small, rarely-modified arrays (say, a handful of tags), this is fine. For arrays that grow large or get frequent element-level updates, you're creating unnecessary write amplification.

## Indexing: GIN, Not B-tree

B-tree indexes don't help with array containment queries. You need GIN:

```sql
CREATE INDEX idx_article_tags ON article USING gin (tags);
```

GIN supports the `@>` (contains) and `&&` (overlap) operators:

```sql
-- Find articles with all of these tags
SELECT * FROM article WHERE tags @> ARRAY['postgresql', 'performance'];

-- Find articles with any of these tags
SELECT * FROM article WHERE tags && ARRAY['postgresql', 'rust'];
```

**Important:** `= ANY(column)` does NOT use GIN indexes. This is a common mistake. If you write `WHERE 'postgresql' = ANY(tags)`, PostgreSQL can't use the GIN index and will do a sequential scan. Rewrite it as `WHERE tags @> ARRAY['postgresql']` to get index support.

## Storage and MVCC

Every time you modify an array (append element, remove element, update one value) PostgreSQL creates a new version of the entire row. That's MVCC for you. The old version sticks around until VACUUM cleans it up.

For large arrays, this matters. A lot.

### TOAST Behavior

When an array exceeds roughly 2KB, PostgreSQL moves it to a separate TOAST table. This has implications:

- Reading TOASTed arrays requires an extra lookup
- Modifying the array means rewriting the entire TOAST entry
- But the upside: the main table's rows stay compact, which helps sequential scan performance

### LZ4 Compression (PG 14+)

Starting with PostgreSQL 14, you can use LZ4 compression for TOAST storage. It's significantly faster than the default pglz:

```sql
ALTER TABLE article ALTER COLUMN tags SET STORAGE EXTENDED;
ALTER TABLE article ALTER COLUMN tags SET COMPRESSION lz4;
```

Worth doing if you have large arrays and you're on PG 14+. The compression ratio is similar, but LZ4 is much faster at both compression and decompression.

## Bulk Loading with unnest

Need to insert array data from a normalized source? Or expand arrays for analysis? `unnest` is your friend:

```sql
-- Expand array elements into rows
SELECT id, unnest(tags) AS tag FROM article;

-- Bulk insert using unnest (much faster than row-by-row)
INSERT INTO article (title, tags)
SELECT title, ARRAY(SELECT tag FROM temp_tags WHERE temp_tags.article_id = source.id)
FROM source;
```

For the reverse, aggregating rows into an array, use `array_agg`:

```sql
SELECT article_id, array_agg(tag ORDER BY tag) AS tags
FROM article_tag
GROUP BY article_id;
```

## The intarray Extension

If you're working with integer arrays specifically (and you often are, for ID lists), the `intarray` extension is worth enabling:

```sql
CREATE EXTENSION IF NOT EXISTS intarray;
```

It gives you optimized operators for integer arrays: sorting, deduplication, union, intersection. The GIN operator class `gin__int_ops` is also faster than the default for integer arrays.

```sql
CREATE INDEX idx_article_tag_ids ON article USING gin (tag_ids gin__int_ops);
```

## The Practical Guideline

Ask yourself two questions:

1. **Does this data share the parent row's lifecycle?** If yes, arrays might be right.
2. **Do I need referential integrity on individual elements?** If yes, use a junction table.

Arrays are great for tags, labels, small config lists, and other data that's always read/written as a unit. They're terrible for relationships that need FK enforcement, frequently modified element-by-element, or that grow unbounded.

When in doubt, start with a junction table. You can always denormalize later if performance demands it. Going the other direction, normalizing arrays back into tables, is much more painful.
