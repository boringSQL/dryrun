---
title: Schema Anti-Patterns
keywords: anti-pattern, god table, column explosion, money float, nullable boolean, missing index, over-indexing, soft delete, eav, entity attribute value, array foreign key
safety: info
---

## God Tables

You know the type. Fifty-plus columns, half of them nullable, trying to represent three different concerns in one table. The `user` table that also tracks preferences, billing info, notification settings, and somehow their last GPS coordinates.

**Signs you've got one:** dozens of nullable columns, column names with prefixes grouping concerns (`billing_`, `shipping_`, `pref_`), and `ALTER TABLE ADD COLUMN` being your most common migration.

Split them. Focused tables with clear ownership.

## Column Explosion

Adding a column per variant instead of normalizing:

```sql
-- BAD
CREATE TABLE product (
  id bigint PRIMARY KEY,
  shipping_address_line1 text,
  shipping_address_line2 text,
  shipping_city text,
  billing_address_line1 text,
  billing_address_line2 text,
  billing_city text
);

-- GOOD: normalize to a separate table
CREATE TABLE address (
  id bigint PRIMARY KEY,
  line1 text NOT NULL,
  line2 text,
  city text NOT NULL,
  address_type text NOT NULL  -- 'shipping', 'billing'
);
```

When you see column names that differ only by a prefix, that's a table trying to escape. Let it.

## Money as Float

Never store monetary values as `float` or `double precision`. IEEE 754 cannot represent most decimal fractions exactly. This isn't theoretical; it will bite you:

```sql
-- BAD:  0.1 + 0.2 = 0.30000000000000004
amount DOUBLE PRECISION

-- GOOD: exact decimal arithmetic
amount NUMERIC(12, 2)
```

Try `SELECT 0.1::float + 0.2::float` in psql. That extra `0.00000000000000004` might not seem like a big deal, until you're reconciling millions of transactions and the books don't balance. Use `numeric`. Always.

## Nullable Booleans

A `boolean NULL` column has three states: true, false, and "I have no idea." That third state is almost never intentional. Use `NOT NULL DEFAULT false`, or if you genuinely need more than two states, reach for an enum or text column and be explicit about it.

## Missing FK Indexes

Every foreign key column needs an index. Without it:

- `ON DELETE CASCADE` does a **sequential scan** of the entire child table
- Joining on the FK does a sequential scan
- Parent table updates acquire locks that block way longer than necessary

This is the silent killer. Your queries work fine in development with 100 rows. In production with 10 million? You're holding locks for seconds while PostgreSQL scans the whole table. (See the constraints convention for a query that finds these.)

## Over-Indexing

Every index slows writes and eats disk. Don't add indexes speculatively, "just in case someone queries by this column." Measure first. If `pg_stat_user_indexes.idx_scan = 0` for an index, it's dead weight. Drop it.

## Arrays as Foreign Keys

This one's tempting. You've got a `tag_ids integer[]` column and it feels clean, with all the references in one place and no junction table clutter. But here's what it doesn't do:

**No foreign keys exist for array elements.** No `ON DELETE CASCADE`. No referential integrity at all. When you delete a referenced row, the orphaned ID just stays in your array forever, silently pointing at nothing.

```sql
-- BAD: no referential integrity
CREATE TABLE article (
  id bigint PRIMARY KEY,
  tag_ids integer[]  -- hope those IDs still exist!
);

-- GOOD: junction table with real FKs
CREATE TABLE article_tag (
  article_id bigint NOT NULL REFERENCES article(id),
  tag_id bigint NOT NULL REFERENCES tag(id),
  CONSTRAINT pk_article_tag PRIMARY KEY (article_id, tag_id)
);
```

If you need referential integrity, and you almost certainly do, use junction tables. Arrays are for data that lives and dies with the parent row, not for relationships across tables.

## Why These Matter

These anti-patterns are the most common sources of production database pain. God tables lead to lock contention because every `UPDATE` competes for the same row. Float money leads to accounting errors that are maddening to track down. Missing FK indexes are the silent performance killer that doesn't show up until you have real data. And arrays as foreign keys give you the illusion of integrity without any of the actual guarantees.
