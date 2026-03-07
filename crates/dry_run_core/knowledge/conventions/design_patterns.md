---
title: Schema Design Patterns
keywords: design pattern, audit trail, enum table, polymorphic, polymorphic association, junction table, many-to-many, enum, lookup table, sti, single table inheritance
safety: info
---

## Audit Trails

For tracking who changed what, use a separate audit table. Don't try to bolt audit columns onto the main table because that bloats every row and makes the main table's lock contention worse.

```sql
CREATE TABLE audit_log (
  id bigint GENERATED ALWAYS AS IDENTITY,
  table_name text NOT NULL,
  record_id bigint NOT NULL,
  action text NOT NULL,  -- INSERT, UPDATE, DELETE
  old_data jsonb,
  new_data jsonb,
  changed_by text,
  changed_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT pk_audit_log PRIMARY KEY (id)
);

CREATE INDEX idx_audit_log_table_record ON audit_log(table_name, record_id);
CREATE INDEX idx_audit_log_changed_at ON audit_log(changed_at);
```

The `jsonb` columns for old/new data give you full change history without needing a separate audit table per domain table. One table, all changes, easy to query.

## Enum Tables vs PostgreSQL Enums

This one needs nuance. The knee-jerk reaction is "never use PostgreSQL enums," but that's too dogmatic.

**Prefer lookup tables when values may change:**

```sql
-- Flexible: add, rename, reorder anytime
CREATE TABLE order_status (
  id smallint GENERATED ALWAYS AS IDENTITY,
  name text NOT NULL,
  CONSTRAINT pk_order_status PRIMARY KEY (id),
  CONSTRAINT uq_order_status_name UNIQUE (name)
);
```

**PostgreSQL enums are fine for truly fixed sets:**

```sql
-- Fine: compass directions aren't going to change
CREATE TYPE direction AS ENUM ('north', 'south', 'east', 'west');

-- Also fine: ISO currency codes
CREATE TYPE currency_code AS ENUM ('USD', 'EUR', 'GBP', 'CZK');
```

Here's the practical decision tree:

- **"Will this set ever change?"** If yes, or even "probably not but...", use a lookup table
- **"Is this defined by international standard or physics?"** Then enum is fine
- **"Do I need to remove values?"** Use a lookup table (before PG 14, you literally can't remove enum values; after PG 14, you can, but it's still a migration)
- **"Do I need metadata on each value?"** (display order, description, active flag) Lookup table, no question

Enums enforce exact values at the type level, which is stronger than a CHECK constraint on text. But that rigidity is also their weakness. Choose based on how stable the values actually are, not how stable you hope they'll be.

## Avoid Polymorphic Associations

The `(target_type, target_id)` pattern. It looks clever. It's not. You can't create real foreign keys on it, which means the database can't enforce referential integrity:

```sql
-- BAD: no FK enforcement, target_id could point at anything
CREATE TABLE comment (
  id bigint PRIMARY KEY,
  target_type text NOT NULL,  -- 'article', 'product'
  target_id bigint NOT NULL   -- could point to deleted row, wrong table, whatever
);

-- GOOD: separate FK per target
CREATE TABLE article_comment (
  id bigint PRIMARY KEY,
  article_id bigint NOT NULL REFERENCES article(id),
  body text NOT NULL
);
```

"But I'll have so many tables!" Yes. And every one of them will have actual referential integrity.

## Avoid Entity-Attribute-Value (EAV)

EAV tables, `(entity_id, attribute_name, attribute_value)`, destroy everything that makes a relational database useful. No type safety. No constraints. Queries become monstrous pivots. Every "simple" question requires joining the same table N times.

If you need truly dynamic attributes, use a `jsonb` column. It's not perfect, but it's indexable, queryable, and at least keeps the data together. If the attributes are knowable at design time, just add concrete columns.

## Why These Patterns

These patterns trade a small amount of upfront design for significant long-term maintainability. Audit trails in separate tables keep your main tables lean and fast. Lookup tables are queryable, FK-enforced, and can carry metadata. Avoiding polymorphic associations means the database actually does its job: enforcing referential integrity instead of hoping your application gets it right every time.
