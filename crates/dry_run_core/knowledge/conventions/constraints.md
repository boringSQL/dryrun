---
title: Constraint Conventions
keywords: constraint, foreign key, index, not null, check, unique, unnamed constraint, fk index, referential integrity
safety: info
---

## The Rules

- **Name all constraints explicitly.** Auto-generated names are fragile, unreadable in error messages, and a nightmare when you need to drop one six months later.
- Every foreign key **must** have a B-tree index on the referencing column(s). Without it, FK checks and cascading deletes cause full table scans and lock the parent.
- Default to `NOT NULL`. Nullability should be a conscious decision, not something you get because you forgot to type seven characters.
- Use `CHECK` constraints for domain validation; let the database protect its own invariants.

## Bad Pattern

```sql
CREATE TABLE order_item (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- unnamed PK
  order_id bigint REFERENCES "order"(id),              -- unnamed FK, no index
  quantity integer,                                      -- nullable by default
  price numeric(10,2)
);
```

Four columns, four problems. The PK has no name. The FK has no name and no index. `quantity` and `price` are nullable for no reason. When this hits production, you'll see error messages like `violates constraint "order_item_order_id_fkey"`, and good luck finding that in your migration files.

## Good Pattern

```sql
CREATE TABLE order_item (
  id bigint GENERATED ALWAYS AS IDENTITY,
  order_id bigint NOT NULL,
  quantity integer NOT NULL,
  price numeric(10,2) NOT NULL,
  CONSTRAINT pk_order_item PRIMARY KEY (id),
  CONSTRAINT fk_order_item_order_id FOREIGN KEY (order_id) REFERENCES "order"(id),
  CONSTRAINT ck_order_item_positive_qty CHECK (quantity > 0),
  CONSTRAINT ck_order_item_positive_price CHECK (price >= 0)
);

CREATE INDEX idx_order_item_order_id ON order_item(order_id);
```

Every constraint named. Every FK indexed. Every column explicitly `NOT NULL`. The error messages tell you exactly what went wrong: `violates constraint "ck_order_item_positive_qty"`. That's debugging in seconds, not minutes.

## Check Query

Want to find the FK indexes you're missing? This query catches them:

```sql
-- Find FKs without a matching index on the referencing side
SELECT
  c.conname AS fk_name,
  c.conrelid::regclass AS table_name,
  a.attname AS column_name
FROM pg_constraint c
JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
WHERE c.contype = 'f'
  AND NOT EXISTS (
    SELECT 1 FROM pg_index i
    WHERE i.indrelid = c.conrelid
      AND a.attnum = ANY(i.indkey)
  );
```

Run this on your database right now. If it returns rows, you've got missing indexes that are silently killing performance.

## Why This Matters

Named constraints produce readable error messages. `"ck_order_item_positive_qty"` tells you what's wrong immediately. Auto-generated names like `"order_item_check1"` tell you nothing.

But the real killer is **missing FK indexes.** This is the #1 cause of unexpected lock contention in production. When you delete a parent row, PostgreSQL needs to check if any child rows reference it. Without an index on the child's FK column, that's a sequential scan of the entire child table while holding a lock. On a table with millions of rows, that means seconds of blocking during peak traffic.
