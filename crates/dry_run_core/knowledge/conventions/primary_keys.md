---
title: Primary Key Conventions
keywords: primary key, identity, serial, bigserial, auto increment, generated always, natural key, surrogate key
safety: info
---

## The Rules

Use `bigint GENERATED ALWAYS AS IDENTITY` for all primary keys.

**Never use `serial` or `bigserial`.** They create implicit sequences with weird ownership semantics that'll bite you during `pg_dump`/`pg_restore`. They're legacy syntax from before PostgreSQL had proper identity columns.

**Never use natural keys** (email, SSN, product SKU) as primary keys. "But this value will never change!" Famous last words. Businesses rename SKUs. People change emails. SSNs get reassigned. When your "immutable" natural key changes, you get cascading updates across every FK in the database.

Name the constraint: `CONSTRAINT pk_{table} PRIMARY KEY (id)`.

## Bad Pattern

```sql
CREATE TABLE product (
  id SERIAL PRIMARY KEY,     -- implicit sequence, 32-bit overflow risk
  sku VARCHAR(20) PRIMARY KEY -- natural key, will change
);
```

Two problems in two lines. `SERIAL` gives you a 32-bit integer, which is 2.1 billion rows. Sounds like a lot? High-throughput tables chew through that faster than you'd think. And a SKU as primary key? Good luck when marketing decides to "refresh the product codes."

## Good Pattern

```sql
CREATE TABLE product (
  id bigint GENERATED ALWAYS AS IDENTITY,
  sku text NOT NULL,
  CONSTRAINT pk_product PRIMARY KEY (id),
  CONSTRAINT uq_product_sku UNIQUE (sku)
);
```

Surrogate key for identity, natural key as a unique constraint for lookups. Best of both worlds.

## Check Query

Already have tables in production and want to find the legacy `serial` ones? Here you go:

```sql
-- Find tables using serial/sequence-based PKs
SELECT c.table_name, c.column_name, c.column_default
FROM information_schema.columns c
JOIN information_schema.table_constraints tc
  ON c.table_name = tc.table_name AND tc.constraint_type = 'PRIMARY KEY'
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name AND c.column_name = kcu.column_name
WHERE c.column_default LIKE 'nextval%'
  AND c.table_schema = 'public';
```

## Why This Matters

`bigint` avoids the 32-bit overflow cliff. `GENERATED ALWAYS` is SQL-standard, prevents accidental manual inserts that'd mess up your sequence, and doesn't have the ownership issues that make `serial` columns a headache during backups and restores. Natural keys as PKs cause cascade nightmares the moment business decides the "immutable" value needs changing, and they always do.
