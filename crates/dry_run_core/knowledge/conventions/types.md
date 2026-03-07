---
title: Type Conventions
keywords: type, varchar, text, timestamp, timestamptz, json, jsonb, integer, bigint, serial, money, float, numeric, boolean, enum, array
safety: info
---

## The Rules

- **`text` over `varchar(n)`**: length limits belong in CHECK constraints or application logic, not in the type
- **`timestamptz` over `timestamp`**: always store timezone-aware timestamps
- **`bigint` for PKs and FKs**, not `integer`, to avoid the 32-bit overflow cliff (unless the PK is obviously going to be small)
- **`jsonb` over `json`**: binary format, indexable, supports containment operators
- **`numeric` for money**, never `float` or `double precision` because IEEE 754 can't represent most decimal fractions exactly
- **Use PostgreSQL enums sparingly**: they come with baggage (more on that below)
- **Arrays for co-located data**: but understand the tradeoffs before reaching for them

## Bad Pattern

```sql
CREATE TABLE payment (
  id SERIAL,
  amount FLOAT,                     -- rounding errors
  metadata JSON,                    -- not indexable
  created_at TIMESTAMP,             -- no timezone
  status VARCHAR(20),               -- arbitrary limit
  user_id INTEGER REFERENCES "user" -- 32-bit FK
);
```

Every column here has a better alternative.

## Good Pattern

```sql
CREATE TABLE payment (
  id bigint GENERATED ALWAYS AS IDENTITY,
  amount numeric(12,2) NOT NULL,
  metadata jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  status text NOT NULL,
  user_id bigint NOT NULL REFERENCES "user"(id),
  CONSTRAINT pk_payment PRIMARY KEY (id)
);
```

## The Enum Question

Use PostgreSQL enums sparingly; prefer lookup tables for flexibility. Before PG 14, you **can't remove values from an enum at all**. Even after PG 14, adding values requires a migration. Renaming? Also a migration.

But here's the nuance: for truly fixed sets like compass directions, ISO currency codes, or days of the week, enums work fine. They enforce exact values at the type level, which is stronger than a CHECK constraint on text. `integer[]` guarantees every element is an integer; similarly, an enum guarantees every value is from the allowed set.

The question to ask: "Will this set of values ever change?" If the answer is "maybe" or "probably not but...", use a lookup table. If it's "literally never, this is defined by international standard", enum is fine.

## Arrays: A Quick Word

Arrays store as atomic values. When you modify a single element, PostgreSQL rewrites the entire row. Use them for data that shares the parent row's lifecycle: tags, settings, small lists that always get read and written together. **Don't** use them for relationships across tables. That's what junction tables are for.

(See the full arrays convention document for the detailed treatment.)

## Why These Choices

`text` has zero performance difference from `varchar(n)` in PostgreSQL, so the length check is pure overhead with no benefit. `timestamptz` stores UTC internally and converts on display, preventing the timezone bugs that haunt applications at daylight saving boundaries. `jsonb` is smaller on disk and supports GIN indexes for `@>` queries. And `float` literally cannot represent 0.1 exactly. Try `SELECT 0.1::float + 0.2::float` and weep. Use `numeric` for money.
