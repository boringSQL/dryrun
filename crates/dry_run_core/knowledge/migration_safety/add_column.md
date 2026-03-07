---
title: ADD COLUMN Safety
keywords: add column, alter table add
min_pg_version: 11
safety: caution
---

## ADD COLUMN

### Quick Decision

- **Column with no DEFAULT and nullable?** → Safe. Metadata-only change (all PG versions).
- **Column with DEFAULT (PG 11+)?** → Safe for immutable defaults. Metadata-only since PG 11.
- **Column with volatile DEFAULT (e.g. `now()`)?** → Dangerous. Full table rewrite.
- **Column with DEFAULT (PG 10 and below)?** → Dangerous. Full table rewrite regardless of volatility.

### Version Behavior

| PG Version | DEFAULT behavior | Lock |
|------------|-----------------|------|
| PG 12–18 | Immutable DEFAULT is metadata-only | ACCESS EXCLUSIVE (brief) |
| PG 11 | Immutable DEFAULT is metadata-only (first version) | ACCESS EXCLUSIVE (brief) |
| PG 10 and below | Any DEFAULT triggers full table rewrite | ACCESS EXCLUSIVE (long) |

### What's Actually Happening

Before PG 11, adding a column with a DEFAULT was a nightmare. PostgreSQL would rewrite every single row in the table just to tack on your new value. On a 500M-row table? Go grab lunch.

PG 11 changed the game. Now, when you add a column with an **immutable** default (a constant, an immutable function), Postgres just writes the default value into `pg_attrdef` and calls it a day. When rows are read, the default gets applied lazily. No rewrite, no drama.

But here's what it doesn't do: **volatile defaults** like `now()`, `random()`, or `gen_random_uuid()` still trigger a full table rewrite. Why? Because each row needs its own distinct value, and Postgres can't just store one value and pretend it works for everyone.

### Safe Pattern

```sql
-- Safe on PG 11+: immutable default
ALTER TABLE orders ADD COLUMN status text DEFAULT 'pending';

-- Safe on all versions: nullable, no default
ALTER TABLE orders ADD COLUMN notes text;
```

Nothing exciting here. The first one is metadata-only on PG 11+. The second is metadata-only everywhere. You're good.

### Dangerous Pattern

```sql
-- Rewrites table on ALL versions: volatile default
ALTER TABLE orders ADD COLUMN created_at timestamptz DEFAULT now();
```

Wait, you need a `created_at` with `now()` as default? Do it in two steps:

```sql
-- Safe alternative: add nullable, then backfill
ALTER TABLE orders ADD COLUMN created_at timestamptz;
-- Then backfill in batches
UPDATE orders SET created_at = now() WHERE created_at IS NULL AND id BETWEEN ... AND ...;
-- Then add NOT NULL if needed (see add_not_null.md)
```

Yes, it's more work. But your users won't notice. They would definitely notice a table lock lasting minutes.

### Lock Duration

ACCESS EXCLUSIVE lock is acquired in all cases, but the difference is massive:
- **Metadata-only:** lock held for milliseconds.
- **Table rewrite:** lock held for the entire rewrite duration, proportional to table size.

### Check Before Running

```sql
-- Check table size
SELECT pg_size_pretty(pg_total_relation_size('your_table'));

-- Check if default is volatile
SELECT provolatile FROM pg_proc WHERE proname = 'your_function';
-- 'i' = immutable (safe), 'v' = volatile (rewrite), 's' = stable (rewrite)
```

If `provolatile` comes back as anything other than `'i'`, you're looking at a rewrite. Use the two-step pattern instead.
