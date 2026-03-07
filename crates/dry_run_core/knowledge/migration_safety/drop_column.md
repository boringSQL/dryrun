---
title: DROP COLUMN Safety
keywords: drop column, alter table drop column
min_pg_version: 12
safety: caution
---

## DROP COLUMN

### Quick Decision

- **`DROP COLUMN`?** → Generally safe. Metadata-only operation (marks column as dropped).
- **Column has a DEFAULT with a volatile function?** → Still metadata-only.
- **Column is part of a view, index, or FK?** → Use `CASCADE` or drop dependents first.

### How It Works

Here's a fun one: PostgreSQL doesn't actually remove your column data. Not even a little bit.

What it does:
1. Marks the column as "dropped" in `pg_attribute` (sets `attisdropped = true`)
2. Walks away

The actual bytes? They're still sitting there on disk, taking up space. They get reclaimed lazily, when VACUUM processes rows that have been updated or deleted. So your `DROP COLUMN` is fast, but your table doesn't shrink immediately.

### Lock

ACCESS EXCLUSIVE lock, but held only briefly. It's just a catalog update.

### Safe Pattern

```sql
-- Fast, metadata-only
ALTER TABLE orders DROP COLUMN legacy_field;

-- If there are dependent objects
ALTER TABLE orders DROP COLUMN legacy_field CASCADE;
```

### Caveats

- **`CASCADE` is the nuclear option.** It drops dependent views, indexes, constraints, everything that touches this column. Always review what depends on it before you pull the trigger.
- The dropped column still occupies space on disk until rows are updated and then vacuumed. Don't expect your table to shrink right away.
- Column ordinal positions of remaining columns don't change. This is Postgres being polite to existing code.
- `NOT NULL` constraints on the dropped column are removed automatically.

### Check Before Running

```sql
-- Check what depends on this column
SELECT
    d.classid::regclass, d.objid, d.deptype,
    pg_describe_object(d.classid, d.objid, d.objsubid)
FROM pg_depend d
WHERE d.refobjid = 'your_table'::regclass
  AND d.refobjsubid = (
      SELECT attnum FROM pg_attribute
      WHERE attrelid = 'your_table'::regclass AND attname = 'your_column'
  );
```

Run this first. Seriously.
