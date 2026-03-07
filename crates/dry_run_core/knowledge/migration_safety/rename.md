---
title: RENAME Safety
keywords: rename, alter table rename, rename column, rename table
min_pg_version: 12
safety: dangerous
---

## RENAME (Table or Column)

### Quick Decision

- **Rename table or column?** → Metadata-only (fast lock), but **application-breaking**.
- **All callers updated?** → Safe to apply.
- **Any callers still using old name?** → Dangerous. Instant breakage.

### Lock

ACCESS EXCLUSIVE lock, but held only briefly. It's just a catalog update; Postgres doesn't touch your data.

### Why It's Dangerous

The rename itself takes milliseconds. The danger isn't PostgreSQL, it's everything else.

The moment you rename a column or table:
- Application queries using the old name fail **immediately**. Not eventually. Immediately.
- Views referencing the old name break.
- Functions with hardcoded names break.
- ORMs love to cache column and table names. They won't know about your rename until they restart (or worse, until a request hits the wrong code path).

This is one of those operations where the database side is trivial, but coordination with application is everything.

### Safe Pattern: Rename Column

```sql
-- 1. Add new column as alias (PG doesn't support column aliases, so use a view)
-- Or: deploy app changes first, then rename

-- If you must rename:
BEGIN;
ALTER TABLE users RENAME COLUMN email TO email_address;
-- Also update any views:
CREATE OR REPLACE VIEW user_emails AS SELECT id, email_address FROM users;
COMMIT;
```

The safest approach? Deploy your app code first to handle both the old and new name. Then rename. Then deploy again to remove the old-name support. Yes, it's two deploys. That's the price of zero downtime.

### Safe Pattern: Rename Table

```sql
-- 1. Create a view with the old name pointing to the new name
ALTER TABLE orders RENAME TO customer_orders;
CREATE VIEW orders AS SELECT * FROM customer_orders;

-- 2. Migrate callers to use new name

-- 3. Drop the compatibility view
DROP VIEW orders;
```

The compatibility view buys you time. Old code keeps working while you migrate everything to the new name. Once all callers are updated, drop the view.

### Check Before Running

```sql
-- Check views referencing this table/column
SELECT viewname, definition
FROM pg_views
WHERE definition LIKE '%your_table%' OR definition LIKE '%your_column%';

-- Check functions referencing this table/column
SELECT proname, prosrc
FROM pg_proc
WHERE prosrc LIKE '%your_table%' OR prosrc LIKE '%your_column%';
```

These queries won't catch everything because they can't see your application code, your ORMs, your background jobs. But they'll at least tell you what's going to break on database side.
