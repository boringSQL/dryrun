---
title: ADD CHECK CONSTRAINT Safety
keywords: add check constraint, add constraint check, alter table add constraint check
min_pg_version: 12
safety: caution
---

## ADD CHECK CONSTRAINT

### Quick Decision

- **`ADD CONSTRAINT ... CHECK ... NOT VALID` then `VALIDATE`?** → Safe. Same pattern as FK.
- **`ADD CONSTRAINT ... CHECK` (direct)?** → Dangerous. Scans entire table under ACCESS EXCLUSIVE lock.

### The Two-Step Pattern (Do This)

```sql
-- Step 1: Add check without validating existing rows
ALTER TABLE products ADD CONSTRAINT chk_price_positive
    CHECK (price_cents >= 0) NOT VALID;

-- Step 2: Validate existing rows (weaker lock, allows concurrent DML)
ALTER TABLE products VALIDATE CONSTRAINT chk_price_positive;
```

Step 1 tells Postgres: "Enforce this for new inserts and updates, but don't scan what's already there." The lock is brief, just a catalog change.

Step 2 actually checks existing rows, but it only needs SHARE UPDATE EXCLUSIVE. Your app keeps running. Reads work. Writes work. Everyone's happy.

### Lock Behavior

| Step | Lock | Duration |
|------|------|----------|
| ADD ... NOT VALID | ACCESS EXCLUSIVE | Brief (metadata only) |
| VALIDATE CONSTRAINT | SHARE UPDATE EXCLUSIVE | Proportional to table size |
| ADD ... (without NOT VALID) | ACCESS EXCLUSIVE | Proportional to table size |

See that last row? That's the "I didn't read the docs" row. You're holding the strongest possible lock while Postgres scans every row in your table. On a big table, that's a recipe for downtime.

### The Dangerous Way

```sql
-- Scans entire table while holding ACCESS EXCLUSIVE lock
ALTER TABLE products ADD CONSTRAINT chk_price_positive
    CHECK (price_cents >= 0);
```

Looks simpler, right? It is simpler. It's also going to block every query on `products` until it finishes scanning. Don't do this in production.

### Check Before Running

```sql
-- Check for violating rows before adding
SELECT count(*) FROM products WHERE NOT (price_cents >= 0);

-- Check table size
SELECT pg_size_pretty(pg_total_relation_size('products'));
```

**Pro tip:** Always check for violations first. If `VALIDATE` finds a violating row, it fails and you have to fix data before retrying. Better to know upfront than to discover it mid-migration.
