---
title: ADD FOREIGN KEY Safety
keywords: add foreign key, references, alter table add constraint foreign
min_pg_version: 12
safety: caution
---

## ADD FOREIGN KEY

### Quick Decision

- **`ADD CONSTRAINT ... FOREIGN KEY ... NOT VALID` then `VALIDATE CONSTRAINT`?** → Safe. Recommended pattern.
- **`ADD CONSTRAINT ... FOREIGN KEY` (direct)?** → Dangerous on large tables. Holds ACCESS EXCLUSIVE + scans entire table.

### The Two-Step Pattern (Do This)

```sql
-- Step 1: Add FK constraint without validating existing rows
-- Lock: SHARE ROW EXCLUSIVE on both tables (brief, metadata only)
ALTER TABLE orders ADD CONSTRAINT fk_orders_customer
    FOREIGN KEY (customer_id) REFERENCES customers(id) NOT VALID;

-- Step 2: Validate existing rows (can run concurrently with reads/writes)
-- Lock: SHARE UPDATE EXCLUSIVE on child table, ROW SHARE on parent
ALTER TABLE orders VALIDATE CONSTRAINT fk_orders_customer;
```

### Why Two Steps?

Think of it this way:

Step 1 says: "From now on, every new insert into `orders` must have a valid `customer_id`. But I won't check the existing 50 million rows right now." The lock is brief, just a metadata update.

Step 2 says: "OK, now let me scan through existing rows to make sure they're all legit." But it does this with SHARE UPDATE EXCLUSIVE, which is a much weaker lock. Your app keeps handling requests while Postgres validates in the background.

If you skip `NOT VALID` and do it in one step, you get ACCESS EXCLUSIVE while it scans everything. That's the lock that blocks *all* queries. On a large table, you're looking at minutes of downtime.

### The Dangerous Way

```sql
-- Single-step: holds ACCESS EXCLUSIVE while scanning the entire table
ALTER TABLE orders ADD CONSTRAINT fk_orders_customer
    FOREIGN KEY (customer_id) REFERENCES customers(id);
```

### Performance Impact

- The referenced table (parent) **must** have an index on the referenced column(s), typically the PRIMARY KEY. Without it, validation becomes painfully slow.
- Validation scans the child table, checking each row against the parent's index.
- Duration is proportional to the child table size. A million rows? Seconds. A billion? Grab coffee.

### Check Before Running

```sql
-- Verify parent has index on referenced column
SELECT indexdef FROM pg_indexes
WHERE tablename = 'customers' AND indexdef LIKE '%id%';

-- Check for orphaned rows that would violate the FK
SELECT count(*) FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id);

-- Check child table size
SELECT pg_size_pretty(pg_total_relation_size('orders'));
```

**Run that orphan check.** If there are orphaned rows, `VALIDATE` will fail, and you'll need to fix the data first.
