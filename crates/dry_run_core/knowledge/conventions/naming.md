---
title: Naming Conventions
keywords: naming, snake_case, table name, column name, identifier, abbreviation, fk naming, index naming, comment, comment on
safety: info
---

## The Rules

Your identifiers should be `snake_case`. All of them: tables, columns, constraints, indexes. No exceptions.

Table names are **singular**: `user`, not `users`. `order_item`, not `order_items`. One row represents one entity. The name should reflect that.

**No abbreviations.** Ever. It's `first_name`, not `fname`. It's `created_at`, not `crt_at`. Your future self (and your teammates) will thank you when they don't have to guess what `crt_at` means at 2 AM.

Foreign key columns follow the pattern `{referenced_table}_id`, so `user_id`, `order_id`. Simple, predictable, boring.

## Constraint Naming

Name your constraints. **Always.** Here's the pattern:

- Primary keys: `pk_{table}` (e.g. `pk_user`)
- Foreign keys: `fk_{table}_{column}` (e.g. `fk_order_user_id`)
- Unique constraints: `uq_{table}_{columns}` (e.g. `uq_user_email`)
- Check constraints: `ck_{table}_{description}` (e.g. `ck_order_positive_total`)
- Indexes: `idx_{table}_{columns}` (e.g. `idx_order_created_at`)

## Bad Pattern

```sql
CREATE TABLE Users (
  ID SERIAL PRIMARY KEY,
  FName VARCHAR(50),
  LName VARCHAR(50),
  usr_email VARCHAR(255)
);
```

Mixed case, abbreviations, inconsistent prefixes. PostgreSQL will lowercase unquoted identifiers anyway, so `Users` becomes `users`, and now you've got a table that doesn't match your code.

## Good Pattern

```sql
CREATE TABLE "user" (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  first_name text NOT NULL,
  last_name text NOT NULL,
  email text NOT NULL
);
```

Clean, readable, self-documenting.

## Use COMMENT ON Everything

Good names help, but they can't explain business rules, edge cases, or why a column exists. That's what `COMMENT ON` is for. Use it liberally on tables, columns, types, and constraints.

```sql
COMMENT ON TABLE seat_hold IS
    'Temporary reservation preventing double-booking during checkout flow. Rows expire via hold_period upper bound.';

COMMENT ON COLUMN seat_hold.hold_period IS
    'Half-open interval [start, end). The hold is active while now() is contained in this range. Enforced non-overlapping per seat via EXCLUDE constraint.';

COMMENT ON COLUMN payment.amount IS
    'Total charged in smallest currency unit (cents for USD, yen for JPY). Never fractional.';

COMMENT ON COLUMN "user".deleted_at IS
    'Soft delete marker. NULL means active. Filtered out by application queries but preserved for audit and GDPR data subject requests.';
```

Comments are stored in `pg_description` and show up in `\d+` output, `pg_dump`, and every database tool worth using. They cost nothing at runtime and survive schema migrations (they're tied to the object OID, not the DDL).

There's another reason comments matter now more than ever: **LLMs and AI tools read your schema.** When a code assistant introspects your database to generate queries, write migrations, or suggest indexes, comments are the only source of business context it has. A column named `status text NOT NULL` could mean anything. `COMMENT ON COLUMN order.status IS 'One of: draft, pending_payment, paid, shipped, cancelled. Transitions enforced by application. Orders in draft are not visible to customers.'` turns a guessing game into a clear specification. The better your comments, the better the suggestions you'll get from any tool that touches your schema.

A few guidelines:

- **Every table** should have a comment explaining what it represents and any non-obvious lifecycle rules (who creates rows, when they get deleted, etc.)
- **Columns with business logic** need comments. If the column name alone doesn't tell you the unit, the format, or the edge cases, add a comment.
- **Enum types and check constraints** benefit from comments explaining why the values are what they are.
- Don't comment the obvious. `COMMENT ON COLUMN user.email IS 'The user email'` adds nothing. `COMMENT ON COLUMN user.email IS 'Unique per account. Used as login identifier and for transactional emails. Normalized to lowercase on insert.'` actually helps.

You can also query comments programmatically:

```sql
-- List all table comments
SELECT c.relname, d.description
FROM pg_class c
JOIN pg_description d ON c.oid = d.objoid AND d.objsubid = 0
WHERE c.relkind = 'r' AND c.relnamespace = 'public'::regnamespace;

-- List column comments for a specific table
SELECT a.attname, d.description
FROM pg_attribute a
JOIN pg_description d ON a.attrelid = d.objoid AND a.attnum = d.objsubid
WHERE a.attrelid = 'your_table'::regclass AND a.attnum > 0;
```

Comments should be part of your migrations, right next to the `CREATE TABLE`. Not something you "add later." Later never comes.

## Why This Matters

`snake_case` is the PostgreSQL convention. Go against it and you'll be quoting identifiers forever, adding friction to every query you write. Singular table names match the entity they represent. And abbreviations create ambiguity across teams. What does `usr` mean in your codebase? What about `usr_email` vs `user_email`? Don't make people guess.

Consistent naming makes your schema self-documenting, and that's how you keep schema readable at 3 AM during an incident.
