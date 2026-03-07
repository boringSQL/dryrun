---
title: Timestamp Conventions
keywords: timestamp, created_at, updated_at, deleted_at, soft delete, audit, timestamptz, timezone, range, tstzrange
safety: info
---

## The Rules

- Every table gets `created_at timestamptz NOT NULL DEFAULT now()`
- Every table gets `updated_at timestamptz NOT NULL DEFAULT now()` with a trigger (or application-level update, but triggers are more reliable)
- **`timestamptz`**, never `timestamp`. PostgreSQL stores it as UTC internally, converts on display
- For soft deletes, add `deleted_at timestamptz` (nullable; null means "alive")
- Index `created_at` on high-volume tables for time-range queries

## Bad Pattern

```sql
CREATE TABLE article (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  title text NOT NULL,
  body text NOT NULL
  -- no timestamps at all
);
```

You'll regret this the first time someone asks "when was this row created?" and you're digging through application logs to find out.

## Good Pattern

```sql
CREATE TABLE article (
  id bigint GENERATED ALWAYS AS IDENTITY,
  title text NOT NULL,
  body text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz,
  CONSTRAINT pk_article PRIMARY KEY (id)
);

CREATE INDEX idx_article_created_at ON article(created_at);
```

## Trigger for updated_at

Don't rely on your application to set `updated_at`. Every ORM, every script, every manual `UPDATE` would need to remember. Use a trigger instead:

```sql
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_article_updated_at
  BEFORE UPDATE ON article
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
```

One trigger, and you never worry about stale `updated_at` values again.

## When You Have Start/End Pairs

Got a `valid_from` and `valid_until`? A `start_time` and `end_time`? Stop. Don't use two separate columns.

Reach for `tstzrange` instead. You get overlap prevention, adjacency operators (`-|-`), and exclusion constraints, all for free. Two separate columns can't enforce "no overlapping bookings" without application logic or a messy trigger. A range type with an `EXCLUDE` constraint does it in one line.

(See the full range types convention document for details.)

## Why This Matters

Timestamps are essential for debugging, auditing, and data recovery. Without `created_at`, you can't answer "when was this created?" without digging through logs, if the logs even still exist. `updated_at` enables efficient cache invalidation and incremental sync. Soft deletes via `deleted_at` let you recover accidentally deleted data and preserve referential integrity while still hiding "deleted" rows from your application.
