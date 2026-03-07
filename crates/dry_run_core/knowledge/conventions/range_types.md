---
title: Range Type Conventions
keywords: range, tstzrange, daterange, int4range, numrange, exclude, gist, overlap, contains, adjacent, multirange, infinity
safety: info
---

## Why Range Types

You've got a `start_date` and an `end_date`. Two columns. Seems fine, right? But now you need to enforce "no overlapping bookings." With two separate columns, that's a trigger or a mess of application logic. With a range type and an `EXCLUDE` constraint, it's one line.

Range types give you overlap prevention, adjacency checks, and containment queries, all built into the type system. Stop modeling intervals as column pairs.

## Built-in Range Types

PostgreSQL ships with these:

| Type | Element Type | Use Case |
|------|-------------|----------|
| `int4range` | `integer` | Version ranges, age ranges |
| `int8range` | `bigint` | Large integer intervals |
| `numrange` | `numeric` | Price ranges, measurements |
| `tsrange` | `timestamp` | Time intervals (no timezone) |
| `tstzrange` | `timestamptz` | Time intervals (with timezone, **prefer this**) |
| `daterange` | `date` | Date intervals, booking periods |

For timestamps, use `tstzrange`. Same reasoning as always: timezone awareness prevents bugs.

## Boundary Notation

Ranges use bracket notation to indicate inclusive `[` or exclusive `)` bounds:

- `[1, 5)` includes 1, 2, 3, 4. Does NOT include 5.
- `[1, 5]` includes 1, 2, 3, 4, 5.
- `(1, 5)` includes 2, 3, 4. Excludes both endpoints.

The default for most operations is `[)`, inclusive lower, exclusive upper. This is the right default for most use cases. Think about it: a booking from Monday to Friday as `[Monday, Saturday)` means it covers Monday through Friday. No overlap with a booking starting Saturday.

```sql
-- Create a range: half-open interval [start, end)
SELECT tstzrange('2024-01-01', '2024-02-01', '[)');

-- Check if a point is in a range
SELECT tstzrange('2024-01-01', '2024-02-01') @> '2024-01-15'::timestamptz;
-- true
```

## Key Operators

These are the operators you'll use daily:

```sql
-- Overlap: do these ranges share any points?
SELECT tstzrange('2024-01-01', '2024-02-01') && tstzrange('2024-01-15', '2024-03-01');
-- true

-- Contains element
SELECT int4range(1, 10) @> 5;
-- true

-- Contains range
SELECT int4range(1, 100) @> int4range(5, 10);
-- true

-- Adjacent: do these ranges "touch" without overlapping?
SELECT int4range(1, 5) -|- int4range(5, 10);
-- true

-- Union, intersection, difference
SELECT int4range(1, 10) * int4range(5, 15);  -- intersection: [5,10)
SELECT int4range(1, 10) + int4range(5, 15);  -- union: [1,15)
SELECT int4range(1, 10) - int4range(5, 15);  -- difference: [1,5)
```

## The Killer Feature: EXCLUDE Constraints

This is where range types really shine. Prevent overlapping bookings in one line:

```sql
CREATE TABLE booking (
  id bigint GENERATED ALWAYS AS IDENTITY,
  room_id bigint NOT NULL,
  during tstzrange NOT NULL,
  CONSTRAINT pk_booking PRIMARY KEY (id),
  CONSTRAINT no_overlapping_bookings
    EXCLUDE USING gist (room_id WITH =, during WITH &&)
);
```

That constraint says: "No two rows can have the same `room_id` AND overlapping `during` ranges." Try inserting conflicting bookings and PostgreSQL will reject them. No triggers. No application logic. No race conditions.

**Note:** You'll need the `btree_gist` extension for combining equality (`=`) with range overlap (`&&`) in EXCLUDE constraints:

```sql
CREATE EXTENSION IF NOT EXISTS btree_gist;
```

## Infinity Bounds

For open-ended ranges ("from this date onwards" or "up to this point"), use infinity bounds instead of far-future sentinel dates:

```sql
-- Subscription active from now, no end date
INSERT INTO subscription (user_id, active_period)
VALUES (42, tstzrange(now(), null));

-- null bound means infinity
SELECT upper_inf(tstzrange('2024-01-01', null));
-- true
```

Don't use `'9999-12-31'` as "no end date." That's a sentinel value pretending to be data. `null` upper bound means infinity, and PostgreSQL has functions (`upper_inf`, `lower_inf`) to check for it.

## NULL vs Empty Ranges

These are different things:

- **NULL range**: the range itself is unknown/not applicable
- **Empty range**: the range exists but contains no points

```sql
SELECT 'empty'::int4range;          -- empty range, contains nothing
SELECT int4range(5, 5, '[)');       -- also empty: [5,5) has no elements
SELECT isempty(int4range(5, 5));    -- true
```

Use NULL when the range doesn't apply. Use empty ranges when the interval is known to be zero-length. They're semantically different; don't confuse them.

## Multiranges (PG 14+)

PostgreSQL 14 introduced multiranges, which are sets of non-overlapping ranges in a single value. Think "availability windows" or "business hours":

```sql
-- A schedule with gaps
SELECT '{[09:00,12:00), [13:00,17:00)}'::tstzmultirange;

-- Check if a point falls in any sub-range
SELECT '{[09:00,12:00), [13:00,17:00)}'::tstzmultirange @> '10:30'::timestamptz;
-- true (it's in the morning window)
```

Before multiranges, modeling "available Monday 9-12 and 1-5" required multiple rows or arrays of ranges. Now it's a single column value.

## Custom Range Types

Need a range over a custom type? You can create one. The key is providing a `subtype_diff` function so PostgreSQL can estimate selectivity:

```sql
-- Range over a custom domain
CREATE FUNCTION float8_diff(a float8, b float8) RETURNS float8
AS 'SELECT a - b' LANGUAGE sql IMMUTABLE;

CREATE TYPE float8range AS RANGE (
  subtype = float8,
  subtype_diff = float8_diff
);
```

Without `subtype_diff`, the planner can't estimate how many rows a range query will match, and you'll get bad query plans. Always provide it for custom range types.

## Indexing

Two options, each with different strengths:

### GiST (Default)

GiST indexes work with all range types and support EXCLUDE constraints. This is your default choice:

```sql
CREATE INDEX idx_booking_during ON booking USING gist (during);
```

GiST is the only index type that works with `EXCLUDE` constraints. If you need overlap prevention, you need GiST.

### SP-GiST

For some workloads (especially non-overlapping ranges), SP-GiST can be faster:

```sql
CREATE INDEX idx_event_period ON event USING spgist (period);
```

But SP-GiST doesn't support EXCLUDE constraints. So if you need both containment queries and overlap prevention on the same column, stick with GiST.

## The Practical Guideline

Whenever you catch yourself creating `start_` and `end_` column pairs, stop and use a range type instead. You get:

- Atomicity: the interval is one value, not two columns that could get out of sync
- Overlap prevention via `EXCLUDE` constraints, which is impossible with separate columns without triggers
- Built-in operators for overlap, containment, adjacency, with no custom SQL needed
- Proper NULL/infinity semantics instead of sentinel values
