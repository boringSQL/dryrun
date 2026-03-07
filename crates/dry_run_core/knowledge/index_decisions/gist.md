---
title: GiST Index Decision
keywords: gist, geometry, range, overlap, nearest neighbor, exclusion, spatial, postgis
min_pg_version: 12
safety: safe
---

## GiST (Generalized Search Tree): When Data Has Shape

GiST handles the kind of queries where B-tree just can't: "does this range overlap that range?", "what's the nearest point?", "do these time periods conflict?" If your data has geometric, spatial, or range-like properties, this is your index.

### When to Use

- PostGIS spatial queries: `WHERE ST_DWithin(geom, point, 1000)`
- Range type overlap: `WHERE daterange && '[2024-01-01, 2024-02-01)'`
- Exclusion constraints: `EXCLUDE USING gist (room WITH =, during WITH &&)`
- Nearest-neighbor (KNN): `ORDER BY geom <-> point LIMIT 10`
- `inet`/`cidr` containment: `WHERE ip_range >> '192.168.1.0/24'`

### When NOT to Use

- Simple equality/range on scalar types → B-tree
- Full-text search → GIN (faster lookups; GiST can do `@@` too, but it's lossy)
- JSONB containment → GIN

### GiST Is Mandatory for EXCLUDE Constraints

Here's something that narrows down the decision fast: if you need an exclusion constraint, **you must use GiST**. GIN can't do it. So when you write:

```sql
EXCLUDE USING gist (room_id WITH =, during WITH &&)
```

There's no alternative index type. GiST or nothing.

### GiST vs GIN for Range Types

For discrete range types (like date ranges), GIN can technically work too. But most applications should just use GiST and move on. The performance difference rarely matters until you're dealing with millions of rows, and GiST gives you the full operator set: overlap, containment, adjacency, the works.

### GiST vs GIN for Full-Text

| Aspect | GiST | GIN |
|--------|------|-----|
| Build time | Faster | Slower |
| Lookup speed | Slower | Faster |
| Update cost | Lower | Higher |
| Lossy? | Yes (recheck needed) | No |
| Best for | Rarely-queried columns, mixed workloads | Frequent text search |

### Watch Out: `subtype_diff` for Custom Range Types

If you're defining custom range types, pay attention to the `subtype_diff` function. This is what tells GiST how far apart two values are, so it can build balanced tree. If your `subtype_diff` returns 0 for every pair (or you forget to define it), you get an unbalanced index where range operators become almost as slow as sequential scan. Not great.

PostgreSQL's built-in range types have proper `subtype_diff` functions, so this only bites you with custom types.

### Check Queries

```sql
-- Check if your query uses range operators
EXPLAIN SELECT * FROM reservations WHERE during && '[2024-01-01, 2024-02-01)';

-- Check existing GiST indexes
SELECT indexdef FROM pg_indexes
WHERE indexdef LIKE '%gist%' AND tablename = 'your_table';
```

### Example

```sql
-- Range overlap
CREATE INDEX CONCURRENTLY idx_reservations_during ON reservations USING gist(during);

-- PostGIS spatial
CREATE INDEX CONCURRENTLY idx_locations_geom ON locations USING gist(geom);

-- Exclusion constraint (no overlapping bookings for same room)
ALTER TABLE bookings ADD CONSTRAINT no_overlap
    EXCLUDE USING gist (room_id WITH =, during WITH &&);

-- KNN (nearest neighbor)
-- Query: SELECT * FROM places ORDER BY geom <-> ST_Point(lng, lat) LIMIT 5;
CREATE INDEX CONCURRENTLY idx_places_geom ON places USING gist(geom);
```
