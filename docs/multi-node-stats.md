# Multi-Node Statistics

Single-node statistics hide problems that only show up when you look across the cluster. The primary might be well-tuned while a reporting replica is doing millions of sequential scans on the same table, or one replica sits idle because the connection pooler routes all traffic elsewhere.

dryrun merges statistics from every node in your cluster into one snapshot, then surfaces the differences.

## Collecting stats

The idea is straightforward. One full dump from the primary, lightweight stats-only dumps from replicas.

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐ 
│  Primary    │     │  Replica 1  │     │  Replica 2  │
│ (full dump) │     │ (stats only)│     │ (stats only)│
└─────┬───────┘     └──────┬──────┘     └──────┬──────┘
      │                    │                    │
      ▼                    ▼                    ▼
   master.json        r1-stats.json        r2-stats.json
     │                    │                    │
     └────────────┬───────┘────────────────────┘
                  ▼
        dryrun import master.json \
          --stats r1-stats.json r2-stats.json
                  │
                  ▼
           .dryrun/schema.json
          (schema + all node stats)
```

The full dump captures table definitions, columns, constraints, indexes, partitions, functions, enums, extensions, and GUCs. Stats-only dumps capture `pg_stat_user_tables`, `pg_stat_user_indexes`, and `pg_statistic`. They're small, fast, and safe for cron.

### Full dump from primary

```sh
dryrun dump-schema --source "postgres://readonly@primary:5432/mydb" \
  --name "primary" --pretty -o master.json
```

`--name` tags this node in the output and controls whether the primary appears in per-node comparisons (`compare_nodes`, `detect`). Without it, statistics are embedded in the schema but the primary won't appear in the `node_stats` array.

### Stats-only dumps from replicas

```sh
dryrun dump-schema --source "postgres://readonly@replica-1:5432/mydb" \
  --stats-only --name "replica-1" -o r1-stats.json

dryrun dump-schema --source "postgres://readonly@replica-2:5432/mydb" \
  --stats-only --name "replica-2" -o r2-stats.json

dryrun dump-schema --source "postgres://readonly@replica-3:5432/mydb" \
  --stats-only --name "replica-3" -o r3-stats.json
```

`--stats-only` skips structural schema and captures only runtime statistics. Files are typically 1–5 MB and take seconds to produce. `--name` is required with `--stats-only`.

### Import and merge

```sh
dryrun import master.json --stats r1-stats.json r2-stats.json r3-stats.json
```

The result lands in `.dryrun/schema.json`: full schema from the primary plus a `node_stats` array with per-node statistics. Every dryrun tool picks up multi-node data automatically.

## Aggregation rules

When multiple nodes report stats for the same table, dryrun combines them:

| Statistic | Rule | Why |
|---|---|---|
| `reltuples`, `relpages` | max across nodes | All replicas replay the same WAL, so values should be close. Max is the safest estimate for planning |
| `seq_scan`, `idx_scan` | sum across nodes | Reveals total query load hitting the cluster |
| `dead_tuples` | max across nodes | Worst case is what matters for vacuum decisions |
| `table_size` | max across nodes | Same reasoning as reltuples |
| `last_vacuum`, `last_analyze` | primary only | Autovacuum doesn't run on standby replicas, so their timestamps are always null |

## Analysis tools

All multi-node analysis tools are MCP tools. They work offline from imported `node_stats`.

### compare_nodes

Side-by-side stats for a specific table across all nodes.

```
Per-node breakdown (4 node(s)):

                reltuples  relpages  seq_scan     idx_scan  table_size  collected
primary         1,234,567     5,123     1,024       45,000     10 MB    2026-04-01 14:32
replica-1       1,234,567     5,123        12       45,000     10 MB    2026-04-01 14:30
replica-2       1,234,567     5,098   987,654       44,998     10 MB    2026-04-01 14:31
replica-3       1,234,567     5,123       203       45,000     10 MB    2026-04-01 14:28
```

Here `replica-2` has 987k sequential scans while others sit under 1,100, pointing to a routing problem or a missing index on that replica's workload.

The output also includes per-index scan counts and flags indexes with zero scans across all nodes.

### detect

Health checks across all nodes. Pass a specific check or run them all:

```
detect(kind = "all")
detect(kind = "stale_stats")
detect(kind = "unused_indexes")
detect(kind = "anomalies")
detect(kind = "bloated_indexes")
```

**stale_stats** finds tables where `ANALYZE` hasn't run recently, broken down by node (7-day threshold):

```
Stale stats:
  replica-2  public.events        last analyzed 14 days ago
  replica-2  public.audit_log     never analyzed
```

Replicas don't run autovacuum, so `last_analyze` timestamps reflect manual `ANALYZE` runs only. A replica added months ago without scheduled `ANALYZE` will show "never analyzed" here.

**unused_indexes** reports indexes with zero scans on *every* node, not just one. This prevents false positives where an index looks unused on the primary but is critical for replica read queries:

```
Unused indexes (0 scans across all 4 nodes):
  public.users  idx_users_legacy_status  12 MB
  public.orders idx_orders_old_region    8 MB
```

**anomalies** detects seq_scan imbalance (5x threshold):

```
Seq scan imbalance:
  public.events  replica-2 handles 812x more seq_scans than other nodes
```

**bloated_indexes** estimates index bloat from `relpages` vs expected pages (default threshold: 1.5x).

### vacuum_health

Autovacuum analysis using aggregated dead tuple counts but primary-only vacuum timestamps. Replicas don't run autovacuum, so their timestamps are always null. Using dead tuple counts from all nodes and vacuum timing from the primary gives accurate distance-to-trigger calculations.

## Practical scenarios

### Reporting replica with seq scans

The primary uses indexed lookups on `orders`, but a BI tool connected through `replica-2` runs `SELECT ... WHERE created_at BETWEEN ...` without a covering index. Single-node monitoring on the primary shows nothing wrong. `compare_nodes` reveals `replica-2` with millions of sequential scans.

Fix: add a covering index for the BI query pattern, or route analytics to a dedicated replica.

### Safe index cleanup

`idx_users_legacy_email` has `idx_scan = 0` on the primary, but a replica might depend on it. `detect unused_indexes` checks all nodes. Zero everywhere, safe to drop. If one replica shows scans, you know which workload needs it before removing anything.

### Load balancer misconfiguration

A connection pooler is supposed to round-robin across three replicas, but `compare_nodes` shows `replica-1` handling 5x more traffic than the others. The imbalance detection flags it automatically.

## Automating collection

Stats-only dumps are lightweight and safe for cron:

```sh
# /etc/cron.d/dryrun-stats
0 2 * * * app dryrun dump-schema --source "$REPLICA1_DB" --stats-only --name "replica-1" -o /data/dryrun/r1-stats.json
0 2 * * * app dryrun dump-schema --source "$REPLICA2_DB" --stats-only --name "replica-2" -o /data/dryrun/r2-stats.json
0 2 * * * app dryrun dump-schema --source "$REPLICA3_DB" --stats-only --name "replica-3" -o /data/dryrun/r3-stats.json

# merge after all replicas finish
30 2 * * * app dryrun import /data/dryrun/master.json --stats /data/dryrun/r*.json
```

Run the full dump less frequently, weekly or after migrations. Stats-only dumps can run nightly since they capture the runtime counters that shift daily.

## Stats file format

A stats-only dump produces a `NodeStats` JSON object:

```json
{
  "source": "replica-1",
  "timestamp": "2026-04-01T02:00:17Z",
  "is_standby": true,
  "table_stats": [
    {
      "schema": "public",
      "table": "orders",
      "stats": {
        "reltuples": 1234567.0,
        "relpages": 5123,
        "dead_tuples": 398,
        "last_vacuum": null,
        "last_autovacuum": null,
        "last_analyze": "2026-03-31T14:00:03Z",
        "last_autoanalyze": "2026-04-01T01:12:45Z",
        "seq_scan": 987654,
        "idx_scan": 44998,
        "table_size": 10485760
      }
    }
  ],
  "index_stats": [ ... ],
  "column_stats": [ ... ]
}
```

`is_standby` is auto-detected from `pg_is_in_recovery()`. It controls which nodes contribute vacuum and analyze timestamps during aggregation. Only nodes where `is_standby = false` are considered.
