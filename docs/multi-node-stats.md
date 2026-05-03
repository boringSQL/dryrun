# Multi-Node Statistics

Single-node statistics hide problems that only show up when you look across the cluster. The primary might be well-tuned while a reporting replica is doing millions of sequential scans on the same table, or one replica sits idle because the connection pooler routes all traffic elsewhere.

dryrun merges statistics from every node in your cluster into one snapshot, then surfaces the differences.

## Collecting stats

A snapshot in v0.6.0 is split into three rows in `~/.dryrun/history.db`:

- **`schema`**: DDL (tables, columns, constraints, indexes, partitions, functions, enums, extensions, GUCs).
- **`planner_stats`**: what the planner uses (`reltuples`, `relpages`, `pg_statistic`).
- **`activity_stats`**: runtime counters (`seq_scan`, `idx_scan`, `n_dead_tup`, `last_vacuum`).

`snapshot take` writes `schema` + `planner_stats` from the primary. `snapshot activity` writes one `activity_stats` row per replica, tagged with a `--label`. Activity rows attach to the most recent matching schema by `schema_ref_hash`.

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Primary    │     │  Replica 1  │     │  Replica 2  │
│ snapshot    │     │ snapshot    │     │ snapshot    │
│ take        │     │ activity    │     │ activity    │
└─────┬───────┘     └──────┬──────┘     └──────┬──────┘
      │                    │                    │
      │ schema +           │ activity_stats     │ activity_stats
      │ planner_stats      │ (label=replica1)   │ (label=replica2)
      ▼                    ▼                    ▼
            ~/.dryrun/history.db
       (joined by schema_ref_hash)
```

### Schema + planner stats from the primary

```sh
dryrun --profile primary snapshot take
```

Refuses to run on a standby (`pg_is_in_recovery() = false` required). Writes one `schema` row and one `planner_stats` row.

### Activity stats from replicas

```sh
dryrun --profile replica1 snapshot activity \
  --from "postgres://readonly@replica-1:5432/mydb" --label replica1

dryrun --profile replica2 snapshot activity \
  --from "postgres://readonly@replica-2:5432/mydb" --label replica2

dryrun --profile replica3 snapshot activity \
  --from "postgres://readonly@replica-3:5432/mydb" --label replica3
```

`--label` is required and identifies the node in `compare_nodes` and `detect`. `snapshot activity` refuses to run on the primary. Each row captures `pg_stat_user_tables`, `pg_stat_user_indexes`, and `stats_reset` for the node, then joins to the latest schema by `schema_ref_hash`. Use `--allow-orphan` when activity arrives before any schema snapshot exists; orphan rows are stored but not reattached when a matching schema lands later.

Activity dumps are small (single-digit MB) and safe for cron. See [Automating collection](#automating-collection).

## Aggregation rules

When activity rows from multiple nodes attach to the same schema, the `MergedActivity` view combines them per table:

| Field | Rule | Why |
|---|---|---|
| `idx_scan_sum` | sum across nodes | Total indexed reads hitting the cluster |
| `idx_scan_per_node` | per-node breakdown | Powers `compare_nodes` and routing-imbalance detection |
| `seq_scan_sum` | sum across nodes | Reveals which replicas are doing seq scans |
| `n_dead_tup_sum` | sum across nodes | Worst-case dead-tuple pressure for vacuum decisions |
| `last_vacuum_max` | max timestamp | Autovacuum runs on the primary only; replicas always report null |
| `vacuum_count_sum` | sum across nodes | Total vacuum runs observed |
| `partial` | true if any node is missing a `stats_reset` | Flags counters that aren't comparable |

`reltuples` / `relpages` come from the primary's `planner_stats` row, not from activity rows.

## Analysis tools

All multi-node analysis tools are MCP tools. They read from `~/.dryrun/history.db` via `HistoryStore::get_annotated`, which joins the latest schema with each node's most recent activity row by `schema_ref_hash`.

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

Activity captures are lightweight and safe for cron. Take the primary snapshot first so activity rows have a `schema_ref_hash` to attach to:

```sh
# /etc/cron.d/dryrun-stats
0  2 * * * app dryrun --profile primary  snapshot take
15 2 * * * app dryrun --profile replica1 snapshot activity --from "$REPLICA1_DB" --label replica1
15 2 * * * app dryrun --profile replica2 snapshot activity --from "$REPLICA2_DB" --label replica2
15 2 * * * app dryrun --profile replica3 snapshot activity --from "$REPLICA3_DB" --label replica3
```

`snapshot take` is idempotent on a quiet schema; repeated runs produce the same `schema_ref_hash`, so re-attaching activity rows is automatic. Run it nightly alongside activity captures, or only after migrations if you want fewer rows in history.

## Snapshot storage

Snapshots live in `~/.dryrun/history.db`, keyed by `(project_id, database_id, kind, schema_ref_hash, node_label)`. Activity rows from `snapshot activity` carry their `--label` in `node_label`; rows from `snapshot take` use an empty label. `is_standby` is auto-detected from `pg_is_in_recovery()` and enforced by the CLI: `take` requires false, `activity` requires true.

To export a snapshot for sharing or archiving:

```sh
dryrun snapshot export
```

Writes `{project_id}/{database_id}/{timestamp}-{hash}.json.zst`. The zstd-compressed JSON contains the schema row plus all attached planner and activity rows.
