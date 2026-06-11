# Multi-Node Statistics

Single-node statistics hide problems that only show up when you look across the cluster. The primary might be well-tuned while a reporting replica is doing millions of sequential scans on the same table, or one replica sits idle because the connection pooler routes all traffic elsewhere.

dryrun merges statistics from every node in your cluster into one snapshot, then surfaces the differences.

## Collecting stats

A snapshot is split into three rows in `~/.dryrun/history.db`:

- **`schema`**: DDL (tables, columns, constraints, indexes, partitions, functions, enums, extensions, GUCs).
- **`planner_stats`**: what the planner uses (`reltuples`, `relpages`, `pg_statistic`). Masked per `data-masking-policy.yml` at capture time; see [SECURITY.md](../SECURITY.md).
- **`activity_stats`**: runtime counters (`seq_scan`, `idx_scan`, `n_dead_tup`, `last_vacuum`).

`snapshot take` writes all three rows from the primary, with the activity row labeled `primary`. `snapshot activity` writes one `activity_stats` row per replica, tagged with a `--label`. Activity rows attach to the most recent matching schema by `schema_ref_hash`.

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Primary    │     │  Replica 1  │     │  Replica 2  │
│ snapshot    │     │ snapshot    │     │ snapshot    │
│ take        │     │ activity    │     │ activity    │
└─────┬───────┘     └──────┬──────┘     └──────┬──────┘
      │                    │                    │
      │ schema +           │ activity_stats     │ activity_stats
      │ planner_stats +    │ (label=replica1)   │ (label=replica2)
      │ activity_stats     │                    │
      │ (label=primary)    │                    │
      ▼                    ▼                    ▼
            ~/.dryrun/history.db
       (joined by schema_ref_hash)
```

### Schema + planner stats from the primary

```sh
dryrun --profile primary snapshot take
```

Refuses to run on a standby (`pg_is_in_recovery() = false` required). Writes one `schema` row, one `planner_stats` row (masked per policy), and one `activity_stats` row labeled `primary`.

### Activity stats from replicas

```sh
dryrun --profile replica1 snapshot activity \
  --from "postgres://readonly@replica-1:5432/mydb" --label replica1

dryrun --profile replica2 snapshot activity \
  --from "postgres://readonly@replica-2:5432/mydb" --label replica2

dryrun --profile replica3 snapshot activity \
  --from "postgres://readonly@replica-3:5432/mydb" --label replica3
```

`--label` is required and identifies the node in `describe_table` and `detect`. `snapshot activity` refuses to run on the primary. Each row captures `pg_stat_user_tables`, `pg_stat_user_indexes`, and `stats_reset` for the node, then joins to the latest schema by `schema_ref_hash`. Use `--allow-orphan` when activity arrives before any schema snapshot exists; orphan rows are stored but not reattached when a matching schema lands later.

Activity dumps are small (single-digit MB) and safe for cron. See [Automating collection](#automating-collection).

## Aggregation rules

When activity rows from multiple nodes attach to the same schema, the `MergedActivity` view combines them per table:

| Field | Rule | Why |
|---|---|---|
| `idx_scan_sum` | sum across nodes | Total indexed reads hitting the cluster |
| `idx_scan_per_node` | per-node breakdown | Powers `describe_table`'s node breakdown and routing-imbalance detection |
| `seq_scan_sum` | sum across nodes | Reveals which replicas are doing seq scans |
| `n_dead_tup_sum` | sum across nodes | Worst-case dead-tuple pressure for vacuum decisions |
| `last_vacuum_max` | max timestamp | Autovacuum runs on the primary only; replicas always report null |
| `vacuum_count_sum` | sum across nodes | Total vacuum runs observed |
| `partial` | true if any node is missing a `stats_reset` | Flags counters that aren't comparable |

`reltuples` / `relpages` come from the primary's `planner_stats` row, not from activity rows.

## Analysis tools

All multi-node analysis tools are MCP tools. They read from `~/.dryrun/history.db` via `HistoryStore::get_annotated`, which joins the latest schema with each node's most recent activity row by `schema_ref_hash`.

### describe_table (node breakdown)

`describe_table` includes a per-node activity breakdown for a table, surfacing the
counters that genuinely differ between nodes — `seq_scan`, `idx_scan`, tuple
ins/upd/del, dead tuples, and last vacuum/analyze. Sizing (`reltuples`, `relpages`,
table size) is cluster-wide, captured once from the primary's `planner_stats` row,
so it does not vary per node and is reported once rather than repeated per node.

A node showing far more `seq_scan` than its peers points to a routing problem or a
missing index on that node's workload — surfaced directly by `detect kind=anomalies`.

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

The primary uses indexed lookups on `orders`, but a BI tool connected through `replica-2` runs `SELECT ... WHERE created_at BETWEEN ...` without a covering index. Single-node monitoring on the primary shows nothing wrong. `detect kind=anomalies` (or `describe_table`'s node breakdown) reveals `replica-2` with millions of sequential scans.

Fix: add a covering index for the BI query pattern, or route analytics to a dedicated replica.

### Safe index cleanup

`idx_users_legacy_email` has `idx_scan = 0` on the primary, but a replica might depend on it. `detect unused_indexes` checks all nodes. Zero everywhere, safe to drop. If one replica shows scans, you know which workload needs it before removing anything.

### Load balancer misconfiguration

A connection pooler is supposed to round-robin across three replicas, but `replica-1` handles 5x more traffic than the others. `detect kind=anomalies` flags the imbalance automatically.

## Automating collection

Activity captures are lightweight and safe for cron. Take the primary snapshot first so replica activity rows have a `schema_ref_hash` to attach to:

```sh
# /etc/cron.d/dryrun-stats
0  2 * * * app dryrun --profile primary  snapshot take
15 2 * * * app dryrun --profile replica1 snapshot activity --from "$REPLICA1_DB" --label replica1
15 2 * * * app dryrun --profile replica2 snapshot activity --from "$REPLICA2_DB" --label replica2
15 2 * * * app dryrun --profile replica3 snapshot activity --from "$REPLICA3_DB" --label replica3
```

`snapshot take` already captures the primary's own activity row (labeled `primary`), so there is no separate `snapshot activity` line for the primary, and the command itself refuses to run on the primary. `snapshot take` is idempotent on a quiet schema: repeated runs produce the same `schema_ref_hash`, so re-attaching activity rows is automatic. Run it nightly alongside activity captures, or only after migrations if you want fewer rows in history.

## Snapshot storage

Snapshots live in `~/.dryrun/history.db`, keyed by `(project_id, database_id, kind, schema_ref_hash, node_label)`. Activity rows from `snapshot activity` carry their `--label` in `node_label`; the activity row from `snapshot take` uses `primary`. `is_standby` is auto-detected from `pg_is_in_recovery()` and enforced by the CLI: `take` requires false, `activity` requires true.

To share a snapshot across machines, use `snapshot push` / `snapshot pull`. They move `history.db` byte-for-byte between stores without re-masking or re-transforming. See [SECURITY.md](../SECURITY.md) for the sharing trust model.
