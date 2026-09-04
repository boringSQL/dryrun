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

### Activity stats per node

```sh
dryrun --profile replica1 snapshot activity \
  --from "postgres://readonly@replica-1:5432/mydb" --label replica1

dryrun --profile replica2 snapshot activity \
  --from "postgres://readonly@replica-2:5432/mydb" --label replica2

dryrun --profile replica3 snapshot activity \
  --from "postgres://readonly@replica-3:5432/mydb" --label replica3
```

`--label` is required and identifies the node in `describe_table` and `detect`. `snapshot activity` runs against either role — `pg_stat_user_tables` is per-node and meaningful on a primary too, so a primary's activity can be re-captured without re-running `snapshot take`. A label whose previous capture recorded the other role is refused with a role-change error, since that usually means `--label` is pointed at a rotating endpoint and two nodes' counters would append into one series; pass `--allow-role-change` after a genuine promotion or failover. Each row captures `pg_stat_user_tables`, `pg_stat_user_indexes`, and `stats_reset` for the node, then joins to the latest schema by `schema_ref_hash`. Use `--allow-orphan` when activity arrives before any schema snapshot exists; orphan rows are stored but not reattached when a matching schema lands later.

Each capture also records which server answered — `pg_postmaster_start_time()` and `inet_server_addr()` — so a label that stops pointing at one machine can be noticed. A one-way change is reported as a `notice:` (a restart, or a node that was replaced). A fingerprint that *recurs* after a different one intervened is reported as a `warning:`: that is a rotating endpoint — a reader endpoint, a Kubernetes Service, a `kubectl port-forward` reconnecting to whichever pod answers — and it means two servers' cumulative counters are interleaving under one label, so deltas across those rows are wrong. Neither ever fails a capture. If the label deliberately names a pool, pass `--allow-rotation` to silence the warning; the one-way notice still prints. `system_identifier` cannot do this job: replicas inherit the primary's through base backup, so it is identical across a cluster.

Each activity row also carries four database-scoped, best-effort sections, independent of any table: `pg_stat_database` counters for the connected database (deadlocks, temp spill, commit/rollback volume, buffer hit ratio, conflicts, checksum failures), `pg_replication_slots` (per-slot activity and WAL retention risk; `safe_wal_size` is null under the default `max_slot_wal_keep_size`, and snapshots captured below the PG14 floor may lack `wal_status`), `pg_stat_replication` peers (application_name, client_addr, state, sync_state, LSN positions, and lag), and checkpoint counters normalized from `pg_stat_checkpointer` (PG17+) or `pg_stat_bgwriter` (older). All four are CLUSTER facts riding a per-node, per-database document — a multi-database cluster reports identical checkpoint counters once per tracked database, a standby's own replication-slot list is normally empty (its slots, if any, live on the primary), and peers are normally empty on a standby unless it is itself feeding further standbys via cascading replication. A read failure on any of the four leaves it absent on the wire, never a substitute zero.

The peer section is captured on both primaries and standbys; the lag reference LSN is chosen in SQL by `pg_is_in_recovery()` — `pg_current_wal_lsn()` on a primary, `pg_last_wal_receive_lsn()` on a standby — so cascading replicas are visible without aborting the capture. Lag fields are NULL for an idle standby that has not sent a recent replay message, and state/LSN fields are NULL for a `state='backup'` walsender (a running `pg_basebackup`), so a nil field always means "unknown", never "no lag". The `pg_read_all_stats` role is required for full peer visibility; without it `pg_stat_replication` still returns one row per walsender but with every column NULL except `pid`, which dryrun records as rows with an empty `application_name` and all other fields absent. So `replication_peers_read_ok=true` with a list of such blank rows signals missing privilege, while a genuinely empty list means no standbys. To join peer rows to `describe_table` / `detect` node breakdowns, set each replica's `--label` to its `application_name` (the `primary_conninfo` setting on the replica). Peer rows also carry `client_addr` (replica IP or NULL for Unix sockets); that is infrastructure metadata, not table data, and is not routed through the column-oriented data-masking policy.

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
detect(kind = "bloated_tables")
detect(kind = "vacuum_health")
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

### detect kind=vacuum_health

Reports only tables with an autovacuum concern; a table with nothing wrong does not appear. Analysis uses aggregated dead tuple counts but primary-only vacuum timestamps. Replicas don't run autovacuum, so their timestamps are always null. Using dead tuple counts from all nodes and vacuum timing from the primary gives accurate distance-to-trigger calculations.

## Practical scenarios

### Reporting replica with seq scans

The primary uses indexed lookups on `orders`, but a BI tool connected through `replica-2` runs `SELECT ... WHERE created_at BETWEEN ...` without a covering index. Single-node monitoring on the primary shows nothing wrong. `detect kind=anomalies` (or `describe_table`'s node breakdown) reveals `replica-2` with millions of sequential scans.

Fix: add a covering index for the BI query pattern, or route analytics to a dedicated replica.

### Safe index cleanup

`idx_users_legacy_email` has `idx_scan = 0` on the primary, but a replica might depend on it. `detect unused_indexes` checks all nodes. Zero everywhere, safe to drop. If one replica shows scans, you know which workload needs it before removing anything.

### Load balancer misconfiguration

A connection pooler is supposed to round-robin across three replicas, but `replica-1` handles 5x more traffic than the others. `detect kind=anomalies` flags the imbalance automatically.

## Declaring the fleet

Wiring every node's URL into cron by hand does not scale past a couple of replicas, and a missed variable fails silently until that node's next capture. Describe the fleet once instead:

```toml
[[node]]
name     = "primary"
role     = "primary"
url      = "service=dryrun-primary"
streams  = ["planner", "activity", "query"]
interval = "1h"

[[node]]
name     = "replica-eu"
role     = "standby"
url      = "service=dryrun-replica-eu"
streams  = ["activity", "query"]
interval = "30m"

[[node]]
name     = "read-pool"
url      = "service=dryrun-read-pool"
streams  = ["query"]
interval = "15m"
pool     = true
```

`url = "service=name"` is preferred: the entry lives in `~/.pg_service.conf` (or `$PGSERVICEFILE`) on the capture host and the password in `~/.pgpass`, so `dryrun.toml` never carries a password — or even a variable name — and stays committable. Where a service file is impractical, `url_env` names an environment variable and `url` may hold a `${VAR}` reference instead; either way nothing secret lands in the file. `role` is asserted against the node at capture time; `auto` (the default) accepts whatever it finds. Omit `streams` and the detected role decides: a standby has no schema of its own and its planner stats mirror the primary's, so it captures activity and query only.

`pool = true` says the label names a read pool rather than one machine. Members rotate by design there, so the identity-drift warning is suppressed for that label — see the fingerprint paragraph above. Do not set it on a label that is supposed to be one node: the warning is the only thing that tells you two servers' counters are interleaving.

Then capture the whole fleet:

```sh
dryrun snapshot capture --all          # every node, every configured stream
dryrun snapshot capture --all --due    # only what its interval says is due
dryrun snapshot capture --node primary # one node from the config
dryrun snapshot capture --from "$URL" --label replica-3 --streams query
```

### Checking the wiring first

A fleet's connection details are the part that goes wrong, and a wrong one is usually silent: a URL copied from the primary into the replica block captures one server twice, an unset `url_env` fails only that node's next tick, a `role` left over from a failover fails every tick. `--check` is the preflight — it connects to every target, runs `SELECT 1`, and reports what a capture would do, without capturing anything:

```sh
dryrun snapshot capture --all --check
```

```
NODE       STATUS  ROLE      PG      DATABASE      SERVER           STREAMS
analytics  FAIL    primary   17.10   analytics     10.0.0.1         activity
primary    ok      primary   17.10   app           10.0.0.1         activity,query
replica-1  FAIL    primary   17.10   app           10.0.0.1         activity,query
replica-2  FAIL    -         -       -             -                -

warning: analytics, primary and replica-1 are the same server (10.0.0.1), different databases.

error: analytics: capture does not match this project's snapshot history: database name differs (history "app", capture "analytics").
       The connection may point at the wrong database, or you may be in the wrong project directory.
       Re-check --db / DATABASE_URL.
       If the identity legitimately changed, re-baseline with `dryrun snapshot take --force`
       on the primary; --force here bypasses this run only.
error: primary and replica-1 point at 10.0.0.1 (database app): one server under 2 labels. --all captures it once per label, and the fleet view shows nodes that are the same server.
error: replica-1: [[node]] replica-1 declares role standby, but this node is a primary;
  swap the roles in dryrun.toml, or set role = auto
error: replica-2: url_env REPLICA2_URL is unset in this environment
```

Each node is checked on its own, so one unreachable node still leaves a full report rather than aborting at the first failure, and `--check-timeout` (10s by default) bounds a node that neither answers nor refuses. Every check is one that capture itself makes: the privileged-role refusal, the declared vs. detected role, the role a label was last captured under, the cluster and database identity against this project's schema baseline (waived by `--force`), whether this label's last capture came from a different server, `pg_stat_statements` for the query stream, a schema snapshot for every stream (waived by `--allow-orphan`, except for planner, which annotates against the snapshot itself), the stream names, the row cap, and the masking policy `require_masks` demands. Two checks are only visible across the fleet — two labels resolving to one server (fatal; identified by the server's boot time, not by the URL text, so a hostname and its IP are still caught, and only a warning when one of the labels is a `pool = true` endpoint that lands on a member by design) and a fleet spread over several databases, whose stats all land under one `database_id`. Failures exit nonzero; warnings do not. No capture lock is taken, so the preflight is safe to run while a cron capture is in flight.

`--due` is what makes one cron line implement every cadence:

```sh
*/5 * * * * app dryrun snapshot capture --all --due
```

Cadence is decided before connecting, so a tick with nothing due opens no database connections at all. One node failing logs and continues so a single unreachable replica does not strand the fleet, and the command exits nonzero if any node failed. With `--push`, a partially failed run still pushes what captured. A capture lock stops an overlapping tick from stacking a second set of production connections. A crashed run's lock is not reclaimed automatically — taking a lock over by path cannot be made race-free, so two runs that both judged it stale would both proceed — and the next run reports the lock's age and the command to clear it.

`--due` keys off the newer of two things: the newest stored row, and the last time this host attempted that stream. The attempt half matters because `planner` and `query` deduplicate — an unchanged capture stores no row, so a clock read from rows alone would leave a quiet stream due on every tick forever. Attempt records are local: they say what this host did, never what the project contains, so they are neither pushed nor pulled. A stream that errors is not recorded, so a broken node retries on the next tick instead of going quiet for a full interval.

Two limits worth knowing. A `snapshot pull` writes rows into the same tables, so pulling can make a node look freshly captured and skip one interval — it self-heals on the next tick. And a timestamp dated in the future (a peer with a skewed clock) is ignored for cadence rather than trusted, since it is not evidence that this host captured anything. Note too that a manual `dryrun snapshot capture --streams planner` advances the cron's clock for that stream, since it is the same clock.

## Reading what changed

`snapshot diff --kind query` subtracts two captures of one node:

```sh
dryrun snapshot diff --latest --kind query --node primary
```

```
Query stats: node=primary, window 21h16m22s (2026-08-19 09:37:59 -> 2026-08-20 06:54:21)
  +15190558 calls, +4348376 ms total

  STATUS           CALLS       TIME(ms) MEAN(ms)           QUERY
  grew           +337262        +623588 1.85<-1.78         SELECT time_entry.id, time_entry.type, time_ent…
  grew              +330        +372305 1128.20<-463.64    WITH RECURSIVE project_hierarchy AS (SELECT id,…
  new             +30963        +179610 5.80               WITH excluded_projects AS MATERIALIZED (SELECT …
```

The mean column is the point. `pg_stat_statements.mean_exec_time` averages since pgss last reset, so a query that got slower today is invisible in it; the window mean is `Δtotal_time / Δcalls` over the two captures, and `12.00<-2.00` reads as "12ms per call this window, up from 2ms". In the example above a recursive CTE running 330 times went from 464ms to 1128ms a call — a regression that the totals, dominated by a query running 3.9 million times at 0.03ms, would never show.

Counters are cumulative, so the diff refuses rather than guesses. It will not subtract across a `pg_stat_statements` reset, a qshape regrouping, a capture-rule change, or two different labels, and says which. The recorded server fingerprint is consulted too: two captures whose `inet_server_addr` differ are two machines under one label, and the diff refuses them. A changed `pg_postmaster_start_time` alone is reported but not refused, since pg_stat_statements survives a clean restart; when the address is unknown (a Unix socket, or a tunnel that shows every member as 127.0.0.1) the change is a caveat rather than a refusal. Any counter going backwards makes that shape unsubtractable: an entry groups several queryids, so one member being evicted can pull time down while calls rise. If the older capture hit its row cap, shapes that appear in the newer one are marked `truncated` rather than `new` — they may have been running below the cap all along — and are left out of the totals.

See [query-stats.md](query-stats.md) for the full capture and diff reference.

Statuses: `grew`, `shrank`, `flat`, `new`, `gone` (absent from an uncapped newer capture), `evicted` (absent from a capped one, so it may still be running), `reset`, `truncated`. `--json` carries every shape; the console shows movers only.

## Automating collection

Activity captures are lightweight and safe for cron. Take the primary snapshot first so replica activity rows have a `schema_ref_hash` to attach to:

```sh
# /etc/cron.d/dryrun-stats
0  2 * * * app dryrun snapshot take            # schema + planner, on the primary
*/5 * * * * app dryrun snapshot capture --all --due
```

With [[node]] blocks the second line covers every node at its own interval. Without them, each node needs its own line:

```sh
15 2 * * * app dryrun --profile replica1 snapshot activity --from "$REPLICA1_DB" --label replica1
15 2 * * * app dryrun --profile replica2 snapshot activity --from "$REPLICA2_DB" --label replica2
```

`snapshot take` already captures the primary's own activity row (labeled `primary`), so the cron above has no separate `snapshot activity` line for it. Adding one is legal — `snapshot activity` accepts a primary — and is how you refresh primary activity counters more often than you re-snapshot the schema; reuse the same `--label` so both land in one series. `snapshot take` is idempotent on a quiet schema: repeated runs produce the same `schema_ref_hash`, so re-attaching activity rows is automatic. Run it nightly alongside activity captures, or only after migrations if you want fewer rows in history.

## Snapshot storage

Snapshots live in `~/.dryrun/history.db`, keyed by `(project_id, database_id, kind, schema_ref_hash, node_label)`. Activity rows from `snapshot activity` carry their `--label` in `node_label`; the activity row from `snapshot take` uses `primary`. `is_standby` is auto-detected from `pg_is_in_recovery()` and recorded on every row. `take` still requires a primary; `activity` accepts either role and instead checks the label: if the newest row for that label recorded the other role, the capture is refused unless `--allow-role-change` is passed.

To share a snapshot across machines, use `snapshot push` / `snapshot pull`. They move `history.db` byte-for-byte between stores without re-masking or re-transforming. See [SECURITY.md](../SECURITY.md) for the sharing trust model.
