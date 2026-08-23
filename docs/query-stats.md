# Query stats

The `query` stream captures `pg_stat_statements` per node and diffs two captures against each other. Fleet capture is covered in [multi-node-stats.md](multi-node-stats.md), config keys in [dryrun-toml.md](dryrun-toml.md).

## Capturing

```sh
dryrun snapshot capture --node replica-1 --streams query
dryrun snapshot capture --from "$URL" --label replica-1 --streams query
```

`capture` is the command to use. Given `--node` it reads a `[[node]]` block from `dryrun.toml` and applies the fleet machinery: role assertion, the single-flight lock, `--due`, `--push`. Given `--from` and `--label` it captures a node that is not in config.

`dryrun snapshot query-stats --from "$URL" --label replica-1` still works and writes the same row through the same code, without the lock or the interval check. It is superseded by `capture --from` and kept for existing scripts.

Each row stores the raw `pg_stat_statements` rows, the grouped shapes with their rollups, and one `pg_stat_statements_info` read (`stats_reset`, `dealloc`). On PG13 the info part is absent rather than zero. A zero would look like a reset epoch that never happened.

The fetch is capped at 500 rows by default; `--query-stats-limit` or `[query_stats].row_cap` changes it. A capped capture records `raw_rows` and `row_cap`. Without that, a diff couldn't tell a genuinely new shape from one that only made it under the cap this time.

Grouping is done by qshape. ORMs generate variants of the same query, and qshape collapses them into one shape with a per-node rollup. Every row records two versions: `qshape_version` for the normalizer, `capture_rule_version` for what the fetch included (toplevel filter, cap semantics).

The content digest comes from the raw rows, not the grouped shapes. A better normalizer in a future release will regroup old captures but won't change their hashes, so upgrading dryrun doesn't invalidate history. Only toplevel statements are captured; the `nested_calls` field on old rows predates the filter.

Stored query text is the canonical shape, not the raw SQL. Labels from a leading comment go through qshape's tag policy. A deny-listed or literal-shaped value is recorded as a key with a value count. The value itself is not stored.

## Diffing

```sh
dryrun snapshot diff --latest --kind query --node primary
```

pgss counters are cumulative, so the diff subtracts two captures. Where subtraction would produce nonsense it refuses and says why:

- different labels
- qshape or capture-rule version changed between the captures
- pgss was reset inside the window (`stats_reset` moved)
- a counter went backwards — an entry groups several queryids, and one member being evicted can pull time down while calls rise

Statuses: `grew`, `shrank`, `flat`, `new`, `gone` (absent from an uncapped newer capture), `evicted` (absent from a capped one, so it may still be running), `reset`, `truncated` (the older capture hit its row cap, so "new" shapes may only be newly visible). `reset` and `truncated` rows don't count into the headline totals.

The mean column is the window mean, Δtime/Δcalls between the two captures, printed as `12.00<-2.00`: 12ms per call this window, 2ms before. This matters because `mean_exec_time` averages over everything since pgss last reset, and a query that got slower this week barely moves that number. A rising `dealloc` between captures means pgss was evicting entries under pressure; the diff reports it instead of letting evictions look like regressions.

Console output shows movers only, 25 rows at most. `--json` has every shape.

The diff also checks which server answered. Two captures whose `inet_server_addr` differ are two machines under one label, and it refuses them. A changed postmaster start time alone is reported but not refused, since pg_stat_statements survives a clean restart. When the address is unknown — a Unix socket, or a tunnel where every member shows 127.0.0.1 — the change is a caveat rather than a refusal.

## Storage

Rows go to `.dryrun/history.db` under `(project_id, database_id, kind, schema_ref_hash, node_label)`. Query rows dedup by content hash, so an idle node adds nothing. Same-second captures order by row id; `--latest` and `--latest~1` never pick the same row. `snapshot list --node X --kind query` shows the series.
