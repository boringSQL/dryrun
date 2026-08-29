# dryrun - Tutorial

dryrun gives your AI assistant (or CLI) full visibility into your PostgreSQL schema, query plans, and migration risks. This tutorial covers three workflows, pick the one that fits.

## Build

```sh
go build -o bin/dryrun ./cmd/dryrun
```

Binary: `bin/dryrun`

---

## Part A: Offline workflow (no database needed)

This is the **recommended starting point** for evaluation. Someone captures the schema once and pushes it to a shared directory; everyone else pulls.

### 1. Get a snapshot

Whoever has credentials captures and pushes — the directory can live in the repo, or anywhere both sides can reach:

```sh
export DATABASE_URL="postgres://readonly_user:pass@host:5432/your_db"
dryrun init --db "$DATABASE_URL"
dryrun snapshot push --to-path ./snapshots
```

They commit `dryrun.toml` along with `./snapshots` — it carries the `database_id` the snapshot is keyed under, and a teammate who runs `dryrun init` without `--db` would derive a different one and pull nothing.

### 2. Pull it

```sh
dryrun snapshot pull --from-path ./snapshots
```

This loads the snapshot into `.dryrun/history.db`, which is the only schema source dryrun reads. Add `.dryrun/` to `.gitignore`.

Unlike a plain JSON export, a pushed snapshot carries planner and activity stats alongside the schema, so the tools that need sizing and vacuum data work offline too. `dryrun dump-schema` still writes JSON when a human or an agent wants readable text — it is an export, not an input.

### 3. Lint

```sh
dryrun lint
```

Checks naming conventions, PK types, varchar vs text, timestamps, FK indexes, etc. Works entirely from the saved snapshot.

```sh
# JSON output for CI
dryrun lint --json --pretty

# filter to one schema
dryrun lint --schema-name public
```

Customize via `dryrun.toml`:

```toml
[conventions]
table_name = "snake_singular"
require_timestamps = true
prefer_text_over_varchar = true

[conventions.disabled_rules]
rules = ["naming/index_pattern"]
```

### 4. MCP server (offline)

Install in Claude Code, it reads the newest snapshot from `.dryrun/history.db`:

```sh
claude mcp add dryrun -- dryrun mcp-serve
```

No DB credentials needed. Available tools: `list_tables`, `describe_table`, `search_schema`, `validate_query`, `check_migration`, `lint_schema`.

Not available without a live DB: `explain_query`, `advise`, `check_drift`.

---

## Part B: Online workflow (live database)

For full capabilities including EXPLAIN and schema refresh.

### 1. Probe the connection

```sh
export DATABASE_URL="postgres://readonly_user:pass@host:5432/your_db"
dryrun probe --db "$DATABASE_URL"
```

Expected output:

```
PostgreSQL 16.3
  PostgreSQL 16.3 on x86_64-pc-linux-gnu ...
Privileges:
  pg_catalog:           ok
  information_schema:   ok
  pg_stat_user_tables:  ok
```

### 2. Initialize

```sh
dryrun init --db "$DATABASE_URL"
```

Creates `dryrun.toml` and the `.dryrun/` directory, and captures the first snapshot. Snapshot history lives in `.dryrun/history.db`, keyed by `(project_id, database_id)`. If a `data-masking-policy.yml` resolves, `init` masks planner stats in-process before writing; for projects with PII, set `require_masks = true` in `dryrun.toml` to fail closed when the policy is missing. See [SECURITY.md](SECURITY.md).

### 3. Snapshots and diffing

```sh
dryrun snapshot take --db "$DATABASE_URL"          # saves to history
dryrun snapshot list --db "$DATABASE_URL"          # show all snapshots
dryrun snapshot diff --db "$DATABASE_URL" --latest --pretty  # diff last saved vs live
```

### 4. Profiles

Instead of passing `--db` every time:

```toml
# dryrun.toml
[default]
profile = "development"

[profiles.development]
db_url = "${DEV_DATABASE_URL}"

[profiles.staging]
db_url = "${STAGING_DATABASE_URL}"
database_id = "staging"

[profiles.production]
db_url = "${PROD_DATABASE_URL}"
```

```sh
dryrun profile list
dryrun --profile staging lint
```

### 5. MCP server (live)

```sh
claude mcp add dryrun -- env DATABASE_URL=postgres://user:pass@host:5432/db dryrun mcp-serve
```

All tools available including EXPLAIN ANALYZE (runs in rolled-back transactions, safe on read replicas).

---

## Part C: Multi-node workflow

For setups with one primary and N replicas serving different query patterns. Activity counters (`seq_scan`, `idx_scan`, `n_dead_tup`) differ per node and only live where the queries actually run, on the replicas. dryrun captures schema + planner stats from the primary and activity stats from each replica, then aggregates them.

A snapshot is split into three rows in `.dryrun/history.db`: `schema`, `planner_stats`, `activity_stats`. `snapshot take` writes all three from the primary, with the activity row labeled `primary`. `snapshot activity` writes one additional `activity_stats` row per replica, tagged with `--label`. Planner stats are masked per `data-masking-policy.yml` at capture time; see [SECURITY.md](SECURITY.md).

### 1. Schema + planner + activity from the primary

```sh
dryrun --profile primary snapshot take
```

Refuses to run on a standby. Writes `schema` (DDL), `planner_stats` (`reltuples`, `relpages`, `pg_statistic`; masked per policy), and one `activity_stats` row labeled `primary`.

### 2. Activity stats from each replica

```sh
dryrun --profile replica1 snapshot activity --from "$REPLICA1_URL" --label replica1
dryrun --profile replica2 snapshot activity --from "$REPLICA2_URL" --label replica2
dryrun --profile replica3 snapshot activity --from "$REPLICA3_URL" --label replica3
```

`--label` is required and identifies the node in `describe_table` and `detect`. `snapshot activity` refuses to run on the primary. Activity rows attach to the most recent `schema` row by `schema_ref_hash`; pass `--allow-orphan` to capture before a schema exists.

### 3. Define profiles for repeatable runs

```toml
# dryrun.toml
[project]
id = "myapp"

[profiles.primary]
db_url = "${PRIMARY_DATABASE_URL}"

[profiles.replica1]
db_url = "${REPLICA1_DATABASE_URL}"

[profiles.replica2]
db_url = "${REPLICA2_DATABASE_URL}"
```

### 4. Cron

Schema changes rarely; activity counters shift daily. Capture each on its own schedule:

```sh
# /etc/cron.d/dryrun-stats
0  2 * * * app dryrun --profile primary  snapshot take
15 2 * * * app dryrun --profile replica1 snapshot activity --from "$REPLICA1_URL" --label replica1
15 2 * * * app dryrun --profile replica2 snapshot activity --from "$REPLICA2_URL" --label replica2
```

### 5. Verify

```sh
dryrun snapshot list
```

Each row prints its `kind` (`schema` / `planner_stats` / `activity_stats`), `node_label` for activity rows, and the `schema_ref_hash` linking activity to schema. The MCP `describe_table` node breakdown then exposes per-node `idx_scan` for any table.

---

## Part D: MCP setup reference

### Claude Code (recommended)

```sh
# offline (reads .dryrun/history.db in the current project)
claude mcp add dryrun -- dryrun mcp-serve

# live database
claude mcp add dryrun -- env DATABASE_URL=postgres://user:pass@host:5432/db dryrun mcp-serve

# project-scope (creates .mcp.json, shared with the team via version control)
claude mcp add --scope project dryrun -- dryrun mcp-serve
```

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "dryrun": {
      "command": "dryrun",
      "args": ["mcp-serve"],
      "cwd": "/path/to/your/project"
    }
  }
}
```

### SSE mode (remote / Docker)

```sh
DATABASE_URL="$DB" dryrun mcp-serve --transport sse --port 3000
```

Connect your MCP client to `http://host:3000/sse`.

---

## Part E: Tool reference

| Tool | Needs DB? | Description |
|------|-----------|-------------|
| `list_tables` | No | List all tables with row estimates and comments |
| `describe_table` | No | One table in full: columns, constraints, indexes, stats; `detail=relations` for foreign keys, `detail=stats` adds vacuum |
| `search_schema` | No | Search across table/column names, comments, constraints |
| `validate_query` | No | Parse SQL, check table/column existence, detect anti-patterns |
| `check_migration` | No | Migration safety: lock types, rewrite risk, safe alternatives |
| `lint_schema` | No | Convention checks: naming, types, constraints, timestamps |
| `snapshot_diff` | No\* | Compare two snapshots: schema, planner, activity, query drift |
| `detect` | No | Health checks: stale stats, unused indexes, seq-scan anomalies, bloat, vacuum health |
| `analyze_plan` | No | Analyze a pre-existing EXPLAIN JSON plan |
| `advise` | Hybrid | Query review: validation + index suggestions offline, plus plan review with a DB |
| `list_top_queries` | No | Captured pg_stat_statements shapes, ranked, per node |
| `explain_query` | **Yes** | EXPLAIN with structured plan and warnings |
| `check_drift` | **Yes** | Compare live database schema against saved snapshot |
| `columnar_report` | **Yes** | AlloyDB only: columnar-engine state and findings |

\* `snapshot_diff` needs snapshot history; without a live DB it compares saved snapshots only.

---

## Part F: Troubleshooting

**"connection refused"** - Check your connection string. If using Docker, PG host may differ from `localhost`.

**"permission denied for pg_stat_user_tables"** - Grant `pg_monitor` to your user:
```sql
GRANT pg_monitor TO your_readonly_user;
```

**EXPLAIN ANALYZE times out** - The query actually runs (rolled back). Use `analyze=false` (default) for cost estimates only.

**Schema is stale** - Re-run `dryrun snapshot take` (or `snapshot pull`). The server picks the new snapshot up on the next tool call.

**MCP connection issues** - Server logs to stderr, MCP protocol to stdout. For SSE mode, test with `curl http://host:port/sse`.

**"invalid schema JSON"** - The file must be a valid SchemaSnapshot. If you renamed fields or edited by hand, re-dump from the database.

**Multi-node stats not showing** - Run `dryrun snapshot list` and confirm you see a `schema` row plus `activity_stats` rows for each node sharing the same `schema_ref_hash`. The primary contributes its own `activity_stats` row (labeled `primary`) via `snapshot take`; each replica contributes one via `snapshot activity --label ...`. Activity captured before any schema exists needs `--allow-orphan` and won't reattach automatically.


