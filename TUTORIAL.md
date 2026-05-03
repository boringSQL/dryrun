# dryrun - Tutorial

dryrun gives your AI assistant (or CLI) full visibility into your PostgreSQL schema, query plans, and migration risks. This tutorial covers three workflows, pick the one that fits.

## Build

```sh
cargo build --release
```

Binary: `target/release/dryrun`

---

## Part A: Offline workflow (no database needed)

This is the **recommended starting point** for evaluation. Someone dumps the schema once, everyone else works from the JSON file.

### 1. Get a schema file

Either from a teammate, CI, or dump it yourself:

```sh
export DATABASE_URL="postgres://readonly_user:pass@host:5432/your_db"
dryrun dump-schema --source "$DATABASE_URL" --pretty --name "production" -o schema.json
```

### 2. Import it

```sh
dryrun import schema.json
```

This validates the JSON and copies it to `.dryrun/schema.json`. Add `.dryrun/` to `.gitignore`.

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

Install in Claude Code, it auto-discovers `.dryrun/schema.json`:

```sh
claude mcp add dryrun -- dryrun mcp-serve
```

No DB credentials needed. Available tools: `list_tables`, `describe_table`, `search_schema`, `find_related`, `validate_query`, `check_migration`, `lint_schema`.

Not available without a live DB: `explain_query`, `advise`, `refresh_schema`.

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

Creates `dryrun.toml`, the `.dryrun/` directory, and `.dryrun/schema.json`. Snapshot history lives in `~/.dryrun/history.db` (shared across projects, keyed by `(project_id, database_id)`).

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
schema_file = ".dryrun/staging-schema.json"

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

In v0.6.0 a snapshot is split into three rows in `~/.dryrun/history.db`: `schema`, `planner_stats`, `activity_stats`. `snapshot take` writes the first two from the primary; `snapshot activity` writes one `activity_stats` row per replica, tagged with `--label`.

### 1. Schema + planner stats from the primary

```sh
dryrun --profile primary snapshot take
```

Refuses to run on a standby. Writes `schema` (DDL) + `planner_stats` (`reltuples`, `relpages`, `pg_statistic`) to history.

### 2. Activity stats from each replica

```sh
dryrun --profile replica1 snapshot activity --from "$REPLICA1_URL" --label replica1
dryrun --profile replica2 snapshot activity --from "$REPLICA2_URL" --label replica2
dryrun --profile replica3 snapshot activity --from "$REPLICA3_URL" --label replica3
```

`--label` is required and identifies the node in `compare_nodes` and `detect`. `snapshot activity` refuses to run on the primary. Activity rows attach to the most recent `schema` row by `schema_ref_hash`; pass `--allow-orphan` to capture before a schema exists.

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

Each row prints its `kind` (`schema` / `planner_stats` / `activity_stats`), `node_label` for activity rows, and the `schema_ref_hash` linking activity to schema. The MCP `compare_nodes` tool then exposes per-node `idx_scan` for any table.

---

## Part D: MCP setup reference

### Claude Code (recommended)

```sh
# offline (auto-discover .dryrun/schema.json)
claude mcp add dryrun -- dryrun mcp-serve

# offline (explicit schema file)
claude mcp add dryrun -- dryrun mcp-serve --schema-file /path/to/schema.json

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
      "args": ["mcp-serve", "--schema-file", "/path/to/schema.json"]
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
| `describe_table` | No | Full table detail: columns, constraints, indexes, stats |
| `search_schema` | No | Search across table/column names, comments, constraints |
| `find_related` | No | Foreign key relationships with sample JOIN patterns |
| `validate_query` | No | Parse SQL, check table/column existence, detect anti-patterns |
| `check_migration` | No | Migration safety: lock types, rewrite risk, safe alternatives |
| `lint_schema` | No | Convention checks: naming, types, constraints, timestamps |
| `schema_diff` | No\* | Compare snapshots for schema changes |
| `vacuum_health` | No | Autovacuum analysis with effective settings and recommendations |
| `detect` | No | Health checks: stale stats, unused indexes, seq-scan anomalies |
| `compare_nodes` | No | Per-node breakdown for a specific table with anomaly detection |
| `analyze_plan` | No | Analyze a pre-existing EXPLAIN JSON plan |
| `advise` | Hybrid | Comprehensive query analysis: EXPLAIN + anti-patterns + index suggestions |
| `explain_query` | **Yes** | EXPLAIN with structured plan and warnings |
| `check_drift` | **Yes** | Compare live database schema against saved snapshot |
| `refresh_schema` | **Yes** | Re-introspect the live database |

\* `schema_diff` needs snapshot history; without live DB it compares saved snapshots only.

---

## Part F: Troubleshooting

**"connection refused"** - Check your connection string. If using Docker, PG host may differ from `localhost`.

**"permission denied for pg_stat_user_tables"** - Grant `pg_monitor` to your user:
```sql
GRANT pg_monitor TO your_readonly_user;
```

**EXPLAIN ANALYZE times out** - The query actually runs (rolled back). Use `analyze=false` (default) for cost estimates only.

**Schema is stale** - Ask Claude to "refresh the schema", or re-run `init` / `import`.

**MCP connection issues** - Server logs to stderr, MCP protocol to stdout. For SSE mode, test with `curl http://host:port/sse`.

**"invalid schema JSON"** - The file must be a valid SchemaSnapshot. If you renamed fields or edited by hand, re-dump from the database.

**Multi-node stats not showing** - Run `dryrun snapshot list` and confirm you see both `schema` rows (from `snapshot take` on the primary) and `activity_stats` rows (from `snapshot activity --label ...` on each replica) sharing the same `schema_ref_hash`. Activity captured before any schema exists needs `--allow-orphan` and won't reattach automatically.


