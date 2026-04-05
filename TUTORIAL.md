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

Creates `.dryrun/schema.json` and `.dryrun/history.db`.

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

For setups with one master and N replicas serving different query patterns. Stats (seq_scan, idx_scan, reltuples) differ per node. dryrun can aggregate them.

### 1. Full dump from master

```sh
dryrun dump-schema --source "$MASTER_DB" --name "master" -o master.json
```

### 2. Stats-only dumps from replicas

No structural schema, just pg_stat_user_tables and pg_stat_user_indexes data:

```sh
dryrun dump-schema --source "$REPLICA1_DB" --stats-only --name "replica-1" -o r1-stats.json
dryrun dump-schema --source "$REPLICA2_DB" --stats-only --name "replica-2" -o r2-stats.json
dryrun dump-schema --source "$REPLICA3_DB" --stats-only --name "replica-3" -o r3-stats.json
```

These are lightweight, good for nightly cron. Example cron entry:

```sh
# /etc/cron.d/dryrun-stats
0 2 * * * app dryrun dump-schema --source "$REPLICA1_DB" --stats-only --name "replica-1" -o /data/dryrun/r1-stats.json
```

### 3. Import with merged stats

```sh
dryrun import master.json --stats r1-stats.json r2-stats.json r3-stats.json
```

The resulting `.dryrun/schema.json` contains the full schema from master plus per-node stats from each replica. Consumers (suggest, validate, lint) automatically use aggregated values:

- **reltuples**: max across nodes
- **seq_scan / idx_scan**: sum across nodes (reveals which replicas are doing seq scans)
- **table_size**: max across nodes

### 4. Verify

```sh
cat .dryrun/schema.json | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'{len(d.get(\"node_stats\", []))} node stats attached')
for ns in d.get('node_stats', []):
    print(f'  {ns[\"source\"]}: {len(ns[\"table_stats\"])} tables, {len(ns[\"index_stats\"])} indexes')
"
```

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

**Multi-node stats not showing** - Verify `node_stats` array is present in `.dryrun/schema.json`. Each stats file must be a valid NodeStats JSON (from `--stats-only`).


