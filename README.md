# DryRun PostgreSQL MCP

The PostgreSQL MCP server that doesn't need connection to the production.

`dryrun` gives AI agents, IDEs, and CI full schema awareness. From offline snapshot, not live database connection. Lint your schema, validate queries, check migration safety, and explore foreign key graphs. All without credentials leaving the DBA's machine.

`dryrun` is part of the [boringSQL](https://boringsql.com) suite alongside [RegreSQL](https://github.com/boringsql/regresql) and [Fixturize](https://github.com/boringSQL/fixturize).

## The problem

LLM/AI coding assistants are very good in writing code/SQL queries. But they are blind. THey don't know your schema, your indexes or your constraints. They might generate a migration that takes an `ACCESS EXCLUSIVE` lock on your busiest table and send your app down.

Some PostgreSQL MCP server ask you for the database connection. And to perform the administrative tasks you might need SUPERUSER permission. But that's like asking for problem.


We've already seen where this leads: [production databases wiped by AI agents](https://fortune.com/2025/07/23/ai-coding-tool-replit-wiped-database-called-it-a-catastrophic-failure/), and [SQL injection in MCP servers](https://securitylabs.datadoghq.com/articles/mcp-vulnerability-case-study-SQL-injection-in-the-postgresql-mcp-server/) that were supposed to be read-only.

The model doesn't need to *query* your database. It needs to *understand* your schema: the structure, constraints, statistics, and version-specific behavior. That knowledge is structural. It changes when you deploy a migration, not between queries.

## DryRun features

`dryrun` is two things: a **CLI tool** and an **MCP server**. The CLI extracts and analyzes your schema. The MCP server exposes that analysis to AI assistants. They're separate on purpose.

### CLI - extract and analyze

The CLI connects to your PostgreSQL database, introspects the full catalog (tables, views, indexes, constraints, partitions, functions, enums, RLS policies, triggers, extensions, GUCs), and writes a single JSON snapshot. That snapshot is the source of truth for everything else.

Once you have the snapshot, the CLI works offline:

- **Lint** - 20+ convention rules (naming, types, primary keys, timestamps, partitioning) and 13 structural audit rules (duplicate indexes, FK coverage, circular FKs, vacuum tuning)
- **Migration safety** - lock type analysis, duration estimates, table rewrite detection, safe alternatives for each DDL statement
- **Query validation** - SQL parsing via libpg_query, column reference checks against the actual schema, anti-pattern detection
- **Schema diff** - compare snapshots over time, detect drift between live database and saved state
- **Multi-node stats** - per-replica statistics, seq_scan hotspots, routing imbalances

### MCP server - give your AI assistant a schema brain

The MCP server reads the same snapshot. It exposes 16 tools over stdio or SSE: schema exploration, query validation, migration checks, linting, vacuum health. Your AI assistant understands your database while it writes SQL.

No database connection needed. The assistant never sees credentials.

## Why offline

**Schema context belongs in a file, not a live connection.** Column types, row estimates, index definitions, FK relationships, and PostgreSQL version can all be exported once and committed to the repo. One person with database access dumps the schema. Everyone else, humans and AI agents alike, gets full schema intelligence without credentials.

**Credentials shouldn't leave the DBA's machine.** If an MCP server needs `DATABASE_URL` to do anything useful, every developer who uses it needs production credentials. That's a security problem that has nothing to do with AI.

**The server should do analysis, not pass-through.** Returning raw `\d+` output is marginally better than pasting it into the chat yourself. The value is in *interpreting* that data: checking whether a migration is safe for your PostgreSQL version, flagging missing FK indexes, and validating column references against the actual schema.

## 30-second demo

Point **`dryrun`** at any schema JSON file (see [examples/demo](examples/demo/) for a ready-made one):

```sh
cd examples/demo
dryrun lint
```

```
[ERROR] public.audit_log: table has no primary key
       fix: add a primary key (bigint GENERATED ALWAYS AS IDENTITY recommended)
[WARN ] public.audit_log: gap in range partitions: ends at '2024-07-01' but next starts at '2024-10-01'
       fix: inserts into the gap will fail unless a DEFAULT partition exists
[ERROR] public.task_comments: table has no primary key
       fix: add a primary key (bigint GENERATED ALWAYS AS IDENTITY recommended)
[WARN ] public.projects.created_at: timestamp column uses timestamp without time zone instead of timestamptz
       fix: use timestamptz for timestamp columns
[ERROR] public.tasks.project_id: FK 'tasks_project_id_fkey' on column(s) (project_id) has no covering index
       fix: add an index on FK columns to avoid sequential scans on DELETE/UPDATE
[WARN ] public.users.email: column 'email' uses character varying(255), prefer text
       fix: VARCHAR(n) adds a hidden CHECK constraint with no performance benefit
[WARN ] public.user_notifications: table is missing 'created_at' column
       fix: add: created_at timestamptz NOT NULL DEFAULT now()

22 violation(s): 6 error, 16 warning, 0 info (13 tables checked)
```

No database needed. Works entirely from the JSON file.

## Install

**Homebrew:**

```sh
brew install boringsql/tap/dryrun
```

**From source:**

```sh
git clone https://github.com/boringsql/dryrun.git
cd dryrun
cargo build --release
```

The binary is at `target/release/dryrun`.

## Quickstart

There are two ways to get started, pick whichever fits your setup.

### Option A: You have database access

If you can connect to a PostgreSQL instance (local, dev, or production), one command does everything:

```sh
dryrun init --db "$DATABASE_URL"
```

This creates `dryrun.toml`, the `.dryrun/` data directory, and introspects the database into `.dryrun/schema.json`. You're ready to go.

### Option B: Someone else has database access

The person with credentials exports the schema once:

```sh
dryrun dump-schema --source "$DATABASE_URL" --pretty --name "production" -o schema.json
```

They commit `schema.json` to the repo (or share it however you like). Everyone else initializes and imports:

```sh
dryrun init
dryrun import schema.json
```

`dryrun init` creates `dryrun.toml` and `.dryrun/`. `dryrun import` loads the snapshot. No database needed on their machine.

### Then use it

```sh
dryrun lint
```

All commands work offline from the schema file. Each project has its own `dryrun.toml` and `.dryrun/`, there is no global state. Add `.dryrun/` to your `.gitignore`.

## More

- **[boringSQL](https://boringsql.com)**, the blog and project home
- **[RegreSQL](https://github.com/boringsql/regresql)**, SQL regression testing and **`dryrun`**'s companion tool


## License

[BSD 2-Clause License](LICENSE)
