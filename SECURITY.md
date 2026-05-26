# DryRun Security Overview

## What DryRun is

**Don't let AI touch your production database.** That is the whole
reason DryRun exists.

DryRun captures a PostgreSQL database's schema, planner statistics, and
activity counters into a local file (`.dryrun/history.db`). AI
assistants, developers, and tooling read that file instead of opening a
connection to production.

The single binary has two modes, and the security model follows that
split:

| Mode | Who runs it | Touches the database? | Needs credentials? |
|------|-------------|-----------------------|--------------------|
| **CLI** (`init`, `probe`, `dump-schema`, `drift`, `snapshot take/diff`, `stats apply`) | DBA / CI | Yes | Yes |
| **MCP + offline tools** (`mcp-serve`, `lint`, `import`, `snapshot list/push/pull`) | Agent / developer | No | No |

The captured file is the boundary. The CLI writes it against
production. Everything else only reads it. Production credentials stay
with the DBA.

## MCP and offline tools

This is the side an agent or teammate actually interacts with.

- No database connection, no credentials. Only reads
  `.dryrun/history.db` and snapshot files.
- SQL submitted by an agent through MCP is parsed locally with
  libpg_query and validated against the captured schema. It is never
  executed against a real database.
- An agent sees exactly what the snapshot contains, nothing more.

Rule of thumb: **if you would not hand a value to your AI assistant, it
must not be in the snapshot.**

## CLI, the side that touches production

The CLI is the only place real credentials live. Run it as a DBA, not
as an agent.

- **Prod-reading**: `init`, `probe`, `dump-schema`, `drift`,
  `snapshot take`, `snapshot diff`. Connect read-only. Use the included
  `dryrun-readonly-role.sql`. Never SUPERUSER, never the app role. Pass
  `DATABASE_URL` via environment, not flags.
- **Local-writing**: `stats apply` mutates planner stats on local or
  dev database so `EXPLAIN` matches production shape. Never point it at
  production.

Of the prod-reading commands, `init` and `snapshot take` write the
shared `history.db`. Both apply the masking policy in-process *before*
anything is written to disk. DryRun does not persist the connection
string. The snapshot records a logical `database_id`, not the URL.

## Data masking

Masking runs **once**, at capture time, inside `init` or `snapshot
take`. The masked form is what lands in `history.db`. There is no
re-masking later, and `snapshot push`/`pull` move bytes without
transforming them.

Two consequences worth knowing:

- A missing or wrong policy at capture is a **permanent leak**.
  Recapture is the only fix. Old snapshots in shared storage stay leaky
  until you delete them.
- For projects with real PII, set `require_masks = true` in
  `dryrun.toml`. That turns "no policy resolved" into a hard error and
  refuses `--no-masks`.

Independent backstop, always on: `jsonb` `most_common_vals` and
`most_common_freqs` are stripped at capture regardless of policy.
`histogram_bounds` is *not* auto-stripped. List sensitive jsonb columns
in the policy explicitly.

Full workflow and examples: see [`MASKING-TUTORIAL.md`](MASKING-TUTORIAL.md).

## Telemetry

DryRun sends telemetry from the **MCP server only**. The CLI, capture,
and snapshot commands send nothing.

The MCP server emits two events per session: a `dryrun_start` when an
agent connects, and a `dryrun_summary` when the session ends. No
snapshot contents, no DDLs, no SQL text, no database identifiers, no column or
table names.

Example `dryrun_start` payload:

```json
{
  "event": "dryrun_start",
  "session_id": "00000000-0000-0000-0000-000000000001",
  "app_version": "0.8.0",
  "timestamp": "2026-05-25T21:00:00Z",
  "transport": "stdio",
  "schema_loaded": true,
  "table_count": 12,
  "planner_loaded": true,
  "activity_nodes": 4
}
```

`dryrun_summary` adds session duration and a `{tool_name: count}` map
of which MCP tools the agent invoked. Tool names are the built-in
DryRun tool identifiers, not anything the agent typed.

**Opt out** by setting either of these in the MCP server's
environment:

```sh
export DO_NOT_TRACK=1            # industry-standard, also honored
export DRYRUN_TELEMETRY=off      # dryrun-specific
```

Or persist it in `dryrun.toml`:

```toml
[telemetry]
enabled = false
```

Any of these disables the endpoint entirely. No event leaves the
process. The env var wins over the config file, so CI runs can stay
silent without editing checked-in TOML.

## Related documents

- [`MASKING-TUTORIAL.md`](MASKING-TUTORIAL.md): masking policy
  workflow.
- `dryrun-readonly-role.sql`: minimum-privilege role for capture.
