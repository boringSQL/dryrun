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

Read-only is enforced, not just recommended. Every session DryRun opens
sets `default_transaction_read_only = on` plus defensive timeouts
(`statement_timeout` 30s, `lock_timeout` 2s,
`idle_in_transaction_session_timeout` 10s — tunable via
`--statement-timeout`, `--lock-timeout`, `--idle-tx-timeout`). The
prod-reading commands additionally refuse superuser, replication, and
bypassrls roles before the first capture query; `--allow-privileged` is
the explicit, loudly-warned escape hatch. Capture itself runs inside a
`REPEATABLE READ, READ ONLY` transaction (the pg_dump pattern). These
are independent layers, and the test suite asserts each one: a write
through the default connection path fails with SQLSTATE `25006`, a
privileged role is refused, and the session GUCs are actually set.

The one sanctioned writer is `stats apply` (and the MCP
`explain_query` stats-injection path against a dev database): it opts
out per-transaction with an explicit `SET TRANSACTION READ WRITE`, so
the session default stays read-only even on that connection.

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

## Sharing snapshots

`snapshot push` / `pull` and direct file copies move the captured bytes without
re-masking. The snapshot may include infrastructure metadata that the column-oriented
masking policy does not cover, notably replica `client_addr` values from
`pg_stat_replication`. Treat shared snapshots as containing that metadata under the
same trust model as the rest of the file.

## Related documents

- [`MASKING-TUTORIAL.md`](MASKING-TUTORIAL.md): masking policy
  workflow.
- `dryrun-readonly-role.sql`: minimum-privilege role for capture.
