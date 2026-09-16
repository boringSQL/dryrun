# CLI stability

dryrun groups its command surface into stability tiers. Stable commands keep their flags and output shape across minor releases. Experimental commands may change while the design settles, so pin a version if you script against them.

## Experimental: OCI remotes

OCI registry storage shipped recently. The flags and the on-registry layout may change before they are marked stable.

| Surface | Purpose |
|---------|---------|
| `dryrun remote add\|list\|rm` | Manage `[[remote]]` entries in dryrun.toml. |
| `dryrun snapshot push --oci\|--remote` | Push snapshots to an OCI registry. |
| `dryrun snapshot pull --oci\|--remote` | Pull snapshots from an OCI registry. |
| `dryrun snapshot take --push [--remote]` | Capture and push in one step. |

`--oci <ref>` targets a registry base path directly. `--remote <name>` resolves the base and credentials from a configured `[[remote]]`. The `--all` flag on push and pull syncs every key in the source rather than the resolved profile's.

The artifact format carries its version in the media type (`application/vnd.dryrun.bundle.v1+zstd`). A future format would use a new version rather than reinterpreting v1, so older snapshots stay readable.

See [dryrun-toml.md](dryrun-toml.md) for the `[[remote]]` block and the per-profile `remote` and `stream` keys.

## Experimental: `dryrun check`

`dryrun check` runs the `check_migration` and `validate_query` analyses offline, without an MCP client. Flags and JSON shape may change while the surface settles.

| Surface | Purpose |
|---------|---------|
| `dryrun check migration <file>... [--direction up\|down] [--json] [--pretty]` | Lock-safety verdicts for one or more migration files. |
| `dryrun check query <file>... [--json] [--pretty]` | Validate one or more query files against the snapshot. |

Both read the newest schema + planner snapshot from `.dryrun/history.db` (or `--history-db <path>`); they never connect to a live database. Capture one first with `dryrun init` or `dryrun snapshot pull`.

With `--json`, one file argument emits the report object, and several emit an array of them — even when some of those files failed to check (they land on stderr, exit 2). The fields match the MCP tools (`framework`, `direction`, `checks`, `migration_sql`; `valid`, `errors`, `warnings`), so output can be piped to `jq`.

Exit codes:

- `0` — clean: no dangerous migration statement, every query valid.
- `1` — a dangerous migration statement, or an invalid (`valid: false`) query. `caution` verdicts do not fail the run.
- `2` — tool error: an unreadable file, a missing snapshot, a SQL parse failure, or a bad `--direction`.

