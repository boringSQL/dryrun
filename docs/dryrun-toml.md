# dryrun.toml

Project configuration. dryrun finds this file by walking up from the current directory, checking each directory for `dryrun.toml`. The search stops at the repository root (`.git` boundary). If no config is found by then, there is none.

## Minimal example

```toml
[project]
id = "myapp"

[default]
profile = "offline"

[profiles.offline]
database_id = "myapp"
```

That's it. Everything else has sensible defaults. The schema itself is read from
`.dryrun/history.db`; a profile only says which key to read it under, and how to
connect when a command needs a live database.

## Project

```toml
[project]
id = "myapp"
```

Identifies the project. Snapshots are keyed by `(project_id, database_id)` so a single store can hold history for multiple projects without collisions. Defaults to the cwd basename if absent.

## Profiles

A profile names the snapshot key dryrun reads under, and optionally how to connect to a live database. Most projects have two or three: one for offline work, one for local dev, maybe one for staging.

`schema_file` is removed, along with the `--schema-file` and `mcp-serve --schema` flags. A config that still carries the key loads fine — unknown keys are ignored — but it selects nothing: snapshots live in `.dryrun/history.db`, and offline commands read them from there.

```toml
[profiles.offline]
database_id = "myapp"

[profiles.local]
db_url = "postgresql://dev:dev@localhost:5432/myapp"

[profiles.staging]
db_url = "${STAGING_DATABASE_URL}"    # environment variables work

[profiles.prod-auth]
db_url = "${PROD_AUTH_DATABASE_URL}"
database_id = "auth"                  # set when a project has multiple databases
```

`database_id` is the snapshot stream id. `dryrun init --db` bakes the real `current_database()` here when it scaffolds the profile; a profile that leaves it unset falls back to the project id (the profile name never becomes the stream id). Set it explicitly when one project tracks several databases (e.g. `auth`, `billing`).

Pick one with `--profile`, or set a default:

```toml
[default]
profile = "offline"
```

### Resolution order

A profile is selected from:

1. `--profile` flag
2. `PROFILE` environment variable
3. `[default].profile` in dryrun.toml

The `--db` flag overrides the resolved profile's `db_url` for that invocation; it doesn't bypass the profile, so `database_id` and `project_id` are still taken from it. `--profile billing --db $OTHER` connects to `$OTHER` but keys snapshots under billing's `database_id`.

Every DB command (`init`, `probe`, `dump-schema`, `drift`, all `snapshot` subcommands) accepts `--profile` and falls back to the resolved profile's `db_url` when `--db` is omitted. `lint` is offline: it reads `.dryrun/history.db` under the profile's key and only connects when `--db` is passed explicitly.

### Environment variable expansion

`db_url` supports `${VAR}` syntax. Missing variables expand to an empty string.

```toml
[profiles.dev]
db_url = "postgres://${DB_USER}:${DB_PASS}@${DB_HOST}:5432/myapp"
```

## Remotes

A remote is an OCI registry that holds snapshots for sharing. The point is to capture once and distribute: someone with database access takes a snapshot and pushes it; everyone else, and CI, pulls it and works offline. No one else needs credentials to the database.

The lifecycle is three commands:

```sh
dryrun snapshot take --push      # capture from the database and publish
dryrun snapshot pull             # fetch the latest snapshots into local history
dryrun snapshot push             # publish snapshots already in local history
```

`push` and `pull` read the remote from the active profile, or take `--remote <name>` to pick one, or `--oci <ref>` to target a registry directly without configuring anything. The rest of this section is how you configure a remote so you don't repeat `--oci` every time.

Any registry works: GitHub Container Registry, Google Artifact Registry, Amazon ECR, Docker Hub, Harbor, or a self-hosted `registry:2`/zot. A remote is one `[[remote]]` block:

```toml
[[remote]]
name = "ghcr"
type = "oci"
ref = "ghcr.io/myorg/dryrun"
default = true
```

| Key | Meaning |
|-----|---------|
| `name` | How you refer to it: `--remote ghcr`. |
| `type` | `oci`. The only type today; defaults to `oci` when omitted. |
| `ref` | Registry base path. The repository is `<ref>/<project_id>/<database_id>`. |
| `token_env` | Name of an environment variable holding a bearer token. Optional. |
| `default` | Use this remote when `--remote` is omitted. |

`ref` names the registry and a base repository, not the final location. A snapshot belongs to a database, so dryrun appends `<project_id>/<database_id>`. With the remote above, `myapp`'s `auth` database lands at `ghcr.io/myorg/dryrun/myapp/auth`. One `ref` covers every project and database, each in its own repository. That trailing path is the `stream`, which a profile can [override](#per-profile-remote-and-stream).

Declare more than one and mark one as the default:

```toml
[[remote]]
name = "ghcr"
ref = "ghcr.io/myorg/dryrun"
default = true

[[remote]]
name = "gar"
ref = "us-docker.pkg.dev/myproj/dryrun"
```

With a single remote, `--remote` is optional and resolves to it. With several, push and pull use the one marked `default`; pass `--remote` to pick another.

Add and remove remotes from the command line instead of editing the file by hand:

```sh
dryrun remote add ghcr --ref ghcr.io/myorg/dryrun --default
dryrun remote list
dryrun remote rm ghcr
```

### Authentication

By default dryrun reads `~/.docker/config.json` and any configured credential helpers, so whatever authenticates `docker push` authenticates dryrun:

```sh
docker login ghcr.io
gcloud auth configure-docker us-docker.pkg.dev
```

For a static bearer token, set `token_env` to the name of the variable that holds it:

```toml
[[remote]]
name = "ci"
ref = "ghcr.io/myorg/dryrun"
token_env = "GHCR_TOKEN"
```

dryrun reads the token from `$GHCR_TOKEN` at push and pull time, so it never appears in the config file. A `token_env` that names an unset variable is an error, not a silent fall back to anonymous access.

### Per-profile remote and stream

A profile can pin a remote and override where its snapshots are stored:

```toml
[profiles.prod-auth]
db_url = "${PROD_AUTH_DATABASE_URL}"
database_id = "auth"
remote = "ghcr"
stream = "shared/auth"
```

`remote` is the remote used when you run `snapshot push` or `pull` under this profile without `--remote`.

`stream` is the repository path suffix under the remote's `ref`. It defaults to `<project_id>/<database_id>`, the same layout the filesystem store uses, so anything already pushed keeps resolving. It changes the storage location only: the local snapshot key stays `(project_id, database_id)`, so `lint`, `drift`, `take`, and `list` against local history are unaffected.

### Sharing a database across projects

A snapshot describes a database, not a project. When two projects point at the same physical database, give both the same `stream` so they read and write one shared history instead of two near-duplicates:

```toml
# project A                          # project B
[profiles.auth]                      [profiles.auth]
db_url = "postgres://.../auth"       db_url = "postgres://.../auth"
database_id = "auth"                 database_id = "auth"
remote = "gar"                       remote = "gar"
stream = "shared/auth"               stream = "shared/auth"
```

Both profiles resolve to one repository, so the schema is stored once. Two rules come with a shared stream:

- One owner takes and pushes; everyone else pulls. If both projects take snapshots on a schedule, the schema blob deduplicates but the differing timestamps and activity rows do not, leaving two near-duplicate observations per interval. Designate one CI job as the owner.
- Retention and access live on the shared repository. Cleanup policies and IAM are per repository: grant the owner write and consumers read on `shared/auth`.

## Nodes

Fleet members for `snapshot capture`:

```toml
[[node]]
name     = "primary"
role     = "primary"
url      = "service=dryrun-primary"
streams  = ["planner", "activity", "query"]
interval = "1h"

[[node]]
name     = "read-pool"
url      = "service=dryrun-read-pool"
streams  = ["query"]
interval = "15m"
pool     = true
```

- `name` — the label the node's rows are stored under.
- `role` is checked at capture time. The default `auto` accepts whatever it finds.
- `url = "service=name"` is the preferred way to connect: it names an entry in a libpq service file (`~/.pg_service.conf`, or the file `PGSERVICEFILE` points to), with the password in `~/.pgpass`. Both live on the capture host only, so `dryrun.toml` carries no credentials and no per-environment variable names. Use the keyword DSN form — the `postgres://` URL form cannot express a service. Anything else in the string overrides the service entry (`url = "service=x dbname=other"`).
- `url_env` names an environment variable, and `url` may hold a `${VAR}` reference — also committable, for environments where a service file is impractical. Set exactly one of `url` / `url_env` per node.
- Without `streams`, the detected role decides. A standby has no schema of its own and its planner stats mirror the primary's, so it captures `activity` and `query`.
- `interval` — the cadence enforced by `snapshot capture --all --due`.
- `pool = true` marks a label that names a read pool rather than one machine, which suppresses the identity-drift warning. Don't set it on a single machine; that warning is how you find out two servers answer on one label.

See [multi-node-stats.md](multi-node-stats.md) for the capture semantics.

## Query stats

```toml
[query_stats]
row_cap = 1000
```

`row_cap` limits how many `pg_stat_statements` rows one capture stores (default 500), and `--query-stats-limit` overrides it per run. [query-stats.md](query-stats.md) explains what the cap means at diff time.

## Conventions

These control what `dryrun lint` checks. Skip the whole section to use the defaults.

```toml
[conventions]
table_name = "snake_plural"
column_name = "snake_case"
pk_type = "bigint_identity"
fk_pattern = "fk_{table}_{column}"
index_pattern = "idx_{table}_{columns}"
require_timestamps = true
timestamp_type = "timestamptz"
prefer_text_over_varchar = true
min_severity = "warning"
```

### table_name

How tables should be named.

| Value | Example |
|-------|---------|
| `auto` (default) | Detects singular vs plural from your existing tables |
| `snake_singular` | `user`, `lab_session` |
| `snake_plural` | `users`, `lab_sessions` |
| `camelCase` | `labSession` |
| `PascalCase` | `LabSession` |
| `custom_regex` | Your own pattern (see [custom patterns](#custom-patterns)) |

Auto-detection samples your existing tables. If 5+ snake_case tables exist, it picks whichever form (singular or plural) dominates. Below that threshold, falls back to `snake_singular`.

### column_name

Same idea, for columns. Default: `snake_case`. Also supports `camelCase` and `custom_regex`.

### pk_type

What primary keys should look like.

| Value | Accepts | Identity required? |
|-------|---------|-------------------|
| `bigint_identity` (default) | `bigint` | Yes |
| `int_identity` | `integer` and `bigint` | Yes |

`int_identity` is for projects where 2 billion rows is plenty. `bigint` is always accepted since it's a superset, not a violation. This also suppresses the `types/bigint_pk_fk` overflow warning for integer columns.

Set to empty string (`pk_type = ""`) to disable the check entirely.

### Patterns

`fk_pattern` and `index_pattern` use `{table}` and `{column}`/`{columns}` placeholders:

```toml
fk_pattern = "fk_{table}_{column}"       # fk_orders_user_id
index_pattern = "idx_{table}_{columns}"   # idx_users_email
```

### Timestamps

```toml
require_timestamps = true      # every table needs created_at and updated_at
timestamp_type = "timestamptz" # warns about bare timestamp without time zone
```

### min_severity

Filter lint output by severity. Default: `info` (show everything).

| Value | Shows |
|-------|-------|
| `info` | Everything |
| `warning` | Warnings and errors only |
| `error` | Errors only |

### Custom patterns

For naming conventions that don't fit the built-in styles:

```toml
[conventions]
table_name = "custom_regex"

[conventions.custom]
table_name_regex = "^[a-z][a-z0-9_]*$"
column_name_regex = "^[a-z][a-z0-9_]*$"
```

Only used when `table_name` or `column_name` is set to `"custom_regex"`.

## Disabling rules

Turn off rules that don't apply to your project:

```toml
[conventions.disabled_rules]
rules = ["naming/fk_pattern", "constraints/unnamed"]
```

All rules and their default severities:

| Rule | Default | What it checks |
|------|---------|----------------|
| `naming/table_style` | warning | Table name matches convention |
| `naming/column_style` | warning | Column name matches convention |
| `naming/fk_pattern` | info | FK constraint naming |
| `naming/index_pattern` | info | Index naming |
| `pk/exists` | error | Every table has a primary key |
| `pk/bigint_identity` | warning | PK uses the configured type with IDENTITY |
| `types/text_over_varchar` | warning | TEXT preferred over VARCHAR |
| `types/timestamptz` | warning | Bare `timestamp` without time zone |
| `types/no_serial` | warning | Prefers IDENTITY over serial/sequence |
| `types/bigint_pk_fk` | warning | PK/FK columns aren't too small |
| `constraints/fk_has_index` | error | FK columns have covering indexes |
| `constraints/unnamed` | info | Auto-generated constraint names |
| `timestamps/has_created_at` | warning | Table has `created_at` |
| `timestamps/has_updated_at` | warning | Table has `updated_at` |
| `timestamps/correct_type` | warning | Timestamp columns use the right type |
| `partition/too_many_children` | warning | Under 500 partitions |
| `partition/range_gaps` | warning | No gaps in range partition bounds |
| `partition/no_default` | info | Range partitions have a DEFAULT partition |
| `partition/gucs` | warning | PostgreSQL GUCs tuned for partitioning |

### Rule suppression

Some rules overlap. When a more specific rule fires on the same table/column, the generic one is automatically suppressed:

| If this fires... | ...this is suppressed |
|---|---|
| `timestamps/correct_type` | `types/timestamptz` |
| `pk/bigint_identity` | `types/no_serial` |
| `pk/bigint_identity` | `types/bigint_pk_fk` |

You don't need to disable the suppressed rules manually.

## Full example

```toml
[default]
profile = "offline"

[profiles.offline]
database_id = "myapp"

[profiles.dev]
db_url = "${DEV_DATABASE_URL}"
remote = "ghcr"

[profiles.staging]
db_url = "${STAGING_DATABASE_URL}"
database_id = "staging"

[[remote]]
name = "ghcr"
ref = "ghcr.io/myorg/dryrun"
default = true

[conventions]
table_name = "snake_singular"
column_name = "snake_case"
pk_type = "bigint_identity"
fk_pattern = "fk_{table}_{column}"
index_pattern = "idx_{table}_{columns}"
require_timestamps = true
timestamp_type = "timestamptz"
prefer_text_over_varchar = true
min_severity = "warning"

[conventions.disabled_rules]
rules = ["naming/fk_pattern", "naming/index_pattern", "constraints/unnamed"]

[conventions.custom]
table_name_regex = "^[a-z][a-z0-9_]*$"
column_name_regex = "^[a-z][a-z0-9_]*$"

```
