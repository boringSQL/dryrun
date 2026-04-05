# dryrun.toml

Project configuration. dryrun finds this file by walking up from the current directory, checking each directory for `dryrun.toml`. The search stops at the repository root (`.git` boundary). If no config is found by then, there is none.

## Minimal example

```toml
[default]
profile = "offline"

[profiles.offline]
schema_file = ".dryrun/schema.json"
```

That's it. Everything else has sensible defaults.

## Profiles

A profile points dryrun at a schema source, either an offline JSON snapshot or a live database connection. Most projects have two or three: one for offline work, one for local dev, maybe one for staging. Each profile has a name and exactly one source.

```toml
[profiles.offline]
schema_file = ".dryrun/schema.json"

[profiles.local]
db_url = "postgresql://dev:dev@localhost:5432/myapp"

[profiles.staging]
db_url = "${STAGING_DATABASE_URL}"    # environment variables work
```

Pick one with `--profile`, or set a default:

```toml
[default]
profile = "offline"
```

### Resolution order

1. `--db` flag (CLI only, bypasses profiles entirely)
2. `--schema-file` flag (CLI only)
3. `--profile` flag
4. `PROFILE` environment variable
5. `[default].profile` in dryrun.toml
6. Auto-discovery of `.dryrun/schema.json`

Relative paths in `schema_file` are resolved from the project root (the directory containing `dryrun.toml`). Absolute paths work too.

### Environment variable expansion

`db_url` supports `${VAR}` syntax. Missing variables expand to an empty string.

```toml
[profiles.dev]
db_url = "postgres://${DB_USER}:${DB_PASS}@${DB_HOST}:5432/myapp"
```

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

Filter lint output by severity. Default: `warning`.

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
schema_file = ".dryrun/schema.json"

[profiles.dev]
db_url = "${DEV_DATABASE_URL}"

[profiles.staging]
schema_file = ".dryrun/staging-schema.json"

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
