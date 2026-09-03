# Data masking with DryRun

## Why masking exists

DryRun captures PostgreSQL planner statistics so `EXPLAIN` plans are
realistic without a connection to production. Those statistics include
`most_common_vals` and `histogram_bounds`, which are literal column
values lifted from your tables. For a `users.email` or `users.phone`
column, that is PII landing in `.dryrun/history.db`.

The masking policy is a small YAML file that names sensitive columns.
DryRun reads it at capture time and writes NULL into the offending
stats before anything touches disk.

Masking runs once, inside `init` or `snapshot take`. The masked form is
what lands in `history.db`. `push` and `pull` move bytes and do not
re-mask, so a missing or wrong policy at capture is a permanent leak
and recapture is the only fix.

By default a missing policy is a warning, so fresh checkouts work.
Projects with real PII should set `require_masks = true` in
`dryrun.toml` to turn that warning into a hard error.

The file is shared with [fixturize](https://boringsql.com/fixturize).
fixturize uses the `expr` field to rewrite rows on extract. DryRun
ignores `expr` and uses the column set to NULL planner stats.

The rest of this document shows the small case first, two columns
hand-written, and then the automated path with `fixturize analyze`.

---

## The minimum viable policy

Suppose `users.email` and `users.phone` are the only sensitive columns
you care about. Drop this file at the repo root as
`data-masking-policy.yml`:

```yaml
version: 1

databases:
  dev:
    columns:
      users.email: { expr: "'user_' || id || '@masked.test'", tags: [pii] }
      users.phone: { expr: "'+1-000-000-0000'",               tags: [pii] }
```

The `dev` key must match your DryRun `database_id` (see below), which
defaults to `[project].id` unless the profile overrides it. The
`version` field must be `1`. `expr` is only read by fixturize, but it
is part of the shared schema, so leave it in even if you never run
fixturize.

Column keys can be `table.column` to match in any schema, or
`schema.table.column` to qualify.

### Wiring it to a profile

DryRun resolves the policy file in three ways, highest priority first:

1. `--masks-file <path>` on `dryrun init`.
2. `masks_file` in the active `dryrun.toml` profile.
3. Auto-discovery, walking up from the working directory looking for
   `data-masking-policy.yml` and stopping at `.git`.

If the file sits at the repo root, auto-discovery is enough. To be
explicit:

```toml
require_masks = true

[project]
id = "appdb"

[profiles.dev]
db_url     = "${DATABASE_URL}"
masks_file = "data-masking-policy.yml"
```

The profile's `database_id` defaults to `[project].id` (not the profile
name) and is what DryRun uses to select a block inside the YAML. Set it
explicitly when the two differ. The block name in the policy file must
match it, so `databases.dev` rather than `databases.default`.

### Capture and verify

```sh
dryrun --db "$DATABASE_URL" --profile dev init
```

The summary line names how many columns got blanked:

```
Captured schema: 24 tables, 3 views, 8 functions
  Schema:   4f9c2a1b8e7d305c6a2f1e9b4d8c7a3f2e1d0c9b8a7f6e5d4c3b2a1908f7e6d5
  Planner:  24 tables, 41 indexes, 312 columns
  Masked:   2 planner-stats columns
  Activity: node=db-primary, 24 tables, 41 indexes
```

Confirm it on the history store directly:

```sh
sqlite3 -noheader .dryrun/history.db \
  "SELECT payload_json FROM planner_stats ORDER BY id DESC LIMIT 1" \
| jq -c '.masking,
    (.columns[] | select(.column | IN("email","phone"))
      | {c: "\(.table.name).\(.column)", mcv: .stats.most_common_vals})'
```

```
{"applied":true,"columns_masked":2,"jsonb_mcv_stripped":true}
{"c":"users.email","mcv":null}
{"c":"users.phone","mcv":null}
```

Masked columns show no `most_common_vals`. Non-sensitive columns keep
their statistics, so plans on those columns stay realistic.

For a full pre-handover review, see
[`docs/anonymized-snapshots.md`](docs/anonymized-snapshots.md).

Regardless of policy, DryRun strips `jsonb` MCV values at capture as an
always-on backstop. `histogram_bounds` is not auto-stripped, so list
sensitive jsonb columns in the policy explicitly.

For a one-off opt-out, `dryrun init --no-masks` writes raw planner
stats to `history.db`. It is refused when `require_masks = true`. To
select a subset of columns by tag, group them under `policies` in the
YAML and pass `--mask-policy <name>`. Without it, every listed column
is masked.

---

## Scaling up with `fixturize analyze`

Two columns are easy to type. A real schema has dozens, and the names
drift over time. `fixturize analyze` scans the live schema, flags
columns that look like PII by name and type, and emits a policy in the
same format.

```sh
fixturize analyze \
  --connection "$DATABASE_URL" \
  --yaml \
  --output data-masking-policy.yml
```

Useful flags:

| Flag | Purpose |
|------|---------|
| `--min-confidence low\|medium\|high` | Drop low-confidence guesses. Default `low`. |
| `--root users --depth 2` | Limit the scan to tables reachable from a root table. |
| `--schema public` | Default schema for unqualified names. |

Run it once without `--yaml` to eyeball the report and confidence
levels:

```sh
fixturize analyze --connection "$DATABASE_URL" --min-confidence medium
```

The generated file looks identical to the hand-written one above, with
one caveat. `fixturize analyze` always writes the block under
`databases.default`. DryRun looks up the block by your profile's
`database_id` and fails with an error if it does not find a match.
Rename `default` to your actual `database_id`. Multi-DB projects need
one entry per captured database, even if empty.

Automated heuristics miss domain-specific PII like a `notes` text
column or a `metadata` jsonb blob, and over-flag harmless lookups, so
the output is a starting point rather than ground truth.

Because both tools key off `schema.table.column`, the same file
protects test-data extraction in fixturize (via `expr`) and local
planner-stats capture in DryRun (via the column set).

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `masks file has no entry for database_id "X"` | Rename the YAML block to match the profile's `database_id`. Multi-DB projects need one entry per captured database. |
| `no data-masking-policy.yml resolved` warning | Expected on fresh checkouts. To enforce, set `require_masks = true` or pass `--masks-file`. |
| `require_masks=true ... must exist` | Provide a file via any of the three resolution paths, or unset `require_masks`. |

---

## Related documents

- [`SECURITY.md`](SECURITY.md) covers DryRun's security model and where
  masking sits in it.
- `dryrun-readonly-role.sql` is the minimum-privilege role for capture.
