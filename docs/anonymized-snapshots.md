# Anonymized snapshots

A snapshot is schema plus planner statistics stored in `.dryrun/history.db`. Planner statistics include `most_common_vals` and `histogram_bounds`, which hold literal values from your tables, so an unmasked capture of a real database leaks that data. [MASKING-TUTORIAL.md](../MASKING-TUTORIAL.md) covers the concepts; this page is the end-to-end walkthrough.

Masking runs once, at capture. `push` and `pull` copy bytes and never re-mask, so a policy that was wrong at capture time can only be fixed by recapturing.

Point `DATABASE_URL` at a replica or restored dump if you have one. Capture is read-only and refuses to run as a superuser, replication or bypassrls role. [`dryrun-readonly-role.sql`](../dryrun-readonly-role.sql) creates a role with only `CONNECT`, `pg_read_all_data` and `pg_read_all_stats`.

## Write the masking policy

The policy is a YAML file naming sensitive columns. [fixturize](https://github.com/boringSQL/fixturize) generates a first draft by scanning column names and types:

```sh
fixturize analyze --connection "$DATABASE_URL" --yaml --output data-masking-policy.yml
```

```yaml
version: 1

databases:
  default: # fixturize always writes "default"
    columns:
      users.email: { expr: "'user_' || \"user_id\" || '@test.com'", tags: [pii] }
      contacts.phone: { expr: "'+1555' || LPAD(\"contact_id\"::text, 7, '0')", tags: [pii] }
    policies:
      pii: { include_tags: [pii] }
```

Rename `default` to the database's `database_id` — `appdb` in the config below, not the profile name `prod`. dryrun looks the block up by that key and errors out when it is missing.

Keys are `table.column`, or `schema.table.column` when qualified. `expr` is used by `fixturize extract` to rewrite rows; dryrun reads only the column set, but keep `expr` in place because both tools share the file. On large schemas, `--min-confidence medium` drops low-confidence guesses.

## Configure dryrun

```toml
require_masks = true

[project]
id = "appdb"

[default]
profile = "prod"

[profiles.prod]
db_url      = "${DATABASE_URL}"
database_id = "appdb"
masks_file  = "data-masking-policy.yml"
```

`database_id` defaults to `[project].id` when omitted, never to the profile name. Multi-database projects need one block in the YAML per captured database, even an empty one.

`require_masks = true` makes a missing policy a hard error and disables `--no-masks`. Without it, a missing policy only warns and the capture runs unmasked.

Do not scaffold with `dryrun init` here: with `DATABASE_URL` set, a bare `dryrun init` captures immediately, without masks.

## Capture

```sh
dryrun init --source appdb-primary
```

```
Captured schema: 41 tables, 3 views, 8 functions
  Planner:  41 tables, 96 indexes, 312 columns
  Masked:   14 planner-stats columns
  Activity: node=appdb-primary, 41 tables, 96 indexes
```

`--source` sets a stable node label; without it the label is derived from the server address. Unmatched keys under `columns:` are ignored silently, so if the masked count is lower than the number of keys, check spelling and schema qualification.

## Check for leftover values

Name- and type-based matching misses columns like free-text notes or jsonb payloads. Copy [`examples/residual-values.sh`](../examples/residual-values.sh) next to the policy and list what actually survived masking:

```sh
./residual-values.sh
```

```
public.audit_log.ip_address  {10.0.1.1,10.0.2.1,10.0.3.1,...
public.audit_log.payload     {"{\"ip\": \"10.0.0.1\", \"email\": \"jana@acme.com\"}",...
public.contacts.notes        {"Prefers email. Mentioned budget approval in Q3."}
public.deals.stage           {won,lead,lost,proposal,qualified}
public.invoices.vat_number   {CZ10000001,CZ10000066,...
public.organizations.name    {"Acme s.r.o.","Globex",...
public.users.role            {member,admin,owner}
```

The script skips value sets that are purely numeric or purely dates. Everything printed is readable by whoever receives the file. Keep enum-like domain values (`stage`, `status`, `role`, `plan`) because they make offline `EXPLAIN` plans realistic. Mask free text, jsonb, addresses, identifiers and customer names.

For jsonb, MCVs are always stripped at capture, but `histogram_bounds` is not and holds whole documents, so jsonb columns need a policy entry.

Add any misses to `columns:` and rerun until only acceptable values remain.

## Recapture and verify

The policy is fixed, so capture again, from scratch:

```sh
rm -rf .dryrun
dryrun init --source appdb-primary
```

The delete is required: `history.db` is append-only, so the under-masked rows from the first capture are still in the file, and rows deleted from SQLite can survive in free pages. The file you send must come from a single capture with the final policy.

The new capture prints `Masked: 22 planner-stats columns`. That is dryrun's own report; verify it against the file directly:

```sh
# 1. What dryrun claims it did
sqlite3 -noheader .dryrun/history.db \
  "SELECT payload_json FROM planner_stats ORDER BY id DESC LIMIT 1" | jq -c .masking
# {"applied":true,"columns_masked":22,"jsonb_mcv_stripped":true}

# 2. What actually survived
./residual-values.sh

# 3. What the bytes say. Strings you know are in your data; expect silence
strings .dryrun/history.db | grep -oiE 'acme\.com|\+420 [0-9]+|Vinohradska'
```

Check 1 only reports what dryrun recorded. Check 3 reads the raw bytes with no metadata involved, so it is the one that catches mistakes.

## Share

Push the snapshot through the store instead of mailing the database file around:

```sh
dryrun snapshot push --to-path ./handover       # a directory you can tar up or sync
dryrun snapshot push --oci ghcr.io/org/dryrun   # or straight to an OCI registry
```

The receiver runs `dryrun snapshot pull --from-path ./handover` (or `--oci`) and the snapshot lands in their own `.dryrun/history.db`. This is why the checks above run before pushing, not after.

If the snapshot is going to a shared workspace rather than one recipient, push it to [Hindsight](https://boringsql.com/products/hindsight/), the boringSQL registry. It never connects to your database; the CLI captures locally and pushes. A workspace toggle refuses any push carrying unmasked planner statistics, so a policy mistake gets rejected instead of stored:

```sh
export DRYRUN_TOKEN=<workspace token>
dryrun remote add hindsight --type http --ref <workspace-url>
dryrun snapshot push --all --remote hindsight
```

Send `data-masking-policy.yml` alongside: it documents what was masked and is what you rerun on the next capture.

A snapshot contains:

- **Schema** — table, column, index and constraint names, types, `DEFAULT` and `CHECK` expressions, enum labels, view definitions as full SQL, the extension list, non-default GUCs, PostgreSQL version and `system_identifier`
- **Planner statistics** — per column `null_frac`, `n_distinct`, `correlation`, `avg_width`, and whatever survived masking
- **Activity counters** — per table and per index, numbers only

It does not contain table rows, function bodies (signatures only), or credentials — the connection URL is stored as a hash. Query text appears only if you explicitly run `dryrun snapshot query-stats`, as normalized `pg_stat_statements` shapes.

Before pushing, check that `CHECK` and `DEFAULT` expressions and view definitions do not embed sensitive literals, and that table and enum names do not disclose anything confidential.
