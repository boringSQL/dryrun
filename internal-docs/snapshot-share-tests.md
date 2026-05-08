# Snapshot share — end-to-end test suite

Black-box scenarios for validating shared-filesystem snapshot behavior
(`FilesystemStore` + `dryrun snapshot push --to-path` / `pull --from-path`)
against the **v0.6.1** baseline (last release before `FilesystemStore` and the
`Push` / `Pull` CLI verbs landed).

The point isn't to prove the unit tests right (they live in
`crates/dry_run_core/src/history/filesystem_store.rs`). The point is to attack
the load-bearing claims:

> two workstations sharing a directory both see each other's snapshots after
> `push` / `pull`; round-trip preserves content; multi-database is a primitive,
> not a workaround; the OSS tool stays whole.

This document enumerates the scenarios, the expected behavior, and the harness
that runs them in Docker against real Postgres instances.

---

## What "v0.6.1" gives us as baseline

`git diff v0.6.1 HEAD --stat` shows v0.6.1 has:
- `HistoryStore` (SQLite) but **no** `FilesystemStore`.
- `dryrun snapshot export` (writes the `{root}/{project}/{database}/{ts}-{hash}.json.zst`
  layout already), but **no** `push` / `pull`.
- The legacy in-tree `introspect` modules; HEAD uses `pg_introspect` v0.2.0.

So the comparison axis is:

1. **Forward compat (HEAD reads v0.6.1):** `FilesystemStore::get` must read a
   directory produced by `v0.6.1 snapshot export` without modification.
2. **Backward compat (v0.6.1 reads HEAD's push output):** dropping a file
   HEAD pushed into `~/.dryrun/` must keep the OSS escape hatch intact.
3. **Round-trip identity:** a snapshot taken on HEAD, pushed, wiped locally,
   pulled — `content_hash` and the JSON payload must match byte-for-byte (or
   at minimum semantically; bytes after zstd-3 are deterministic for the same
   `zstd` crate version).

---

## Test harness shape (Docker)

```
tests/snapshot-e2e/
  docker-compose.yml
  Dockerfile.dryrun        # multi-stage; builds both v0.6.1 and HEAD
  fixtures/
    schemas/               # seed SQL: 01_simple.sql, 02_partitioned.sql, 03_huge.sql, …
    dryrun.toml.tmpl
  shared/                  # mounted as the "team filesystem" / git repo
  workstations/
    devA/                  # ~/.dryrun for workstation A
    devB/                  # ~/.dryrun for workstation B
  run.sh                   # orchestrates one scenario end-to-end
  scenarios/
    s01_uc1_fresh_clone.sh
    s02_uc2_versioned_history.sh
    s03_uc3_multi_db_all.sh
    …
```

### Containers

- `pg-A`, `pg-B`, `pg-C` — three Postgres 16 containers (different
  schema variants; one for "prod", one for "staging", one for the unrelated
  `database_id` collision case).
- `dryrun-old` — image built from `v0.6.1` checkout, binary at `/usr/local/bin/dryrun`.
- `dryrun-new` — image built from local `HEAD`.
- `runner` — Alpine + bash; mounts `shared/` as the team filesystem; runs the
  scenario scripts with `docker exec` against `dryrun-old` / `dryrun-new`.

The two binaries share `shared/` (the "team git repo" stand-in) and have
**separate** `~/.dryrun/` volumes (devA, devB) so we can simulate two
workstations at the same time without history.db cross-contamination.

### Why Docker and not testcontainers from Rust?

We already have `tests/init_e2e.rs` covering single-process introspection.
This suite is about **two binaries (old vs new) + multiple workstations
sharing a filesystem**, which is awkward to express in-process. Docker
also lets us inject filesystem-quirk volumes (case-insensitive overlay,
read-only mount, full disk) without polluting the host.

### Scenario script contract

Every `sN_*.sh` script:
1. Resets `shared/`, `workstations/devA`, `workstations/devB`, the relevant
   Postgres containers.
2. Seeds Postgres from `fixtures/schemas/<file>.sql`.
3. Runs a sequence of `dryrun-{old,new} snapshot {take,push,pull,list,diff}`
   calls.
4. Asserts: exit code, stdout substrings, file presence in `shared/`,
   filename-vs-content hash equality, `list` output cardinality.
5. Emits TAP-ish output so `run.sh` can aggregate pass/fail.

---

## Scenario matrix

Legend: **OLD** = `dryrun` built from `v0.6.1`; **NEW** = local HEAD.
Each scenario lists the binaries involved, the FS layout under `shared/`,
and the failure modes we are specifically hunting for.

### A. Happy-path use cases (must pass)

| #   | Name                           | Tools     | What it proves                                                                                                                                        |
| --- | ------------------------------ | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| A1  | UC1 fresh clone pull           | NEW only  | devB starts empty, runs `pull --from-path shared/`, then `list` shows what devA pushed. `lint` / `check_migration` work without ever touching Postgres on devB. |
| A2  | UC2 versioned history          | NEW only  | take→push every "4h" simulated 6 times → `list` sees 6 distinct snapshots ordered by ts; `diff --from <hash> --to latest` works.                      |
| A3  | UC3 multi-DB `--all`           | NEW only  | `dryrun.toml` declares 3 profiles with distinct `database_id`. `push --all` writes 3 subdirs; `pull --all` on devB rebuilds local history.            |
| A4  | Round-trip hash identity       | NEW only  | take→push→delete `~/.dryrun/history.db`→pull→`list` shows same `content_hash` and `take` against the unchanged DB returns `Unchanged`.                |

### B. Cross-version compatibility (the load-bearing claims)

| #   | Name                           | Tools         | What it proves                                                                                                                                                          |
| --- | ------------------------------ | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| B1  | NEW reads OLD `export` output  | OLD → NEW     | OLD does `snapshot take`+`snapshot export --out shared/`; NEW does `pull --from-path shared/` and `diff` against a fresh `take`. The file-layout contract.            |
| B2  | OLD reads NEW `push` output    | NEW → OLD     | NEW does `push --to-path shared/`. OLD has no `pull`, so we drop the file into `~/.dryrun/snapshots/` and run `OLD snapshot import` (or whatever escape hatch v0.6.1 has). Validates principle #1. |
| B3  | OLD `export` → NEW push (mixed)| OLD + NEW     | OLD exports into `shared/`; NEW takes a *new* snapshot and pushes into the same dir; `pull` on a third workstation sees both, ordered by filename ts.                  |
| B4  | NEW push → OLD's `~/.dryrun`   | NEW → OLD     | If a user manually copies a `.json.zst` into OLD's data dir, does OLD's `reload_schema` / `list` find it? Probably no — file the gap, document the workaround.         |

### C. Filesystem edge cases

| #   | Name                                  | What we're hunting                                                                                                                                                                            |
| --- | ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| C1  | Case-insensitive volume (macOS-like)  | `database_id` `Orders` vs `orders` declared in two profiles. The case-sensitivity contract says it's enforced in config. Verify config validator rejects; verify FS write doesn't silently merge streams.       |
| C2  | Read-only `shared/`                   | `push --to-path` against a ro mount → expect a clean error, not a panic; `pull --from-path` still works.                                                                                      |
| C3  | Disk full mid-write                   | tmpfs sized to 1 MB; push a 5 MB snapshot → `.tmp` partial, then ENOSPC. After cleanup, `list` must not see the truncated file (atomic-rename invariant in `put`).             |
| C4  | Existing `*.tmp` left from crash      | Pre-place `2026-01-01T00-00-00Z-deadbeef.json.zst.tmp` in `shared/`. Subsequent `push` must not be confused; `list` must ignore tmp files; eventual cleanup policy?                            |
| C5  | Filename hash ≠ recomputed hash       | Manually flip a byte inside a pushed `.json.zst`. Next `pull` / `get` must surface `StoreError::CorruptSnapshot { path, expected, actual }`. The contract — "loud failure beats quiet drift".    |
| C6  | Truncated `.json.zst`                 | `truncate -s -10` on a snapshot. Decompression error must not crash, must be the same corrupt-snapshot failure mode as C5.                                                                    |
| C7  | Wrong-encoding filename               | Drop a file named with `:` instead of `-` (would be Linux-legal, breaks the contract). Must be ignored or rejected explicitly, never accepted into `list`.                                        |
| C8  | Symlink in `shared/`                  | Replace one `.json.zst` with a symlink to another. `get` follows? Resists? Document the answer.                                                                                                |
| C9  | Path traversal via project_id         | Attempt to coerce `project_id="../../etc/passwd"` through config (config layer is responsible for validation). Must reject at config load, not at write time.                                        |

### C'. Unicode / locale

| #    | Name                              | What we're hunting                                                                                                                                                                       |
| ---- | --------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| C'1  | NFC vs NFD `database_id`          | `é` as one codepoint vs two. macOS APFS normalizes one way, Linux ext4 doesn't. Push from one, pull from another — does `database_id` resolve?                                          |
| C'2  | Mixed-case hex in `content_hash`  | Manually rename a file to use uppercase hex. The contract says hex is lowercase. Must reject loudly.                                                                                        |

### D. Concurrency

| #   | Name                                       | What we're hunting                                                                                                                                                                                                  |
| --- | ------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| D1  | Two workstations push the same hash        | devA and devB took identical snapshots (same DB). Both `push --to-path shared/` simultaneously. The contract: idempotent. Final state: one file. No `.tmp` debris.                                                       |
| D2  | Two workstations push different hashes     | devA and devB take from DBs at different states. Concurrent push. Both files end up in `shared/`; `list` sees two; ordering is by filename `{ts}` (their wall clocks).                                              |
| D3  | Push during pull                           | devA pushes while devC pulls. Pull must see *either* the prior set or the new set, never a half-written file (atomic-rename guarantee).                                                                              |
| D4  | Clock-skew filename order                  | devA's clock 1h ahead. devA's "old" snapshot has a *later* `{ts}` than devB's "new" one. `list` returns wrong logical order. The contract documents this — verify it's documented in `--help` / README, not "fixed".  |
| D5  | Two pushes of the same hash *to same key*  | Push the same snapshot twice in series → `PutOutcome::Unchanged`, no second file, no `mtime` bump (or do we bump? document).                                                                                         |

### E. Multi-database / multi-project

| #   | Name                                          | What we're hunting                                                                                                                                                |
| --- | --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| E1  | `--all` on empty history                      | No snapshots locally. `push --all` exits clean, prints "No snapshots in history.db to push." (already in code at main.rs:707).                                    |
| E2  | `--all` with one stream missing on remote     | Three streams locally, only two on remote. `push --all` ships the missing one only; the two existing ones report `Unchanged`.                                     |
| E3  | `--profile X` with X undefined                | Clean error message, no panic.                                                                                                                                     |
| E4  | `--all` and `--profile` both set              | Mutually exclusive per the contract. Verify `clap` enforces.                                                                                                          |
| E5  | Two projects share a `shared/` root           | `projectA/db1/…` and `projectB/db1/…` coexist; `pull` only walks the project resolved from local `dryrun.toml`.                                                    |
| E6  | `database_id` collision across projects       | Same `database_id` slug in two projects. `pull` must scope strictly to `{project_id}/{database_id}`.                                                              |

### F. Schema-shape stress (real DB content)

| #   | DB shape                                    | What we're hunting                                                                                                                                                          |
| --- | ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| F1  | Empty database                              | take → push → pull on devB → `list` shows 1 snapshot, `diff` against fresh take is empty. Trivial but catches "we accidentally need ≥1 table".                              |
| F2  | 500 tables, 200 indexes, 50 FKs             | Push size, list directory perf (the contract "if 10k+ entries becomes slow, revisit"). Time the `list` round-trip.                                                            |
| F3  | Partitioned tables (RANGE + LIST + HASH)    | an earlier `pg_introspect` rewrite changed how partitions serialize. v0.6.1 ↔ HEAD round-trip will likely hash-diverge here — quantify.                                      |
| F4  | RLS policies, generated columns, identity   | Same — high-risk for hash divergence between OLD and NEW. Document divergences as known migration costs, not bugs.                                                         |
| F5  | Comments / extensions                       | `pg_introspect` v0.2.0 changed comment ordering (`d36aa91 fix: transparent handling of original snapshots`). Verify NEW reads OLD's comment-bearing snapshots correctly.    |
| F6  | UTF-8 in identifiers (`"票据"."tübingen"`)  | Filenames are still ASCII (hash + RFC3339 ts), but JSON content is UTF-8. Push → pull → diff must report 0 changes.                                                         |
| F7  | Snapshot >100 MB (huge schema)              | zstd-3 compression behavior at scale; atomic rename across volumes (rename(2) is not atomic across mountpoints — does our tmp file land on the same FS as the target?).    |

### G. Git-as-backend (the actual recommended deployment)

| #   | Name                                    | What we're hunting                                                                                                                                                                                          |
| --- | --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| G1  | Push → commit → push again → diff       | Run the README's recommended GitHub Action loop in a `git`-initialized `shared/`. Verify nothing weird with `.tmp` files (must be in `.gitignore` or the design must prevent them from existing post-push). |
| G2  | `*.json.zst` as text in `git diff`      | The contract: confirm `*.json.zst binary` in `.gitattributes` works; otherwise CI logs choke on binary zstd.                                                                                                    |
| G3  | Two writers race-pushing to same branch | One push wins, the other has to rebase/retry. Document the recommended retry strategy in the README.                                                                                                         |
| G4  | Repo size growth                        | After 30 days × 6 snapshots/day on F3 schema, what's `du -sh shared/`? Informs the README defaults.                                                                                                          |
| G5  | LFS path                                | Same as G4 but with `git lfs track '*.json.zst'`. Verify push/pull still work end-to-end (LFS smudge filter doesn't break us).                                                                              |

### H. CLI ergonomics / regressions

| #   | Name                                          | What we're hunting                                                                                                                                                            |
| --- | --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| H1  | `pull --from-path` with non-existent dir       | Clean error, no panic, exit ≠ 0.                                                                                                                                              |
| H2  | `push --to-path` creates parent dirs           | Plan doesn't say. Decide and assert. (Suggest: yes for `{project}/{database}/`, no for the user-supplied root.)                                                              |
| H3  | `--db` without `dryrun.toml`                   | `resolve_read_key` path; key derivation must not read a missing toml.                                                                                                         |
| H4  | `--history-db` to a custom path                | Already supported (main.rs:152, 162). Verify isolation: two scenarios with two separate `--history-db` files don't bleed state.                                              |
| H5  | Exit codes                                     | `--all` partial failure: should it be exit 1 with "2/3 streams pushed" or all-or-nothing? Decide and lock it in.                                                              |

---

## Hash-divergence vs hash-equivalence — the calibration table

Some of the scenarios above (F3–F5) will hash-differ between v0.6.1 and HEAD
because `pg_introspect` changed the introspection output. These are not
push/pull bugs — they're an unavoidable consequence of moving introspection.

The harness should produce a **divergence report** rather than fail:

```
F3 partitioned tables: OLD hash=abc123…, NEW hash=def456…  ← EXPECTED (intro change)
F1 empty DB:           OLD hash=000a…,   NEW hash=000a…    ← MATCH
F6 UTF-8 idents:       OLD hash=…, NEW hash=… ← INVESTIGATE
```

The plan's claim that "an exported file from SQLite store must be readable by
`FilesystemStore::get` without modification" is about **file format**, not
hash equality. Test that NEW can *parse* OLD's output; only assert hash
equality where the introspection layer didn't change.

---

## Concrete harness skeleton

`tests/snapshot-e2e/` contains the runnable scaffold. The pieces:

- `Dockerfile.dryrun` — two-stage build:
  - stage `old`: `git clone --branch v0.6.1` + `cargo build --release`.
  - stage `new`: `COPY . .` + `cargo build --release`.
  - final image: both binaries at `/usr/local/bin/dryrun-old` and
    `/usr/local/bin/dryrun-new`.
- `docker-compose.yml` — `pg-A`, `pg-B`, `pg-C`, `runner` with `shared/`
  bind-mounted into all of them (so all four containers see the same FS, the
  way two workstations and a CI runner would).
- `run.sh` — accepts a glob of scenarios, runs each, accumulates TAP output.
- `scenarios/sN_*.sh` — one file per row in the matrix. Each script is
  a small bash program; no test framework.

A scenario looks like:

```bash
#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/../lib.sh"
scenario "A1: UC1 fresh clone pull"

reset_shared
reset_workstation devA
reset_workstation devB
seed_db pg-A fixtures/schemas/01_simple.sql

# devA captures + pushes
exec_in devA dryrun-new snapshot take --db "$PG_A_URL"
exec_in devA dryrun-new snapshot push --to-path /shared

# devB pulls — must work without ever touching Postgres
exec_in devB dryrun-new snapshot pull --from-path /shared
out="$(exec_in devB dryrun-new snapshot list --json)"
assert_jq "$out" 'length == 1'
assert_jq "$out" '.[0].content_hash | length == 64'

ok
```

Where `lib.sh` provides `scenario`, `reset_*`, `exec_in`, `assert_jq`, `ok`,
`fail`. ~60 lines of bash, nothing fancy.

---

## What this suite cannot tell you

- **It can't certify zstd determinism across crate versions.** If `zstd` the
  crate bumps and changes its frame format, F3-style hash equality between
  two HEAD checkouts done a year apart could drift. Pin `zstd = "0.13"`
  hard in `Cargo.toml`, or accept the drift and store `content_hash` of the
  *uncompressed* JSON instead of the file (already what the code does — good).
- **It can't simulate real cross-timezone clock skew at sub-second precision**
  without messing with container clocks. D4 is a coarse simulation.
- **It can't replicate every macOS APFS quirk on Linux Docker.** Run C1 / C'1
  on a real macOS host as a follow-up.

---

## Suggested rollout

1. Land `tests/snapshot-e2e/` skeleton + `lib.sh`.
2. Implement A1–A4 first; they should be 100% green before merging the snapshot-share suite.
3. Implement B1 (NEW reads OLD). If it fails, the plan's premise is broken —
   fix before anything else.
4. C1, C5, D1, D2 next — these are the "loud failure" guarantees the plan
   makes.
5. F3–F5 last; they will produce a divergence report, not pass/fail. Use the
   report to write the migration note in `docs/shared-snapshots.md`.

The full matrix is ~40 scenarios. With `lib.sh` reuse, each is ~30 lines of
bash + one fixture SQL file. Realistic effort: 1–2 days for A+B+C, another
day for D+E, another for F+G.

---

## Initial run findings (2026-05-08)

First end-to-end run with the four scaffolded scenarios on `filesystem-store`
HEAD vs `v0.6.1`:

| #  | Status                              |
| -- | ----------------------------------- |
| A1 | PASS                                |
| B1 | PASS — NEW reads v0.6.1 export      |
| C5 | **FAIL — real bug**                 |
| D1 | **FAIL (intermittent) — real bug**  |

### Bug 1 — no hash verification on read (C5)

`crates/dry_run_core/src/history/filesystem_store.rs:415` (`read_bundle`)
decompresses zstd and deserializes JSON, but **never compares the filename's
`<content_hash>` segment against the deserialized bundle's
`schema.content_hash`**. Worse, the caller `find_bundle_by_schema_hash`
(line 402–413) silently swallows read errors with `Err(_) => continue`.

Effect: a single byte flipped inside a `.json.zst` is propagated into the
puller's local SQLite as if it were valid. We observed an `activity_stats`
hash field arriving with a non-hex character (`e1hd0dabf9eb…`) on the puller
side — silent corruption.

Fix shape: in `read_bundle`, parse the expected hash from the path, recompute
the bundle's `content_hash`, and return `StoreError::CorruptSnapshot` on
mismatch. Stop swallowing errors in `find_bundle_by_schema_hash`.

### Bug 2 — concurrent same-hash writers race on the shared `.tmp` path (D1)

`write_bundle` (`filesystem_store.rs:435`) computes
`tmp = path.with_extension("zst.tmp")`, deterministic per target path. Two
processes pushing the same `(project, database, ts, hash)` both write to the
same `*.zst.tmp`; whichever loses the rename race gets `ENOENT` because the
winner already renamed the tmp away.

The contract promises "concurrent writers of the same hash are idempotent". They
aren't — one writer errors out. Reproduces on roughly half of D1 runs.

Fix shape: use a unique tmp suffix per writer
(`format!("zst.{}.tmp", std::process::id())` plus a counter or `tempfile`).

### Status of the plan's premise

- File-format claim (B1): **holds** — `FilesystemStore::get` reads v0.6.1's
  `snapshot export` output without modification.
- "Loud failure beats quiet drift" claim (§137, C5): **does not hold** —
  needs the fix above before the plan's exit criteria are met.
- Atomic-rename / idempotent-concurrent-write claim (§81, D1): **does not
  hold** — needs the unique-tmp fix.

Recommend gating the snapshot-share suite sign-off on those two fixes plus passing the full
A+B+C+D matrix.

---

## Fixes applied (2026-05-08)

Both bugs from "Initial run findings" are now fixed; the harness reports 4/4
passing across five back-to-back runs.

### Read-side hash verification (`filesystem_store.rs`)

`read_bundle` now extracts the expected sha256 from the filename and:
1. confirms `bundle.schema.content_hash == filename_hash`,
2. recomputes `compute_content_hash(...)` from the schema's structural fields
   and compares against the filename hash,
3. for each `bundle.planner` and `bundle.activity[label]`, re-serializes with
   `content_hash = ""` and asserts the sha256 matches the stored field.

Verification is gated on the filename hash being a 64-char hex string so
existing test fixtures (which use synthetic identifiers like `"h1"`) still
work — production filenames always satisfy that gate.

`find_bundle_by_schema_hash`, `list_kind`, `delete_before`, and
`list_kinds_sync` now propagate `read_bundle` errors via `?` instead of
silently `continue`-ing past corrupt files.

### Unique tmp-file path (`write_bundle`)

`write_bundle` now derives the temp filename from a process-id + monotonic
counter (`zst.{pid}.{n}.tmp`) instead of a deterministic
`path.with_extension("zst.tmp")`. Two concurrent same-hash writers no longer
race on a shared tmp path. On rename failure we also `remove_file(&tmp)` so
nothing lingers.

### Harness fixes uncovered along the way

- `default_data_dir()` resolves to `CWD/.dryrun/`, not `$HOME/.dryrun/`.
  `ws_run` now `cd`s into the workstation dir so each "developer" gets their
  own SQLite history.
- `cmd_init`-style project_id derivation falls back to the CWD basename
  (`devA` vs `devB`), which puts pushes/pulls into different `<project_id>`
  subtrees and makes them invisible to each other. `reset_workstation` now
  drops a `dryrun.toml` with `project.id = "shared"` and a `primary` profile
  pointing at `$DATABASE_URL`.
- `reset_shared` / `reset_workstation` use `find -mindepth 1 -delete`
  because the bind-mount roots can't be `rm -rf`'d.
