# snapshot-e2e

End-to-end black-box tests for the shared-filesystem snapshot store
(`dryrun snapshot push --to-path` / `pull --from-path`), including
cross-version compatibility against the **v0.6.1** baseline.

The Rust unit tests in `crates/dry_run_core/src/history/filesystem_store.rs`
cover internal correctness. This suite covers the things only a real
multi-process / multi-binary / real-Postgres setup can: cross-version
compat, concurrent writers, filesystem corruption, multi-database flows.

## Running

```sh
./harness.sh                  # all scenarios, full Docker (HEAD + v0.6.1)
./harness.sh 's04*.sh'        # filter scenarios by glob
./harness.sh -- bash          # drop into the runner container
./harness.sh down             # stop the stack
./harness.sh rebuild          # rebuild after CLI code changes

./run-native.sh               # HEAD only, host cargo build, single pg-a
./run-native.sh 's01*.sh'     # filter
```

`harness.sh` keeps the runner container alive between invocations
(`up -d` + `exec`), so warm runs land in **~1.5–2 s**. `run-native.sh`
skips Docker for the dryrun binary entirely — best for iterating while
authoring new scenarios.

Scenarios that need the v0.6.1 binary tag themselves
`# NATIVE: skip`; `run-native.sh` honors that.

## Layout

```
snapshot-e2e/
├── docker-compose.yml          # pg-a, pg-b, pg-c (tmpfs), persistent runner
├── Dockerfile.dryrun           # multi-stage: dryrun-old (v0.6.1) + dryrun-new (HEAD)
├── harness.sh                  # full-Docker entrypoint
├── run-native.sh               # native fast-feedback entrypoint
├── run.sh                      # invoked inside the runner; aggregates scenarios
├── lib.sh                      # shared helpers: scenario / reset_* / ws_run / assert_*
├── fixtures/schemas/*.sql      # seed SQL
├── scenarios/sNN_*.sh          # one bash script per scenario
├── shared/                     # bind-mounted "team filesystem" (gitignored)
└── workstations/{devA,devB}/   # per-workstation HOMEs (gitignored)
```

## Adding a scenario

Each scenario is ~30 lines of bash. Copy an existing one in `scenarios/`
and tweak. Helpers from `lib.sh`:

| Helper                          | What it does                                                             |
| ------------------------------- | ------------------------------------------------------------------------ |
| `scenario "TITLE"`              | Names the scenario; `ok` / `fail` print TAP-ish output.                  |
| `reset_shared`                  | Wipes the shared dir.                                                    |
| `reset_workstation devA`        | Clears `workstations/devA/` and writes a fresh `dryrun.toml`.            |
| `reset_db / seed_db <url> <sql>`| Drops `public` and re-seeds.                                             |
| `ws_run devA <argv...>`         | Runs a command with `cd` + `HOME` set to the workstation dir.            |
| `assert_eq`, `assert_contains`, `assert_no_tmp_files`, `assert_jq`, … | Cheap assertions; failures print but don't `exit`. |

Naming: `sNN_<id>_<short-description>.sh` where `<id>` matches a row in
the design doc (`internal-docs/snapshot-share-tests.md`). Scenarios that
require the v0.6.1 binary should put `# NATIVE: skip` near the top.

## Tearing down

```sh
./harness.sh down       # stop containers
docker compose down -v  # also drop networks (rare)
```

The `shared/` and `workstations/` bind-mount roots persist between runs;
`reset_*` clears their contents at scenario start.
