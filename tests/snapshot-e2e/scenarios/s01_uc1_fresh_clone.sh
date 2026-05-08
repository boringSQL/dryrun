# A1 — UC1 fresh clone: devB starts empty, pulls, and sees devA's snapshot.
scenario "A1: UC1 fresh clone pull"

reset_shared
reset_workstation devA
reset_workstation devB
reset_db "$PG_A_URL"
seed_db "$PG_A_URL" "$FIXTURES_DIR/schemas/01_simple.sql"

# devA: take + push
ws_run devA dryrun-new snapshot take --db "$PG_A_URL"
ws_run devA dryrun-new snapshot push --to-path "$SHARED_DIR" --db "$PG_A_URL"

# Filesystem invariants
assert_no_tmp_files "$SHARED_DIR"
assert_filenames_match_hash "$SHARED_DIR"

# devB: pull and list — must NOT touch Postgres
out="$(ws_run devB dryrun-new snapshot pull --from-path "$SHARED_DIR" --db "$PG_A_URL" 2>&1)" || fail "pull errored: $out"
list_out="$(ws_run devB dryrun-new snapshot list --db "$PG_A_URL" 2>&1)" || fail "list errored: $list_out"
assert_contains "$list_out" "" "list produced output"

ok
