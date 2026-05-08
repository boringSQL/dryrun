# B1 — NEW must read v0.6.1's `snapshot export` output unchanged.
# This is the single most load-bearing claim in the plan.
# NATIVE: skip — needs dryrun-old (v0.6.1) which only the Docker image carries.
scenario "B1: NEW reads OLD export output"

reset_shared
reset_workstation devA
reset_workstation devB
reset_db "$PG_A_URL"
seed_db "$PG_A_URL" "$FIXTURES_DIR/schemas/01_simple.sql"

# OLD: take + export to shared/   (v0.6.1 export takes no --db, reads history.db)
ws_run devA dryrun-old snapshot take --db "$PG_A_URL"
ws_run devA dryrun-old snapshot export --out "$SHARED_DIR" 2>&1 || \
    fail "OLD snapshot export failed (verify v0.6.1 surface)"

# NEW: pull from the same dir on a fresh workstation
out="$(ws_run devB dryrun-new snapshot pull --from-path "$SHARED_DIR" --db "$PG_A_URL" 2>&1)" \
    || fail "NEW pull failed against OLD export: $out"

# diff against a fresh take from the same DB — should be empty changeset
diff_out="$(ws_run devB dryrun-new snapshot diff --db "$PG_A_URL" --latest 2>&1)" \
    || fail "NEW diff against pulled OLD snapshot failed: $diff_out"

ok
