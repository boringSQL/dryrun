# D1 — two workstations push the same snapshot concurrently.
# Idempotency contract: final state is exactly one file, no .tmp debris.
scenario "D1: concurrent push of identical snapshot"

reset_shared
reset_workstation devA
reset_workstation devB
reset_db "$PG_A_URL"
seed_db "$PG_A_URL" "$FIXTURES_DIR/schemas/01_simple.sql"

# Both devs take from the same DB at the same point → identical content_hash.
ws_run devA dryrun-new snapshot take --db "$PG_A_URL"
# Copy devA's history to devB so they have identical local snapshots.
cp -r "$WORKSTATIONS_DIR/devA/.dryrun/." "$WORKSTATIONS_DIR/devB/.dryrun/"

ws_run devA dryrun-new snapshot push --to-path "$SHARED_DIR" --db "$PG_A_URL" &
pid_a=$!
ws_run devB dryrun-new snapshot push --to-path "$SHARED_DIR" --db "$PG_A_URL" &
pid_b=$!
wait $pid_a || fail "devA push failed"
wait $pid_b || fail "devB push failed"

assert_no_tmp_files "$SHARED_DIR"

count="$(find "$SHARED_DIR" -name '*.json.zst' -type f | wc -l | tr -d ' ')"
assert_eq "$count" "1" "exactly one snapshot file (idempotent)"

ok
