# D2 — two workstations push DIFFERENT-hash snapshots concurrently. Both
# files end up in /shared, ordered by their own filename ts. No `.tmp`
# debris, no rename failures.
scenario "D2: concurrent push of distinct snapshots"

reset_shared
reset_workstation devA
reset_workstation devB
reset_db "$PG_A_URL"
seed_db "$PG_A_URL" "$FIXTURES_DIR/schemas/01_simple.sql"

# devA captures the seeded schema.
ws_run devA dryrun-new snapshot take --db "$PG_A_URL" >/dev/null 2>&1

# Mutate the DB; devB captures the new shape → distinct content_hash.
psql "$PG_A_URL" -v ON_ERROR_STOP=1 -c "ALTER TABLE orders ADD COLUMN currency TEXT;" >/dev/null
sleep 1
ws_run devB dryrun-new snapshot take --db "$PG_A_URL" >/dev/null 2>&1

# Race the two pushes.
ws_run devA dryrun-new snapshot push --to-path "$SHARED_DIR" --db "$PG_A_URL" >/dev/null 2>&1 &
pid_a=$!
ws_run devB dryrun-new snapshot push --to-path "$SHARED_DIR" --db "$PG_A_URL" >/dev/null 2>&1 &
pid_b=$!
wait $pid_a || fail "devA push failed"
wait $pid_b || fail "devB push failed"

assert_no_tmp_files "$SHARED_DIR"

count="$(find "$SHARED_DIR" -name '*.json.zst' -type f | wc -l | tr -d ' ')"
assert_eq "$count" "2" "two distinct snapshot files survived"

ok
