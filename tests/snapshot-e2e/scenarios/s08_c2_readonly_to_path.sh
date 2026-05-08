# C2 — push --to-path against a read-only directory: must exit non-zero
# with a clean filesystem error, never panic. Pull from the same dir must
# still succeed (read-only is fine for pulling).
scenario "C2: read-only --to-path"

reset_shared
reset_workstation devA
reset_workstation devB
reset_db "$PG_A_URL"
seed_db "$PG_A_URL" "$FIXTURES_DIR/schemas/01_simple.sql"

# Seed /shared via a normal push, then drop write perms.
ws_run devA dryrun-new snapshot take --db "$PG_A_URL" >/dev/null 2>&1
ws_run devA dryrun-new snapshot push --to-path "$SHARED_DIR" --db "$PG_A_URL" >/dev/null 2>&1
chmod -R a-w "$SHARED_DIR"

# A second push (after another schema change) must fail loudly.
psql "$PG_A_URL" -v ON_ERROR_STOP=1 -c "ALTER TABLE users ADD COLUMN city TEXT;" >/dev/null
sleep 1
ws_run devA dryrun-new snapshot take --db "$PG_A_URL" >/dev/null 2>&1
push_out="$(ws_run devA dryrun-new snapshot push --to-path "$SHARED_DIR" --db "$PG_A_URL" 2>&1)"
push_rc=$?
if [ "$push_rc" -eq 0 ]; then
    fail "push to read-only dir SUCCEEDED (exit 0); should have errored"
fi
assert_contains "$push_out" "error" "error message present"

# Pull from the read-only dir must still work.
pull_out="$(ws_run devB dryrun-new snapshot pull --from-path "$SHARED_DIR" --db "$PG_A_URL" 2>&1)"
pull_rc=$?
[ "$pull_rc" -eq 0 ] || fail "pull from read-only dir failed: $pull_out"

chmod -R u+w "$SHARED_DIR"   # cleanup so reset_shared can wipe next run
ok
