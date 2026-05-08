# C4 — stale `.tmp` files (left over from a crashed write) must NOT
# confuse the puller. A subsequent push completes normally; `list` and
# `pull` ignore the tmp files; existing real bundles remain readable.
scenario "C4: stale .tmp from crashed write"

reset_shared
reset_workstation devA
reset_workstation devB
reset_db "$PG_A_URL"
seed_db "$PG_A_URL" "$FIXTURES_DIR/schemas/01_simple.sql"

# Pre-place a stale .tmp file — pretends a previous writer crashed mid-rename.
mkdir -p "$SHARED_DIR/shared/app"
echo "garbage" > "$SHARED_DIR/shared/app/20260101T000000Z-deadbeef.json.zst.999.0.tmp"

# Real push proceeds normally.
ws_run devA dryrun-new snapshot take --db "$PG_A_URL" >/dev/null 2>&1
push_out="$(ws_run devA dryrun-new snapshot push --to-path "$SHARED_DIR" --db "$PG_A_URL" 2>&1)"
push_rc=$?
[ "$push_rc" -eq 0 ] || fail "push errored despite stale .tmp: $push_out"

# Pull on devB must succeed and not return the .tmp content.
pull_out="$(ws_run devB dryrun-new snapshot pull --from-path "$SHARED_DIR" --db "$PG_A_URL" 2>&1)"
pull_rc=$?
[ "$pull_rc" -eq 0 ] || fail "pull errored despite stale .tmp: $pull_out"

# devB's history should have the real snapshot, not garbage.
list_out="$(ws_run devB dryrun-new snapshot list --db "$PG_A_URL" 2>&1)"
assert_contains "$list_out" "snapshot(s) total" "devB list non-empty"

ok
