# A2 — UC2 versioned history: multi-take/push produces ordered, distinct
# snapshots; `list` returns them by descending ts; `diff --from --to`
# resolves them to a non-empty changeset.
scenario "A2: versioned history (multi-take/push)"

reset_shared
reset_workstation devA
reset_db "$PG_A_URL"
seed_db "$PG_A_URL" "$FIXTURES_DIR/schemas/01_simple.sql"

ws_run devA dryrun-new snapshot take --db "$PG_A_URL" >/dev/null 2>&1

# Mutate schema and take again → second snapshot must have a different hash.
psql "$PG_A_URL" -v ON_ERROR_STOP=1 -c "ALTER TABLE users ADD COLUMN nickname TEXT;" >/dev/null
sleep 1   # filename ts uses second precision; force a distinct slot
ws_run devA dryrun-new snapshot take --db "$PG_A_URL" >/dev/null 2>&1

ws_run devA dryrun-new snapshot push --to-path "$SHARED_DIR" --db "$PG_A_URL" >/dev/null 2>&1

count="$(find "$SHARED_DIR" -name '*.json.zst' -type f | wc -l | tr -d ' ')"
assert_eq "$count" "2" "two distinct snapshots in /shared"

# list output should mention 2 snapshot lines.
list_out="$(ws_run devA dryrun-new snapshot list --db "$PG_A_URL" 2>&1)"
listed="$(echo "$list_out" | grep -cE '^[0-9]{4}-[0-9]{2}-[0-9]{2}')"
assert_eq "$listed" "2" "list reports both snapshots"

# diff between the two hashes must be a non-empty changeset (added column).
hashes=($(find "$SHARED_DIR" -name '*.json.zst' -type f \
    | sed -E 's|.*-([0-9a-f]{64})\.json\.zst$|\1|' | sort -u))
assert_eq "${#hashes[@]}" "2" "two distinct content hashes"
diff_out="$(ws_run devA dryrun-new snapshot diff --from "${hashes[0]}" --to "${hashes[1]}" --db "$PG_A_URL" 2>&1)"
assert_contains "$diff_out" "nickname" "diff mentions added column"

ok
