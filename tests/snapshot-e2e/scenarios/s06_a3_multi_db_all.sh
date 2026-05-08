# A3 — UC3 multi-database `--all`: dryrun.toml declares two database_ids
# bound to the same physical DB. `take` per profile + `push --all` writes
# two subtrees; `pull --all` on a fresh workstation rebuilds both.
scenario "A3: multi-database --all"

reset_shared
reset_workstation devA
reset_workstation devB
reset_db "$PG_A_URL"
seed_db "$PG_A_URL" "$FIXTURES_DIR/schemas/01_simple.sql"

# Override the default single-profile toml with a two-profile config.
cat > "$WORKSTATIONS_DIR/devA/dryrun.toml" <<EOF
[project]
id = "shared"

[default]
profile = "auth"

[profiles.auth]
db_url = "\${DATABASE_URL}"
database_id = "auth"

[profiles.billing]
db_url = "\${DATABASE_URL}"
database_id = "billing"
EOF
cp "$WORKSTATIONS_DIR/devA/dryrun.toml" "$WORKSTATIONS_DIR/devB/dryrun.toml"

# Take in each profile → two streams locally.
DATABASE_URL="$PG_A_URL" ws_run devA dryrun-new --profile auth snapshot take >/dev/null 2>&1
DATABASE_URL="$PG_A_URL" ws_run devA dryrun-new --profile billing snapshot take >/dev/null 2>&1

# Push --all should ship both streams.
DATABASE_URL="$PG_A_URL" ws_run devA dryrun-new snapshot push --to-path "$SHARED_DIR" --all >/dev/null 2>&1

assert_file_exists "$(find "$SHARED_DIR/shared/auth" -name '*.json.zst' | head -1)"
assert_file_exists "$(find "$SHARED_DIR/shared/billing" -name '*.json.zst' | head -1)"

# Pull --all on devB rebuilds both.
DATABASE_URL="$PG_A_URL" ws_run devB dryrun-new snapshot pull --from-path "$SHARED_DIR" --all >/dev/null 2>&1
auth_list="$(DATABASE_URL="$PG_A_URL" ws_run devB dryrun-new --profile auth snapshot list 2>&1)"
billing_list="$(DATABASE_URL="$PG_A_URL" ws_run devB dryrun-new --profile billing snapshot list 2>&1)"
assert_contains "$auth_list" "snapshot(s) total" "auth list non-empty"
assert_contains "$billing_list" "snapshot(s) total" "billing list non-empty"

ok
