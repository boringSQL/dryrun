# A4 — round-trip hash identity: take → push → wipe local SQLite → pull
# → list shows the original snapshot, and a fresh take against the
# unchanged DB yields PutOutcome::Deduped (i.e. same content_hash).
scenario "A4: round-trip hash identity"

reset_shared
reset_workstation devA
reset_db "$PG_A_URL"
seed_db "$PG_A_URL" "$FIXTURES_DIR/schemas/01_simple.sql"

ws_run devA dryrun-new snapshot take --db "$PG_A_URL" >/dev/null 2>&1
ws_run devA dryrun-new snapshot push --to-path "$SHARED_DIR" --db "$PG_A_URL" >/dev/null 2>&1
original_hash="$(find "$SHARED_DIR" -name '*.json.zst' -type f \
    | sed -E 's|.*-([0-9a-f]{64})\.json\.zst$|\1|' | head -1)"
[ -n "$original_hash" ] || fail "no pushed file"

# Wipe local history; pull must recover it from /shared.
rm -f "$WORKSTATIONS_DIR/devA/.dryrun/history.db"*
ws_run devA dryrun-new snapshot pull --from-path "$SHARED_DIR" --db "$PG_A_URL" >/dev/null 2>&1
list_out="$(ws_run devA dryrun-new snapshot list --db "$PG_A_URL" 2>&1)"
assert_contains "$list_out" "${original_hash:0:16}" "list shows pulled hash"

# A fresh take against the unchanged DB must dedupe (same content_hash).
take_out="$(ws_run devA dryrun-new snapshot take --db "$PG_A_URL" 2>&1)"
assert_contains "$take_out" "Schema unchanged" "second take is a no-op (deduped)"

ok
