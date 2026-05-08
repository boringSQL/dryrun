# C5 — flip a byte in a pushed snapshot; pull/get must surface CorruptSnapshot.
scenario "C5: corruption detection — filename hash != recomputed"

reset_shared
reset_workstation devA
reset_workstation devB
reset_db "$PG_A_URL"
seed_db "$PG_A_URL" "$FIXTURES_DIR/schemas/01_simple.sql"

ws_run devA dryrun-new snapshot take --db "$PG_A_URL"
ws_run devA dryrun-new snapshot push --to-path "$SHARED_DIR" --db "$PG_A_URL"

target="$(find "$SHARED_DIR" -name '*.json.zst' -type f | head -n 1)"
[ -n "$target" ] || { fail "no pushed snapshot found"; ok; return; }

# Flip one byte in the middle of the file (zstd payload).
size=$(stat -c%s "$target")
mid=$((size / 2))
printf '\x42' | dd of="$target" bs=1 count=1 seek=$mid conv=notrunc status=none

# NEW pull must fail loudly, not silently accept the corrupt file.
if out="$(ws_run devB dryrun-new snapshot pull --from-path "$SHARED_DIR" --db "$PG_A_URL" 2>&1)"; then
    fail "pull SUCCEEDED on corrupt file (expected loud failure): $out"
else
    assert_contains "$out" "corrupt" "error mentions corruption"
fi

ok
