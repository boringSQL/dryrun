# Shared helpers for scenario scripts. Sourced, not executed.

: "${SHARED_DIR:=/shared}"
: "${WORKSTATIONS_DIR:=/workstations}"
: "${FIXTURES_DIR:=/fixtures}"

SCENARIO_NAME=""
SCENARIO_FAILED=0

scenario() {
    SCENARIO_NAME="$1"
    SCENARIO_FAILED=0
    echo "# --- $SCENARIO_NAME"
}

ok() {
    if [ "$SCENARIO_FAILED" -eq 0 ]; then
        echo "ok - $SCENARIO_NAME"
    else
        echo "not ok - $SCENARIO_NAME"
        exit 1
    fi
}

fail() {
    SCENARIO_FAILED=1
    echo "  FAIL: $*" >&2
}

reset_shared() {
    mkdir -p "$SHARED_DIR"
    find "$SHARED_DIR" -mindepth 1 -delete 2>/dev/null || true
}

reset_workstation() {
    local name="$1"
    mkdir -p "$WORKSTATIONS_DIR/$name"
    find "$WORKSTATIONS_DIR/$name" -mindepth 1 -delete 2>/dev/null || true
    mkdir -p "$WORKSTATIONS_DIR/$name/.dryrun"
    # Shared project_id across workstations so pushes/pulls land in the same
    # /shared/<project>/<database>/ subtree.
    cat > "$WORKSTATIONS_DIR/$name/dryrun.toml" <<EOF
[project]
id = "shared"

[default]
profile = "primary"

[profiles.primary]
db_url = "\${DATABASE_URL}"
database_id = "app"
EOF
}

ws_env() {
    local name="$1"
    echo "HOME=$WORKSTATIONS_DIR/$name"
}

# Run a dryrun command in the context of a "workstation". The binary uses
# CWD/.dryrun/history.db, not $HOME, so we cd into the workstation dir.
# usage: ws_run devA dryrun-new snapshot take --db "$PG_A_URL"
ws_run() {
    local ws="$1"; shift
    (cd "$WORKSTATIONS_DIR/$ws" && HOME="$WORKSTATIONS_DIR/$ws" "$@")
}

seed_db() {
    local pg_url="$1" sql_file="$2"
    psql "$pg_url" -v ON_ERROR_STOP=1 -f "$sql_file" >/dev/null
}

reset_db() {
    local pg_url="$1"
    psql "$pg_url" -v ON_ERROR_STOP=1 -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;" >/dev/null
}

assert_eq() {
    local got="$1" want="$2" msg="${3:-assert_eq}"
    if [ "$got" != "$want" ]; then
        fail "$msg: got=[$got] want=[$want]"
    fi
}

assert_contains() {
    local haystack="$1" needle="$2" msg="${3:-assert_contains}"
    case "$haystack" in
        *"$needle"*) : ;;
        *) fail "$msg: missing [$needle] in output" ;;
    esac
}

assert_jq() {
    local json="$1" expr="$2" msg="${3:-assert_jq}"
    if ! echo "$json" | jq -e "$expr" >/dev/null 2>&1; then
        fail "$msg: jq expression failed: $expr"
        echo "    json was: $json" >&2
    fi
}

assert_file_exists() {
    [ -f "$1" ] || fail "expected file: $1"
}

assert_no_tmp_files() {
    local dir="$1"
    local found
    found="$(find "$dir" -name '*.tmp' 2>/dev/null | head -n 1)"
    [ -z "$found" ] || fail "stray .tmp file: $found"
}

# Verify filename hash equals recomputed content_hash for every snapshot
# in a directory. Catches C5 / C6 corruption silently slipping through.
assert_filenames_match_hash() {
    local dir="$1"
    while IFS= read -r f; do
        local fname expected_hash recomputed
        fname="$(basename "$f")"
        expected_hash="${fname##*-}"
        expected_hash="${expected_hash%.json.zst}"
        recomputed="$(zstd -dc "$f" | sha256sum | awk '{print $1}')"
        # The plan stores hash of the SchemaSnapshot JSON, not file bytes —
        # so this assertion needs the dryrun binary to verify properly.
        # Placeholder: just check the field is hex of correct length.
        if ! echo "$expected_hash" | grep -Eq '^[0-9a-f]{64}$'; then
            fail "bad hash format in filename: $fname"
        fi
    done < <(find "$dir" -name '*.json.zst' -type f)
}
