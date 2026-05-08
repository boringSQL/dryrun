#!/usr/bin/env bash
# Native fast-feedback loop: HEAD `dryrun` binary built locally, against a
# single pg-a container, no runner image. ~10x faster iteration than the
# full Docker harness. Skips cross-version (OLD vs NEW) scenarios — those
# require the dual-binary image and should run via ./harness.sh.
#
# Usage:
#   ./run-native.sh                 # all native-eligible scenarios
#   ./run-native.sh 's01*.sh'       # filter

set -uo pipefail
cd "$(dirname "$0")"
ROOT="$(cd ../.. && pwd)"

# 1. Build the local binary in release mode (cached after first run).
( cd "$ROOT" && cargo build --release --bin dryrun --quiet ) || {
    echo "cargo build failed" >&2
    exit 1
}

# 2. Bring up just pg-a.
docker compose up -d --wait pg-a >/dev/null

# 3. Wire env so scenarios that use $PG_A_URL / dryrun-new work as on Docker.
SCRATCH="$(mktemp -d)"
trap 'rm -rf "$SCRATCH"' EXIT

export SHARED_DIR="$SCRATCH/shared"
export WORKSTATIONS_DIR="$SCRATCH/workstations"
export FIXTURES_DIR="$PWD/fixtures"
export PG_A_URL="postgres://postgres:dryrun@127.0.0.1:$(docker compose port pg-a 5432 | cut -d: -f2)/app"
mkdir -p "$SHARED_DIR" "$WORKSTATIONS_DIR"

# Shadow `dryrun-new` / `dryrun-old` resolution so ws_run finds them.
# Native runs only support dryrun-new (HEAD); scenarios that need dryrun-old
# bail by tagging themselves `# NATIVE: skip`.
BIN_DIR="$SCRATCH/bin"
mkdir -p "$BIN_DIR"
ln -sf "$ROOT/target/release/dryrun" "$BIN_DIR/dryrun-new"
cat > "$BIN_DIR/dryrun-old" <<'EOF'
#!/usr/bin/env bash
echo "dryrun-old not available in native mode — use ./harness.sh" >&2
exit 127
EOF
chmod +x "$BIN_DIR/dryrun-old"
export PATH="$BIN_DIR:$PATH"

filter="${1:-*.sh}"
total=0
passed=0
failed_list=()

for s in scenarios/$filter; do
    [ -f "$s" ] || continue
    if grep -q '^# NATIVE: skip' "$s"; then
        echo "# skipped (native): $(basename "$s")"
        continue
    fi
    total=$((total + 1))
    if bash -c ". ./lib.sh; . $s"; then
        passed=$((passed + 1))
    else
        failed_list+=("$(basename "$s")")
    fi
done

echo
echo "1..$total"
echo "passed: $passed / $total"
if [ "${#failed_list[@]}" -gt 0 ]; then
    echo "failed:"
    printf '  - %s\n' "${failed_list[@]}"
    exit 1
fi
