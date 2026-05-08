#!/usr/bin/env bash
set -uo pipefail

SCENARIOS_DIR="${SCENARIOS_DIR:-/scenarios}"
LIB="${LIB:-/lib.sh}"

filter="${1:-*.sh}"
total=0
passed=0
failed_list=()

for s in "$SCENARIOS_DIR"/$filter; do
    [ -f "$s" ] || continue
    total=$((total+1))
    name="$(basename "$s")"
    if bash -c ". $LIB; . $s"; then
        passed=$((passed+1))
    else
        failed_list+=("$name")
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
