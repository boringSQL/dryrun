#!/usr/bin/env bash
# Persistent-runner wrapper. Brings the stack up once, then `exec`s the
# scenarios against the running container — saves ~3-5s per invocation
# vs `docker compose run --rm`.
#
# Usage:
#   ./harness.sh                  # run all scenarios
#   ./harness.sh 's03*.sh'        # filter scenarios by glob
#   ./harness.sh -- bash          # drop into a shell in the runner
#   ./harness.sh down             # stop the stack
#   ./harness.sh rebuild          # rebuild the runner image (after code changes)

set -uo pipefail
cd "$(dirname "$0")"
# Bind-mount roots — must exist before `docker compose up` so the
# container's /shared and /workstations land on a writable host dir.
mkdir -p shared workstations

case "${1:-run}" in
    down)
        exec docker compose down
        ;;
    rebuild)
        exec docker compose build runner
        ;;
    --)
        shift
        docker compose up -d --wait >/dev/null
        exec docker compose exec runner "$@"
        ;;
    *)
        docker compose up -d --wait >/dev/null
        docker compose exec runner bash /run.sh "$@"
        ;;
esac
