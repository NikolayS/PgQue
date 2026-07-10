#!/usr/bin/env bash
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
set -Eeuo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "${repo_root}"

workflow=${1:-.github/workflows/ci.yml}
if grep -nE 'done[[:space:]]*\|\|' "${workflow}"; then
  echo "FAIL: readiness loop can exit successfully after its final sleep"
  exit 1
fi

containers=(
  pgque-test
  pgque-stable-smoke
  pgque-python-test
  pgque-go-test
  pgque-ts-test
  pgque-ruby-test
)
for container in "${containers[@]}"; do
  if ! grep -Fq "bash ci/wait-for-postgres.sh ${container}" "${workflow}"; then
    echo "FAIL: ${container} does not use the tested readiness helper"
    exit 1
  fi
done

# These functions are invoked by the helper's child Bash process.
# shellcheck disable=SC2329
docker() {
  [[ "$#" -eq 11 \
    && "$1" == exec \
    && "$2" == -e \
    && "$3" == PGPASSWORD=pgque_test \
    && "$4" == pgque-ready \
    && "$5" == psql \
    && "$6" == -U \
    && "$7" == postgres \
    && "$8" == -d \
    && "$9" == pgque_test \
    && "${10}" == -c \
    && "${11}" == 'select 1' ]]
}

# shellcheck disable=SC2329
sleep() {
  :
}

export -f docker sleep
bash ci/wait-for-postgres.sh pgque-ready 1

# shellcheck disable=SC2329
docker() {
  if [[ "$1" == logs && "$2" == pgque-timeout ]]; then
    echo "expected container log"
    return 0
  fi
  return 1
}
export -f docker

if output=$(bash ci/wait-for-postgres.sh pgque-timeout 2 2>&1); then
  echo "FAIL: readiness helper accepted an unavailable target database"
  exit 1
fi
grep -Fq 'PG not ready after 2 seconds' <<<"${output}"
grep -Fq 'expected container log' <<<"${output}"

echo "PASS: PostgreSQL readiness checks fail closed"
