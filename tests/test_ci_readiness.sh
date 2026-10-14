#!/usr/bin/env bash
set -Eeuo pipefail

# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
cd "$(dirname "${BASH_SOURCE[0]}")/.."
workflow="${1:-.github/workflows/ci.yml}"
if grep -nE 'done[[:space:]]*\|\|' "${workflow}"; then
  echo "FAIL: readiness loop can succeed after its final sleep" >&2
  exit 1
fi

# Exercise the subprocess contract without starting a container.
# shellcheck disable=SC2329
docker() {
  if [[ "${1}" == logs ]]; then
    echo 'database startup diagnostic'
    return 0
  fi
  [[ "${READY}" == yes && "$*" == *'--dbname=pgque_test'* ]]
}
# shellcheck disable=SC2329
sleep() { :; }
export -f docker sleep
export READY=yes
bash ci/wait-for-postgres.sh ready 1

READY=no
if output=$(bash ci/wait-for-postgres.sh unavailable 2 2>&1); then
  echo 'FAIL: unavailable database accepted' >&2
  exit 1
fi
grep -Fq 'Postgres not ready after 2 seconds' <<<"${output}"
grep -Fq 'database startup diagnostic' <<<"${output}"

for attempts in 0 invalid; do
  if bash ci/wait-for-postgres.sh unavailable "${attempts}" >/dev/null 2>&1; then
    echo 'FAIL: invalid attempts accepted' >&2
    exit 1
  fi
done

echo 'PASS: target-database readiness fails closed'
