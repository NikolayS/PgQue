#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# Regression checks for the shared CI Postgres readiness helper.

main() {
  local repo_root
  local workflow
  local container
  local output

  repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  cd "${repo_root}"

  workflow="${1:-.github/workflows/ci.yml}"
  if grep -nE 'done[[:space:]]*\|\|' "${workflow}"; then
    echo "FAIL: readiness loop can exit successfully after its final sleep" >&2
    exit 1
  fi

  local -a containers=(
    pgque-test
    pgque-stable-smoke
    pgque-python-test
    pgque-go-test
    pgque-ts-test
    pgque-ruby-test
  )
  for container in "${containers[@]}"; do
    if ! grep -Fq "bash ci/wait-for-postgres.sh ${container}" "${workflow}"; then
      echo "FAIL: ${container} does not use the tested readiness helper" >&2
      exit 1
    fi
  done

  if ! grep -Fq "IFS=\$'\\n\\t'" ci/wait-for-postgres.sh; then
    echo "FAIL: readiness helper must set the safe shell IFS" >&2
    exit 1
  fi

  # These functions are invoked by the helper's child Bash process.
  # shellcheck disable=SC2329
  docker() {
    [[ "${#}" -eq 11 \
      && "${1}" == exec \
      && "${2}" == -e \
      && "${3}" == PGPASSWORD=pgque_test \
      && "${4}" == -e \
      && "${5}" == PAGER=cat \
      && "${6}" == pgque-ready \
      && "${7}" == psql \
      && "${8}" == --no-psqlrc \
      && "${9}" == --username=postgres \
      && "${10}" == --dbname=pgque_test \
      && "${11}" == --command=select\ 1 ]]
  }

  # shellcheck disable=SC2329
  sleep() {
    :
  }

  export -f docker sleep
  bash ci/wait-for-postgres.sh pgque-ready 1

  # shellcheck disable=SC2329
  docker() {
    if [[ "${1}" == logs && "${2}" == pgque-timeout ]]; then
      echo "expected container log"
      return 0
    fi
    return 1
  }
  export -f docker

  if output=$(bash ci/wait-for-postgres.sh pgque-timeout 2 2>&1); then
    echo "FAIL: readiness helper accepted an unavailable target database" >&2
    exit 1
  fi
  grep -Fq 'Postgres not ready after 2 seconds' <<<"${output}"
  grep -Fq 'expected container log' <<<"${output}"

  for invalid_attempts in 0 not-a-number; do
    if output=$(bash ci/wait-for-postgres.sh \
      pgque-timeout "${invalid_attempts}" 2>&1); then
      echo "FAIL: readiness helper accepted invalid attempts" >&2
      exit 1
    fi
    grep -Fq 'ATTEMPTS must be a positive integer' <<<"${output}"
  done

  echo "PASS: Postgres readiness checks fail closed"
}

main "$@"
