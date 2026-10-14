#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# Wait for the PgQue CI target database or fail with container diagnostics.

container="${1:?usage: wait-for-postgres.sh CONTAINER [ATTEMPTS]}"
attempts="${2:-30}"

if [[ ! "${attempts}" =~ ^[1-9][0-9]*$ ]]; then
  echo "ATTEMPTS must be a positive integer" >&2
  exit 2
fi

ready=0
for ((attempt = 1; attempt <= attempts; attempt++)); do
  if timeout 5s bash -c 'docker "$@"' _ exec \
    -e PGPASSWORD=pgque_test \
    -e PGCONNECT_TIMEOUT=3 \
    -e 'PGOPTIONS=-c statement_timeout=3000' \
    -e PAGER=cat \
    "${container}" \
    psql \
    --no-psqlrc \
    --host=127.0.0.1 \
    --username=postgres \
    --dbname=pgque_test \
    --command='select 1' \
    >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done

if [[ "${ready}" -ne 1 ]]; then
  echo "Postgres not ready after ${attempts} attempts" >&2
  timeout 5s bash -c 'docker "$@"' _ logs "${container}" >&2 || true
  exit 1
fi
