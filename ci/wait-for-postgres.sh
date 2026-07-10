#!/usr/bin/env bash
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
set -Eeuo pipefail

container=${1:?usage: wait-for-postgres.sh CONTAINER [ATTEMPTS]}
attempts=${2:-30}

if [[ ! "${attempts}" =~ ^[1-9][0-9]*$ ]]; then
  echo "ATTEMPTS must be a positive integer" >&2
  exit 2
fi

ready=0
for _ in $(seq 1 "${attempts}"); do
  if docker exec -e PGPASSWORD=pgque_test "${container}" \
    psql -U postgres -d pgque_test -c 'select 1' >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done

if [[ "${ready}" -ne 1 ]]; then
  echo "PG not ready after ${attempts} seconds"
  docker logs "${container}"
  exit 1
fi
