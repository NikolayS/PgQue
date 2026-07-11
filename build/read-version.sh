#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# Read and validate the version returned by pgque.version().

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source_file=${1:-${repo_root}/devel/sql/pgque-additions/lifecycle.sql}

version=$(awk '
  /create or replace function pgque\.version/ { in_fn=1; next }
  in_fn && $0 !~ /^[[:space:]]*--/ && match($0, /return '\''[^'\'']+'\''/) {
    print substr($0, RSTART+8, RLENGTH-9)
    matches++
    in_fn=0
  }
  END { if (matches != 1) exit 1 }
' "${source_file}") || {
  echo "FAIL: could not read pgque.version() from ${source_file}" >&2
  exit 1
}

if ! [[ "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.]+)?$ ]]; then
  echo "FAIL: pgque version is not SemVer: ${version}" >&2
  exit 1
fi

echo "${version}"
