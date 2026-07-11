#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# Exercise both documentation channels and fail-closed scan behavior.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workdir=

cleanup() {
  if [[ -n "${workdir}" ]]; then
    rm -rf -- "${workdir}"
  fi
}

write_development_fixture() {
  local root="${1}"

  mkdir -p "${root}/docs" "${root}/web/src/pages"
  printf '%s\n' development > "${root}/docs/.release-channel"
  printf '%s\n' \
    'Development documentation:' \
    '\i devel/sql/pgque.sql' > "${root}/README.md"
  printf '%s\n' 'Build contract.' > "${root}/docs/README.md"
  printf '%s\n' \
    "This page follows the \`main\` branch's in-development build" \
    '\i devel/sql/pgque.sql' > "${root}/docs/installation.md"
  printf '%s\n' \
    'This is the public API reference for the in-development default install' \
    'https://github.com/NikolayS/pgque/blob/main/devel/sql/pgque.sql' \
    > "${root}/docs/reference.md"
  # shellcheck disable=SC2016 # Fixture contains literal Markdown backticks.
  printf '%s\n' \
    'tutorial follows the `main` branch development build' \
    '\i devel/sql/pgque.sql' > "${root}/docs/tutorial.md"
  printf '%s\n' '\\i devel/sql/pgque.sql' \
    > "${root}/web/src/pages/index.astro"
}

write_stable_fixture() {
  local root="${1}"

  mkdir -p "${root}/docs" "${root}/web/src/pages"
  printf '%s\n' 'stable:v1.2.3' > "${root}/docs/.release-channel"
  printf '%s\n' '\i sql/pgque.sql' > "${root}/README.md"
  : > "${root}/docs/README.md"
  printf '%s\n' '\i sql/pgque.sql' > "${root}/docs/installation.md"
  printf '%s\n' \
    'https://github.com/NikolayS/pgque/blob/v1.2.3/sql/pgque.sql' \
    > "${root}/docs/reference.md"
  printf '%s\n' '\i sql/pgque.sql' > "${root}/docs/tutorial.md"
  printf '%s\n' '\\i sql/pgque.sql' > "${root}/web/src/pages/index.astro"
}

assert_finite_ttl_wording() {
  local file

  for file in docs/reference.md docs/producer-idempotency.md; do
    if ! grep -Fq 'positive finite interval' "${file}"; then
      echo "FAIL: ${file} must document a positive finite TTL interval" >&2
      exit 1
    fi
  done
}

main() {
  local development_root
  local stable_root
  local output

  cd "${repo_root}"
  assert_finite_ttl_wording
  workdir="$(mktemp -d)"
  trap cleanup EXIT

  development_root="${workdir}/development"
  write_development_fixture "${development_root}"
  PGQUE_DOCS_ROOT="${development_root}" \
    bash build/check-docs-contract.sh >/dev/null

  stable_root="${workdir}/stable"
  write_stable_fixture "${stable_root}"
  PGQUE_DOCS_ROOT="${stable_root}" \
    bash build/check-docs-contract.sh >/dev/null

  printf '%s\n' 'devel/sql/pgque.sql' >> "${stable_root}/README.md"
  if PGQUE_DOCS_ROOT="${stable_root}" \
    bash build/check-docs-contract.sh >/dev/null 2>&1; then
    echo "FAIL: stable contract accepted a development path" >&2
    exit 1
  fi

  mkdir "${workdir}/bin"
  # shellcheck disable=SC2016 # Stub receives positional parameters later.
  printf '%s\n' \
    '#!/usr/bin/env bash' \
    'if [[ "${1:-}" == -RFn ]]; then exit 2; fi' \
    'exec /usr/bin/grep "$@"' > "${workdir}/bin/grep"
  chmod +x "${workdir}/bin/grep"
  if output=$(PATH="${workdir}/bin:${PATH}" \
    PGQUE_DOCS_ROOT="${development_root}" \
    bash build/check-docs-contract.sh 2>&1); then
    echo "FAIL: documentation scan error was treated as no match" >&2
    exit 1
  fi
  grep -Fq 'documentation scan failed' <<<"${output}"

  echo "PASS: development and stable documentation contracts fail closed"
}

main "$@"
