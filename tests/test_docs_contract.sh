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
  local banner
  local wrong_tag
  local failures=0

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

  # shellcheck disable=SC2016 # Use the exact Markdown development banners.
  for banner in \
    "This page follows the \`main\` branch's in-development build" \
    'tutorial follows the `main` branch development build'; do
    write_stable_fixture "${stable_root}"
    printf '%s\n' "${banner}" >> "${stable_root}/docs/installation.md"
    if PGQUE_DOCS_ROOT="${stable_root}" \
      bash build/check-docs-contract.sh >/dev/null 2>&1; then
      echo "FAIL: stable contract accepted banner: ${banner}" >&2
      failures=$((failures + 1))
    fi
  done

  for wrong_tag in v1x2.3 v1.2x3 v1x2x3; do
    write_stable_fixture "${stable_root}"
    printf '%s\n' \
      "https://github.com/NikolayS/pgque/blob/${wrong_tag}/sql/pgque.sql" \
      > "${stable_root}/docs/reference.md"
    if PGQUE_DOCS_ROOT="${stable_root}" \
      bash build/check-docs-contract.sh >/dev/null 2>&1; then
      echo "FAIL: stable contract accepted wrong source tag: ${wrong_tag}" >&2
      failures=$((failures + 1))
    fi
  done

  # A dotted prerelease tag must also pass when the link matches literally.
  write_stable_fixture "${stable_root}"
  printf '%s\n' 'stable:v1.2.3-rc.2' > "${stable_root}/docs/.release-channel"
  printf '%s\n' \
    'https://github.com/NikolayS/pgque/blob/v1.2.3-rc.2/sql/pgque.sql' \
    > "${stable_root}/docs/reference.md"
  PGQUE_DOCS_ROOT="${stable_root}" \
    bash build/check-docs-contract.sh >/dev/null

  # The keyed tutorial must limit its ordering promise to snapshot windows.
  if ! sed -n '/^## Step 10:/,$p' docs/tutorial.md \
    | grep -Fq 'event-ID order within each snapshot window'; then
    echo "FAIL: keyed tutorial omits snapshot-window ordering limit" >&2
    failures=$((failures + 1))
  fi
  if ! sed -n '/^## Step 10:/,$p' docs/tutorial.md \
    | grep -Fq 'lower event ID can appear in a later window'; then
    echo "FAIL: keyed tutorial omits late-commit ordering limit" >&2
    failures=$((failures + 1))
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

  [[ "${failures}" -eq 0 ]] || return 1

  echo "PASS: development and stable documentation contracts fail closed"
}

main "$@"
