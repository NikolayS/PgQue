#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
channel_file="${repo_root}/docs/.release-channel"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

require_text() {
  local pattern="$1"
  local path="$2"
  rg -q --fixed-strings -- "${pattern}" "${repo_root}/${path}" \
    || fail "${path} must contain: ${pattern}"
}

forbid_text() {
  local pattern="$1"
  shift
  if rg -n --fixed-strings -- "${pattern}" "$@"; then
    fail "forbidden documentation text found: ${pattern}"
  fi
}

[[ -f "${channel_file}" ]] || fail "missing docs/.release-channel"
channel="$(<"${channel_file}")"
doc_paths=(
  "${repo_root}/README.md"
  "${repo_root}/docs"
  "${repo_root}/web/src/pages/index.astro"
)

case "${channel}" in
  development)
    require_text "Development documentation:" "README.md"
    require_text "Build contract." "docs/README.md"
    require_text "This page follows the \`main\` branch's in-development build" "docs/installation.md"
    require_text "This is the public API reference for the in-development default install" "docs/reference.md"
    require_text "tutorial follows the \`main\` branch development build" "docs/tutorial.md"

    require_text "\\i devel/sql/pgque.sql" "README.md"
    require_text "\\i devel/sql/pgque.sql" "docs/installation.md"
    require_text "\\i devel/sql/pgque.sql" "docs/tutorial.md"
    require_text "\\\\i devel/sql/pgque.sql" "web/src/pages/index.astro"

    forbid_text "\\i sql/pgque.sql" "${doc_paths[@]}"
    forbid_text "-f sql/pgque.sql" "${doc_paths[@]}"

    bad_source_links="$(
      rg -o 'https://github\.com/NikolayS/pgque/blob/[^ )]+/(devel/)?sql/[^ )]+' \
        "${repo_root}/docs/reference.md" \
        | rg -v '/blob/main/devel/sql/' \
        || true
    )"
    [[ -z "${bad_source_links}" ]] \
      || fail "development SQL source links must target blob/main/devel/sql/: ${bad_source_links}"
    ;;
  stable:*)
    release_tag="${channel#stable:}"
    [[ "${release_tag}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]] \
      || fail "stable channel must name a release tag, for example stable:v1.2.3"

    forbid_text "devel/sql/" "${doc_paths[@]}"
    forbid_text "Development documentation:" "${doc_paths[@]}"
    forbid_text "Build contract." "${doc_paths[@]}"
    forbid_text "in-development default install" "${doc_paths[@]}"
    forbid_text "main branch development build" "${doc_paths[@]}"

    require_text "\\i sql/pgque.sql" "README.md"
    require_text "\\i sql/pgque.sql" "docs/installation.md"
    require_text "\\i sql/pgque.sql" "docs/tutorial.md"
    require_text "\\\\i sql/pgque.sql" "web/src/pages/index.astro"

    bad_source_links="$(
      rg -o 'https://github\.com/NikolayS/pgque/blob/[^ )]+/sql/[^ )]+' \
        "${repo_root}/docs/reference.md" \
        | rg -v "/blob/${release_tag}/sql/" \
        || true
    )"
    [[ -z "${bad_source_links}" ]] \
      || fail "stable SQL source links must target blob/${release_tag}/sql/: ${bad_source_links}"
    ;;
  *)
    fail "unknown docs release channel: ${channel}"
    ;;
esac

echo "PASS: ${channel} documentation contract"
