#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# Enforce development and stable documentation release-channel invariants.

default_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_root="${PGQUE_DOCS_ROOT:-${default_repo_root}}"
channel_file="${repo_root}/docs/.release-channel"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

require_text() {
  local pattern="${1}"
  local path="${2}"
  local grep_status=0

  grep -Fq -- "${pattern}" "${repo_root}/${path}" || grep_status=$?
  if [[ "${grep_status}" -eq 1 ]]; then
    fail "${path} must contain: ${pattern}"
  elif [[ "${grep_status}" -ne 0 ]]; then
    fail "documentation scan failed for ${path}"
  fi
}

forbid_text() {
  local pattern="${1}"
  local grep_status=0

  shift
  grep -RFn -- "${pattern}" "$@" || grep_status=$?
  if [[ "${grep_status}" -eq 0 ]]; then
    fail "forbidden documentation text found: ${pattern}"
  elif [[ "${grep_status}" -ne 1 ]]; then
    fail "documentation scan failed while checking: ${pattern}"
  fi
}

find_bad_source_links() {
  local source_pattern="${1}"
  local allowed_pattern="${2}"
  local matches
  local bad_links
  local grep_status=0
  local filter_status=0

  matches="$(grep -Eo "${source_pattern}" \
    "${repo_root}/docs/reference.md")" || grep_status=$?
  if [[ "${grep_status}" -gt 1 ]]; then
    fail "documentation scan failed for docs/reference.md"
  fi

  bad_links="$(printf '%s\n' "${matches}" \
    | grep -Ev "${allowed_pattern}")" || filter_status=$?
  if [[ "${filter_status}" -gt 1 ]]; then
    fail "documentation source-link filter failed"
  fi

  printf '%s' "${bad_links}"
}

main() {
  local channel
  local release_tag
  local bad_source_links
  local reference_banner
  local -a doc_paths=(
    "${repo_root}/README.md"
    "${repo_root}/docs"
    "${repo_root}/web/src/pages/index.astro"
  )

  [[ -f "${channel_file}" ]] || fail "missing docs/.release-channel"
  channel="$(<"${channel_file}")"
  printf -v reference_banner '%s' \
    "This is the public API reference for the in-development" \
    " default install"

  case "${channel}" in
  development)
    require_text "Development documentation:" "README.md"
    require_text "Build contract." "docs/README.md"
    require_text \
      "This page follows the \`main\` branch's in-development build" \
      "docs/installation.md"
    require_text "${reference_banner}" "docs/reference.md"
    require_text \
      "tutorial follows the \`main\` branch development build" \
      "docs/tutorial.md"

    require_text "\\i devel/sql/pgque.sql" "README.md"
    require_text "\\i devel/sql/pgque.sql" "docs/installation.md"
    require_text "\\i devel/sql/pgque.sql" "docs/tutorial.md"
    require_text "\\\\i devel/sql/pgque.sql" "web/src/pages/index.astro"

    forbid_text "\\i sql/pgque.sql" "${doc_paths[@]}"
    forbid_text "-f sql/pgque.sql" "${doc_paths[@]}"

    bad_source_links="$(find_bad_source_links \
      'https://github\.com/NikolayS/pgque/blob/[^ )]+/(devel/)?sql/[^ )]+' \
      '/blob/main/devel/sql/')"
    if [[ -n "${bad_source_links}" ]]; then
      fail "development SQL source links must target blob/main/devel/sql/:" \
        "${bad_source_links}"
    fi
    ;;
  stable:*)
    release_tag="${channel#stable:}"
    if ! [[ "${release_tag}" \
      =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
      fail "stable channel must name a release tag," \
        "for example stable:v1.2.3"
    fi

    forbid_text "devel/sql/" "${doc_paths[@]}"
    forbid_text "Development documentation:" "${doc_paths[@]}"
    forbid_text "Build contract." "${doc_paths[@]}"
    forbid_text "in-development default install" "${doc_paths[@]}"
    forbid_text "main branch development build" "${doc_paths[@]}"
    forbid_text "main development build" "${doc_paths[@]}"

    require_text "\\i sql/pgque.sql" "README.md"
    require_text "\\i sql/pgque.sql" "docs/installation.md"
    require_text "\\i sql/pgque.sql" "docs/tutorial.md"
    require_text "\\\\i sql/pgque.sql" "web/src/pages/index.astro"

    bad_source_links="$(find_bad_source_links \
      'https://github\.com/NikolayS/pgque/blob/[^ )]+/sql/[^ )]+' \
      "/blob/${release_tag}/sql/")"
    if [[ -n "${bad_source_links}" ]]; then
      fail "stable SQL source links must target blob/${release_tag}/sql/:" \
        "${bad_source_links}"
    fi
    ;;
  *)
    fail "unknown docs release channel: ${channel}"
    ;;
  esac

  echo "PASS: ${channel} documentation contract"
}

main "$@"
