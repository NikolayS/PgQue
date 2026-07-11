#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# Verify or record the complete stable PgQue artifact manifest.

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
artifacts=(
  pgque.sql
  pgque-tle.sql
  pgque_uninstall.sql
  pgque-tle-uninstall.sql
)
manifest_name=release-manifest.txt

usage() {
  echo "usage: verify-release-artifacts.sh [DIR]" >&2
  echo "       verify-release-artifacts.sh --record VERSION DIR" >&2
  exit 2
}

is_final_semver() {
  [[ "${1}" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${1}" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "${1}" | awk '{print $1}'
  else
    echo "FAIL: sha256sum or shasum is required" >&2
    return 1
  fi
}

assert_directory_contents() {
  local include_manifest=$1
  local actual expected

  actual=$(find "${artifact_dir}" -mindepth 1 -maxdepth 1 \
    -exec basename {} \; | LC_ALL=C sort)
  expected=$(printf '%s\n' "${artifacts[@]}" | LC_ALL=C sort)
  if [[ "${include_manifest}" == 1 ]]; then
    expected=$(printf '%s\n%s\n' "${expected}" "${manifest_name}" \
      | LC_ALL=C sort)
  fi

  if [[ "${actual}" != "${expected}" ]]; then
    echo "FAIL: ${artifact_dir} must contain exactly the four stable" \
      "artifacts" >&2
    if [[ "${include_manifest}" == 1 ]]; then
      echo "and ${manifest_name}" >&2
    fi
    diff <(printf '%s\n' "${expected}") <(printf '%s\n' "${actual}") || true
    return 1
  fi
}

check_single_value() {
  local label=$1
  local expected=$2
  local values=$3
  local count actual

  count=$(printf '%s\n' "${values}" | awk 'NF { n++ } END { print n + 0 }')
  actual=$(printf '%s\n' "${values}" | awk 'NF { print; exit }')
  if [[ "${count}" -ne 1 || "${actual}" != "${expected}" ]]; then
    echo "FAIL: ${label} must be exactly ${expected};" \
      "found ${values:-<missing>}" >&2
    return 1
  fi
}

runtime_versions() {
  awk '
    /create or replace function pgque\.version/ { in_fn=1; next }
    in_fn && $0 !~ /^[[:space:]]*--/ && match($0, /return '\''[^'\'']+'\''/) {
      print substr($0, RSTART+8, RLENGTH-9)
      in_fn=0
    }
  ' "${1}"
}

call_target_versions() {
  local call=$1
  local file=$2
  awk -v needle="perform pgtle.${call}(" '
    index($0, needle) { in_call=1 }
    in_call {
      line=$0
      while (match(line, /\047[^\047]+\047/)) {
        quoted++
        value=substr(line, RSTART+1, RLENGTH-2)
        if (quoted == 2) {
          print value
          exit
        }
        line=substr(line, RSTART+RLENGTH)
      }
      if ($0 ~ /\);/) exit
    }
  ' "${file}"
}

verify_manifest() {
  local manifest=${artifact_dir}/${manifest_name}
  local artifact expected_hash actual_hash count
  local version header_versions tle_header_versions tle_echo_versions
  local embedded_body

  assert_directory_contents 1
  if ! awk '
    BEGIN {
      required["pgque.sql"]=1
      required["pgque-tle.sql"]=1
      required["pgque_uninstall.sql"]=1
      required["pgque-tle-uninstall.sql"]=1
    }
    /^#/ || NF == 0 { next }
    $1 == "version" && NF == 2 { versions++; next }
    $1 == "sha256" && NF == 3 && length($2) == 64 && $2 !~ /[^0-9a-f]/ {
      hashes++
      seen[$3]++
      next
    }
    { bad=1 }
    END {
      if (versions != 1 || hashes != 4 || bad) exit 1
      for (file in required) if (seen[file] != 1) exit 1
      for (file in seen) if (!required[file]) exit 1
    }
  ' "${manifest}"; then
    echo "FAIL: malformed ${manifest}" >&2
    return 1
  fi

  version=$(awk '$1 == "version" { print $2 }' "${manifest}")
  if ! is_final_semver "${version}"; then
    echo "FAIL: stable manifest version must be final SemVer," \
      "got ${version}" >&2
    return 1
  fi

  for artifact in "${artifacts[@]}"; do
    count=$(awk -v file="${artifact}" \
      '$1 == "sha256" && $3 == file { n++ } END { print n + 0 }' "${manifest}")
    if [[ "${count}" -ne 1 ]]; then
      echo "FAIL: ${manifest} must record ${artifact} exactly once" >&2
      return 1
    fi
    expected_hash=$(awk -v file="${artifact}" \
      '$1 == "sha256" && $3 == file { print $2 }' "${manifest}")
    actual_hash=$(sha256_file "${artifact_dir}/${artifact}")
    if [[ "${actual_hash}" != "${expected_hash}" ]]; then
      echo "FAIL: ${artifact} does not match ${manifest_name}" >&2
      return 1
    fi
  done

  header_versions=$(awk '$1 == "--" && $2 == "Version:" { print $3 }' \
    "${artifact_dir}/pgque.sql")
  check_single_value "pgque.sql header version" "${version}" \
    "${header_versions}"
  check_single_value "pgque.sql runtime version" "${version}" \
    "$(runtime_versions "${artifact_dir}/pgque.sql")"

  tle_header_versions=$(awk '$1 == "--" && $2 == "Version:" { print $3 }' \
    "${artifact_dir}/pgque-tle.sql")
  check_single_value "pgque-tle.sql embedded header version" "${version}" \
    "${tle_header_versions}"
  check_single_value "pgque-tle.sql runtime version" "${version}" \
    "$(runtime_versions "${artifact_dir}/pgque-tle.sql")"
  tle_echo_versions=$(sed -n \
    "s/^\\\\echo 'PgQue \([^']*\) registered.*$/\1/p" \
    "${artifact_dir}/pgque-tle.sql")
  check_single_value "pgque-tle.sql registration message version" "${version}" \
    "${tle_echo_versions}"

  check_single_value "pgque-tle.sql install target" "${version}" \
    "$(call_target_versions install_extension "${artifact_dir}/pgque-tle.sql")"
  local -a version_calls=(
    install_extension_version_sql
    install_update_path
    set_default_version
  )
  for call in "${version_calls[@]}"; do
    if grep -Fq "perform pgtle.${call}(" "${artifact_dir}/pgque-tle.sql"; then
      check_single_value "pgque-tle.sql ${call} target" "${version}" \
        "$(call_target_versions "${call}" "${artifact_dir}/pgque-tle.sql")"
    fi
  done

  embedded_body=$(mktemp)
  if ! awk '
    {
      marker_line=$0
      sub(/;[[:space:]]*$/, "", marker_line)
    }
    marker_line == "$pgque_extension_body$" {
      markers++
      if (markers == 1) { inside=1; next }
      if (markers == 2) { inside=0; next }
    }
    inside { print }
    END { if (markers != 2) exit 1 }
  ' "${artifact_dir}/pgque-tle.sql" > "${embedded_body}"; then
    rm -f "${embedded_body}"
    echo "FAIL: pgque-tle.sql extension body markers are malformed" >&2
    return 1
  fi
  if ! cmp -s "${artifact_dir}/pgque.sql" "${embedded_body}"; then
    rm -f "${embedded_body}"
    echo "FAIL: pgque-tle.sql does not embed the exact pgque.sql artifact" >&2
    return 1
  fi
  rm -f "${embedded_body}"

  echo "PASS: stable PgQue ${version} artifacts are coherent"
}

record_manifest() {
  local version=$1
  local manifest=${artifact_dir}/${manifest_name}
  local tmp artifact hash

  if ! is_final_semver "${version}"; then
    echo "FAIL: release version must be final SemVer, got ${version}" >&2
    return 1
  fi
  assert_directory_contents 0

  tmp=$(mktemp "${artifact_dir}/.release-manifest.XXXXXX")
  {
    echo "# Generated by build/promote-release.sh. Do not edit by hand."
    echo "version ${version}"
  } > "${tmp}"
  for artifact in "${artifacts[@]}"; do
    if ! hash=$(sha256_file "${artifact_dir}/${artifact}"); then
      rm -f "${tmp}"
      return 1
    fi
    echo "sha256 ${hash} ${artifact}" >> "${tmp}"
  done
  mv "${tmp}" "${manifest}"
  verify_manifest
}

main() {
  if [[ "${1:-}" == --record ]]; then
    [[ "$#" -eq 3 ]] || usage
    version=$2
    artifact_dir=$3
    [[ -d "${artifact_dir}" ]] || usage
    artifact_dir=$(cd "${artifact_dir}" && pwd)
    record_manifest "${version}"
  else
    [[ "$#" -le 1 ]] || usage
    artifact_dir=${1:-${repo_root}/sql}
    [[ -d "${artifact_dir}" ]] || usage
    artifact_dir=$(cd "${artifact_dir}" && pwd)
    verify_manifest
  fi
}

main "$@"
