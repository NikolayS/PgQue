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
tmp_files=()

cleanup_tmp() {
  local f
  for f in ${tmp_files[@]+"${tmp_files[@]}"}; do
    rm -f -- "${f}"
  done
}
trap cleanup_tmp EXIT

make_temp_file() {
  local f
  f=$(mktemp)
  tmp_files+=("${f}")
  printf '%s\n' "${f}"
}

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

list_relative_files() {
  local dir=$1
  (
    cd "${dir}"
    find . -type f | sed 's|^\./||' | LC_ALL=C sort
  )
}

assert_directory_contents() {
  local include_manifest=$1
  local actual expected extra_dirs non_regular

  # Reject symlinks, FIFOs, sockets, or other special entries.
  non_regular=$(
    cd "${artifact_dir}"
    find . -mindepth 1 ! -type f ! -type d | sed 's|^\./||'
  )
  if [[ -n "${non_regular}" ]]; then
    echo "FAIL: non-regular files in ${artifact_dir}: ${non_regular}" >&2
    return 1
  fi

  # Allow only regular files plus an optional pgque-tle-updates/ directory
  # (used by frozen 0.2.x snapshots). Reject any other unexpected directory.
  extra_dirs=$(
    cd "${artifact_dir}"
    find . -mindepth 1 -type d | sed 's|^\./||' | grep -vx 'pgque-tle-updates' || true
  )
  if [[ -n "${extra_dirs}" ]]; then
    echo "FAIL: unexpected subdirectories in ${artifact_dir}: ${extra_dirs}" >&2
    return 1
  fi

  actual=$(list_relative_files "${artifact_dir}")
  expected=$(printf '%s\n' "${artifacts[@]}")
  if [[ "${include_manifest}" == 1 ]]; then
    expected=$(printf '%s\n%s\n' "${expected}" "${manifest_name}")
    if [[ -f "${artifact_dir}/${manifest_name}" ]]; then
      local extra_manifest
      extra_manifest=$(awk '$1 == "sha256" && $3 ~ /^pgque-tle-updates\/[^\/]+\.sql$/ { print $3 }' \
        "${artifact_dir}/${manifest_name}")
      if [[ -n "${extra_manifest}" ]]; then
        expected=$(printf '%s\n%s\n' "${expected}" "${extra_manifest}")
      fi
    fi
  elif [[ -d "${artifact_dir}/pgque-tle-updates" ]]; then
    local extra_files
    extra_files=$(
      cd "${artifact_dir}"
      find pgque-tle-updates -maxdepth 1 -type f -name '*.sql'
    )
    if [[ -n "${extra_files}" ]]; then
      expected=$(printf '%s\n%s\n' "${expected}" "${extra_files}")
    fi
  fi
  expected=$(printf '%s\n' "${expected}" | awk 'NF' | LC_ALL=C sort)

  if [[ "${actual}" != "${expected}" ]]; then
    echo "FAIL: ${artifact_dir} contents do not match the expected stable" \
      "artifact set" >&2
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
  awk -v needle="perform pgtle.${call}(" -v call="${call}" '
    $0 ~ /^[[:space:]]*--/ { next }
    index($0, needle) { in_call=1; buf="" }
    in_call {
      buf = buf " " $0
      if ($0 ~ /\);/) {
        in_call = 0
        sub(/^.*perform pgtle\.[a-z_]+\(/, "", buf)
        sub(/\);.*$/, "", buf)
        n = split(buf, args, /,/)
        idx = (call == "install_update_path") ? 3 : 2
        if (idx <= n) {
          val = args[idx]
          gsub(/^[[:space:]]+|[[:space:]]+$/, "", val)
          if (val ~ /^\047[^\047]+\047$/) {
            print substr(val, 2, length(val) - 2)
          } else {
            print "<non-literal:" val ">"
          }
        }
      }
    }
  ' "${file}"
}

count_wrapper_calls() {
  local call=$1
  local file=$2
  awk -v needle="perform pgtle.${call}(" '
    $0 ~ /^[[:space:]]*--/ { next }
    index($0, needle) { n++ }
    END { print n + 0 }
  ' "${file}"
}

verify_manifest() {
  local manifest=${artifact_dir}/${manifest_name}
  local artifact expected_hash actual_hash count
  local version header_versions tle_header_versions tle_echo_versions
  local embedded_body wrapper_body update_targets update_call_count parsed_count target

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
      if (($3 in required) || $3 ~ /^pgque-tle-updates\/[^\/]+\.sql$/) {
        seen[$3]++
      } else {
        bad=1
      }
      next
    }
    { bad=1 }
    END {
      if (versions != 1 || bad) exit 1
      for (file in required) if (seen[file] != 1) exit 1
      for (file in seen) if (seen[file] != 1) exit 1
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

  local -a recorded_files=()
  while IFS= read -r artifact; do
    recorded_files+=("${artifact}")
  done < <(awk '$1 == "sha256" { print $3 }' "${manifest}")

  for artifact in "${recorded_files[@]}"; do
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

  embedded_body=$(make_temp_file)
  wrapper_body=$(make_temp_file)
  if ! awk -v body_out="${embedded_body}" -v wrap_out="${wrapper_body}" '
    /^[[:space:]]*extension_sql[[:space:]]+text[[:space:]]*:=[[:space:]]*\$pgque_extension_body\$[[:space:]]*$/ || /^[[:space:]]*\$pgque_extension_body\$;?[[:space:]]*$/ {
      markers++
      if (markers == 1) { inside=1; print > wrap_out; next }
      if (markers == 2) { inside=0; print > wrap_out; next }
      next
    }
    inside { print > body_out; next }
    { print > wrap_out }
    END { if (markers != 2) exit 1 }
  ' "${artifact_dir}/pgque-tle.sql"; then
    echo "FAIL: pgque-tle.sql extension body markers are malformed" >&2
    return 1
  fi

  tle_header_versions=$(awk '$1 == "--" && $2 == "Version:" { print $3 }' \
    "${embedded_body}")
  check_single_value "pgque-tle.sql embedded header version" "${version}" \
    "${tle_header_versions}"
  check_single_value "pgque-tle.sql runtime version" "${version}" \
    "$(runtime_versions "${embedded_body}")"
  if ! cmp -s "${artifact_dir}/pgque.sql" "${embedded_body}"; then
    echo "FAIL: pgque-tle.sql does not embed the exact pgque.sql artifact" >&2
    return 1
  fi

  tle_echo_versions=$(sed -n \
    "s/^\\\\echo 'PgQue \([^']*\) registered.*$/\1/p" \
    "${wrapper_body}")
  check_single_value "pgque-tle.sql registration message version" "${version}" \
    "${tle_echo_versions}"

  check_single_value "pgque-tle.sql install target" "${version}" \
    "$(call_target_versions install_extension "${wrapper_body}")"
  local -a single_calls=(
    install_extension_version_sql
    set_default_version
  )
  for call in "${single_calls[@]}"; do
    if [[ "$(count_wrapper_calls "${call}" "${wrapper_body}")" -gt 0 ]]; then
      check_single_value "pgque-tle.sql ${call} target" "${version}" \
        "$(call_target_versions "${call}" "${wrapper_body}")"
    fi
  done

  update_call_count=$(count_wrapper_calls install_update_path "${wrapper_body}")
  if [[ "${update_call_count}" -gt 0 ]]; then
    update_targets=$(call_target_versions install_update_path "${wrapper_body}")
    parsed_count=$(printf '%s\n' "${update_targets}" | awk 'NF { n++ } END { print n + 0 }')
    if [[ "${parsed_count}" -ne "${update_call_count}" ]]; then
      echo "FAIL: pgque-tle.sql install_update_path count mismatch" \
        "(calls=${update_call_count}, parsed=${parsed_count})" >&2
      return 1
    fi
    local has_current_target=0
    while IFS= read -r target; do
      [[ -n "${target}" ]] || continue
      if ! is_final_semver "${target}"; then
        echo "FAIL: pgque-tle.sql install_update_path target must be final SemVer," \
          "got ${target}" >&2
        return 1
      fi
      if [[ ! -d "${artifact_dir}/pgque-tle-updates" && "${target}" != "${version}" ]]; then
        echo "FAIL: pgque-tle.sql install_update_path target must be ${version}," \
          "got ${target}" >&2
        return 1
      fi
      if [[ "${target}" == "${version}" ]]; then
        has_current_target=1
      fi
    done <<< "${update_targets}"
    if [[ "${has_current_target}" -ne 1 ]]; then
      echo "FAIL: pgque-tle.sql install_update_path targets must include ${version};" \
        "found ${update_targets:-<missing>}" >&2
      return 1
    fi
  fi

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
  tmp_files+=("${tmp}")
  {
    echo "# Generated by build/promote-release.sh. Do not edit by hand."
    echo "version ${version}"
  } > "${tmp}"
  for artifact in "${artifacts[@]}"; do
    hash=$(sha256_file "${artifact_dir}/${artifact}")
    echo "sha256 ${hash} ${artifact}" >> "${tmp}"
  done
  if [[ -d "${artifact_dir}/pgque-tle-updates" ]]; then
    while IFS= read -r artifact; do
      [[ -n "${artifact}" ]] || continue
      hash=$(sha256_file "${artifact_dir}/${artifact}")
      echo "sha256 ${hash} ${artifact}" >> "${tmp}"
    done < <(
      cd "${artifact_dir}"
      find pgque-tle-updates -maxdepth 1 -type f -name '*.sql' | LC_ALL=C sort
    )
  fi
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
