#!/usr/bin/env bash
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
set -Eeuo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "${repo_root}"

artifacts=(
  pgque.sql
  pgque-tle.sql
  pgque_uninstall.sql
  pgque-tle-uninstall.sql
)
lifecycle=devel/sql/pgque-additions/lifecycle.sql
devel_dir=devel/sql
stable_dir=sql
verifier=build/verify-release-artifacts.sh

usage() {
  echo "usage: promote-release.sh VERSION" >&2
  echo "       promote-release.sh --restore-devel VERSION" >&2
  exit 2
}

is_final_semver() {
  [[ "$1" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]
}

source_version() {
  bash build/read-version.sh "${lifecycle}"
}

stamp_source_version() {
  local version=$1
  local stamped=${workspace}/lifecycle.sql

  if ! awk -v new_version="${version}" '
    /create or replace function pgque\.version/ { in_fn=1 }
    in_fn && /^[[:space:]]*return '\''[^'\'']+'\'';[[:space:]]*$/ {
      sub(/return '\''[^'\'']+'\'';/, "return '\''" new_version "'\'';")
      changed++
      in_fn=0
    }
    { print }
    END { if (changed != 1) exit 1 }
  ' "${lifecycle}" > "${stamped}"; then
    echo "FAIL: could not stamp pgque.version() in ${lifecycle}" >&2
    return 1
  fi
  mv "${stamped}" "${lifecycle}"
}

ensure_clean_release_files() {
  local dirty
  if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    return
  fi

  dirty=$(git status --porcelain --untracked-files=all -- \
    "${lifecycle}" \
    "${devel_dir}/pgque.sql" \
    "${devel_dir}/pgque-tle.sql" \
    "${devel_dir}/pgque_uninstall.sql" \
    "${devel_dir}/pgque-tle-uninstall.sql" \
    "${stable_dir}")
  if [[ -n "${dirty}" ]]; then
    echo "FAIL: release artifacts already have uncommitted changes:" >&2
    echo "${dirty}" >&2
    return 1
  fi
}

regenerate_deterministically() {
  local first_build=${workspace}/first-build
  local artifact

  bash build/transform.sh
  mkdir "${first_build}"
  for artifact in "${artifacts[@]}"; do
    cp "${devel_dir}/${artifact}" "${first_build}/${artifact}"
  done

  bash build/transform.sh
  for artifact in "${artifacts[@]}"; do
    if ! cmp -s "${first_build}/${artifact}" "${devel_dir}/${artifact}"; then
      echo "FAIL: regeneration is not deterministic for ${artifact}" >&2
      return 1
    fi
  done
}

backup_release_files() {
  mkdir -p "${backup}/devel"
  cp "${lifecycle}" "${backup}/lifecycle.sql"
  for artifact in "${artifacts[@]}"; do
    cp "${devel_dir}/${artifact}" "${backup}/devel/${artifact}"
  done
  cp -R "${stable_dir}" "${backup}/sql"
  backup_ready=1
}

restore_backup() {
  local artifact
  cp "${backup}/lifecycle.sql" "${lifecycle}"
  for artifact in "${artifacts[@]}"; do
    cp "${backup}/devel/${artifact}" "${devel_dir}/${artifact}"
  done

  rm -rf "${stable_dir}"
  if [[ -d "${workspace}/previous-sql" ]]; then
    mv "${workspace}/previous-sql" "${stable_dir}"
  else
    cp -R "${backup}/sql" "${stable_dir}"
  fi
}

on_exit() {
  local status=$?
  trap - EXIT HUP INT TERM
  if [[ "${status}" -ne 0 && "${backup_ready}" -eq 1 ]]; then
    set +e
    restore_backup
    echo "Release promotion failed; restored the original release files." >&2
  fi
  rm -rf "${workspace}"
  exit "${status}"
}

mode=promote
if [[ "${1:-}" == --restore-devel ]]; then
  [[ "$#" -eq 2 ]] || usage
  mode=restore-devel
  version=$2
else
  [[ "$#" -eq 1 ]] || usage
  version=$1
fi

if ! is_final_semver "${version}"; then
  echo "FAIL: release version must be final SemVer (for example 0.3.0)" >&2
  exit 2
fi

if [[ ! -f "${lifecycle}" || ! -x "${verifier}" ]]; then
  echo "FAIL: run this command from a complete PgQue checkout" >&2
  exit 1
fi

ensure_clean_release_files
bash "${verifier}" "${stable_dir}"

current_version=$(source_version)
desired_version=${version}
if [[ "${mode}" == promote ]]; then
  if [[ "${current_version}" != "${version}-devel" \
    && "${current_version}" != "${version}" ]]; then
    echo "FAIL: ${lifecycle} reports ${current_version}; expected ${version}-devel" >&2
    exit 1
  fi
else
  stable_version=$(awk '$1 == "version" { print $2 }' \
    "${stable_dir}/release-manifest.txt")
  if [[ "${stable_version}" != "${version}" ]]; then
    echo "FAIL: stable release is ${stable_version}, not ${version}" >&2
    exit 1
  fi
  if [[ "${current_version}" != "${version}" \
    && "${current_version}" != "${version}-devel" ]]; then
    echo "FAIL: ${lifecycle} reports ${current_version}; expected ${version}" >&2
    exit 1
  fi
  desired_version=${version}-devel
fi

workspace=$(mktemp -d "${repo_root}/.release-promotion.XXXXXX")
backup=${workspace}/backup
backup_ready=0
trap on_exit EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
backup_release_files

stamp_source_version "${desired_version}"
regenerate_deterministically

if [[ "${mode}" == restore-devel ]]; then
  bash "${verifier}" "${stable_dir}"
  echo "Restored devel/sql to ${desired_version}; stable sql/ remains ${version}."
  exit 0
fi

stage=${workspace}/stage-sql
mkdir "${stage}"
for artifact in "${artifacts[@]}"; do
  cp "${devel_dir}/${artifact}" "${stage}/${artifact}"
done
bash "${verifier}" --record "${version}" "${stage}"

for artifact in "${artifacts[@]}"; do
  cmp "${devel_dir}/${artifact}" "${stage}/${artifact}"
done

# The complete directory is staged and verified before same-filesystem
# renames replace stable sql/. The EXIT trap restores the old directory if
# either rename or the post-promotion verification fails.
mv "${stable_dir}" "${workspace}/previous-sql"
mv "${stage}" "${stable_dir}"

bash "${verifier}" "${stable_dir}"
for artifact in "${artifacts[@]}"; do
  cmp "${devel_dir}/${artifact}" "${stable_dir}/${artifact}"
done

echo "Promoted PgQue ${version} to sql/ with all four stable artifacts."
