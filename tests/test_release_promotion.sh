#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# Regression coverage for atomic stable release promotion and restoration.

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "${repo_root}"

artifacts=(
  pgque.sql
  pgque-tle.sql
  pgque_uninstall.sql
  pgque-tle-uninstall.sql
)
release_files=(
  devel/sql/pgque-additions/lifecycle.sql
  devel/sql/pgque.sql
  devel/sql/pgque-tle.sql
  devel/sql/pgque_uninstall.sql
  devel/sql/pgque-tle-uninstall.sql
  sql/pgque.sql
  sql/pgque-tle.sql
  sql/pgque_uninstall.sql
  sql/pgque-tle-uninstall.sql
  sql/release-manifest.txt
)
shell_scripts=(
  build/promote-release.sh
  build/read-version.sh
  build/verify-release-artifacts.sh
)
workdir=

fingerprint() {
  local file
  for file in "${release_files[@]}"; do
    if [[ -f "${file}" ]]; then
      shasum -a 256 "${file}"
    else
      echo "missing  ${file}"
    fi
  done
}

cleanup() {
  if [[ -n "${workdir}" ]]; then
    rm -rf -- "${workdir}"
  fi
}

main() {
  local before
  local source_version
  local stable_version
  local artifact
  local after

  for script in "${shell_scripts[@]}"; do
    if ! grep -Fqx "IFS=\$'\\n\\t'" "${script}"; then
      echo "FAIL: ${script} must set the safe shell IFS" >&2
      exit 1
    fi
  done

  before=$(fingerprint)
  workdir=$(mktemp -d)
  trap cleanup EXIT

  # The checked-in stable release, currently 0.2.0, must be self-consistent.
  bash build/verify-release-artifacts.sh sql

  # Main is either explicitly 0.3.0-devel or a complete release-prep stamp.
  # A lone final lifecycle stamp must not make the regular CI version override
  # accept a partial promotion.
  source_version=$(bash build/read-version.sh)
  stable_version=$(awk '$1 == "version" { print $2 }' sql/release-manifest.txt)
  if [[ "${source_version}" != 0.3.0-devel ]]; then
    if [[ "${source_version}" != "${stable_version}" ]]; then
      echo "FAIL: final lifecycle version does not match the stable" \
        "manifest" >&2
      exit 1
    fi
    for artifact in "${artifacts[@]}"; do
      if ! cmp -s "devel/sql/${artifact}" "sql/${artifact}"; then
        echo "FAIL: final lifecycle stamp has a partial ${artifact}" \
          "promotion" >&2
        exit 1
      fi
    done
  fi

  # Missing and stale files must both invalidate an otherwise valid release.
  cp -R sql "${workdir}/partial"
  rm "${workdir}/partial/pgque-tle-uninstall.sql"
  if bash build/verify-release-artifacts.sh \
    "${workdir}/partial" >/dev/null 2>&1; then
    echo "FAIL: partial stable artifact set passed verification" >&2
    exit 1
  fi

  cp -R sql "${workdir}/stale"
  printf '\n-- stale promotion fixture\n' >> "${workdir}/stale/pgque.sql"
  if bash build/verify-release-artifacts.sh \
    "${workdir}/stale" >/dev/null 2>&1; then
    echo "FAIL: stale stable artifact passed verification" >&2
    exit 1
  fi

  # Release promotion accepts only final SemVer, never prerelease/devel values.
  if bash build/promote-release.sh 0.3.0-rc.1 >/dev/null 2>&1; then
    echo "FAIL: promotion accepted a non-final version" >&2
    exit 1
  fi

  # Exercise a real 0.3 promotion in a repository copy. The caller's checkout
  # must remain byte-for-byte unchanged while the copy is stamped and rebuilt.
  mkdir "${workdir}/repo"
  rsync -a \
    --exclude .git \
    --exclude build/output \
    --exclude '.release-promotion.*' \
    ./ "${workdir}/repo/"

  (
    cd "${workdir}/repo"
    bash build/promote-release.sh 0.3.0
    bash build/verify-release-artifacts.sh sql

    grep -Fxq 'version 0.3.0' sql/release-manifest.txt
    grep -Fq -- '-- Version: 0.3.0' sql/pgque.sql
    grep -Fq -- '-- Version: 0.3.0' sql/pgque-tle.sql
    if grep -Fq -- '0.3.0-devel' sql/pgque.sql sql/pgque-tle.sql; then
      echo "FAIL: promoted stable artifacts retain a devel version" >&2
      exit 1
    fi

    for artifact in "${artifacts[@]}"; do
      cmp "devel/sql/${artifact}" "sql/${artifact}"
    done

    # The post-tag command restores only devel; stable stays at the release.
    bash build/promote-release.sh --restore-devel 0.3.0
    grep -Fq "return '0.3.0-devel';" devel/sql/pgque-additions/lifecycle.sql
    grep -Fq -- '-- Version: 0.3.0-devel' devel/sql/pgque.sql
    bash build/verify-release-artifacts.sh sql
    grep -Fxq 'version 0.3.0' sql/release-manifest.txt

    # A failure after stamping must roll the source/devel files back and leave
    # the complete stable release untouched.
    rm pgq/structure/tables.sql
    if bash build/promote-release.sh 0.3.0 >/dev/null 2>&1; then
      echo "FAIL: promotion unexpectedly succeeded without its build source" >&2
      exit 1
    fi
    grep -Fq "return '0.3.0-devel';" devel/sql/pgque-additions/lifecycle.sql
    grep -Fq -- '-- Version: 0.3.0-devel' devel/sql/pgque.sql
    bash build/verify-release-artifacts.sh sql
    grep -Fxq 'version 0.3.0' sql/release-manifest.txt
  )

  after=$(fingerprint)
  if [[ "${after}" != "${before}" ]]; then
    echo "FAIL: temporary release promotion mutated the checkout" >&2
    diff <(printf '%s\n' "${before}") <(printf '%s\n' "${after}") || true
    exit 1
  fi

  echo "PASS: stable release promotion is complete, coherent, and isolated"
}

main "$@"
