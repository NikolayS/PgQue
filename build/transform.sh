#!/usr/bin/env bash
set -Eeuo pipefail

# transform.sh -- Mechanical transformation of PgQ sources into pgque
#
# Reads PgQ PL-only source files from vendor/pgq/ and applies:
#   1. Schema rename: pgq -> pgque
#   2. txid_* -> pg_* snapshot function renames
#   3. txid_snapshot type -> pg_snapshot
#   4. bigint -> xid8 for txid-related columns
#   5. Add SET search_path to all SECURITY DEFINER functions
#   6. Remove queue_per_tx_limit column and references
#   7. Remove set default_with_oids
#   8. Remove pgq_node/Londiste hooks from maint_operations
#
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# PgQue includes code derived from PgQ (ISC license, Marko Kreen).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENDOR_DIR="${REPO_ROOT}/vendor/pgq"
OUTPUT_DIR="${SCRIPT_DIR}/output"

# -- Validate prerequisites --------------------------------------------------

if [[ ! -d "${VENDOR_DIR}" ]]; then
  echo "ERROR: vendor/pgq/ not found. Run: git submodule update --init" >&2
  exit 1
fi

if [[ ! -f "${VENDOR_DIR}/structure/tables.sql" ]]; then
  echo "ERROR: vendor/pgq/structure/tables.sql not found. Submodule may be empty." >&2
  exit 1
fi

# -- Prepare output directory -------------------------------------------------

rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}/structure"
mkdir -p "${OUTPUT_DIR}/functions"
mkdir -p "${OUTPUT_DIR}/lowlevel_pl"

# -- Source file list (PL-only install order) ---------------------------------

SOURCE_FILES=(
  structure/tables.sql
  functions/pgq.upgrade_schema.sql
  functions/pgq.batch_event_sql.sql
  functions/pgq.batch_event_tables.sql
  functions/pgq.event_retry_raw.sql
  functions/pgq.find_tick_helper.sql
  functions/pgq.ticker.sql
  functions/pgq.maint_retry_events.sql
  functions/pgq.maint_rotate_tables.sql
  functions/pgq.maint_tables_to_vacuum.sql
  functions/pgq.maint_operations.sql
  functions/pgq.grant_perms.sql
  functions/pgq.tune_storage.sql
  functions/pgq.force_tick.sql
  functions/pgq.seq_funcs.sql
  functions/pgq.quote_fqname.sql
  functions/pgq.create_queue.sql
  functions/pgq.drop_queue.sql
  functions/pgq.set_queue_config.sql
  functions/pgq.insert_event.sql
  functions/pgq.current_event_table.sql
  functions/pgq.register_consumer.sql
  functions/pgq.unregister_consumer.sql
  functions/pgq.next_batch.sql
  functions/pgq.get_batch_events.sql
  functions/pgq.get_batch_cursor.sql
  functions/pgq.event_retry.sql
  functions/pgq.batch_retry.sql
  functions/pgq.finish_batch.sql
  functions/pgq.get_queue_info.sql
  functions/pgq.get_consumer_info.sql
  functions/pgq.version.sql
  functions/pgq.get_batch_info.sql
  lowlevel_pl/insert_event.sql
  lowlevel_pl/jsontriga.sql
  lowlevel_pl/logutriga.sql
  lowlevel_pl/sqltriga.sql
  structure/grants.sql
)

# -- Transformation functions -------------------------------------------------

apply_schema_rename() {
  # Rename pgq schema references to pgque.
  # Must be word-boundary-aware to avoid mangling _pgq_ev_ magic columns,
  # pgq_node, pgq_ext, or text inside larger identifiers like "pgqueue".
  #
  # Strategy: apply targeted replacements in order of specificity.
  local content="$1"

  # 1. Role names: pgq_reader -> pgque_reader, etc.
  content=$(echo "$content" | sed \
    -e "s/pgq_reader/pgque_reader/g" \
    -e "s/pgq_writer/pgque_writer/g" \
    -e "s/pgq_admin/pgque_admin/g")

  # 2. Schema-qualified references: pgq.something -> pgque.something
  #    Matches pgq. followed by a letter or underscore (not a digit boundary issue)
  content=$(echo "$content" | sed -E "s/pgq\\.([a-zA-Z_])/pgque.\\1/g")

  # 3. String literals referencing the schema name:
  #    'pgq' -> 'pgque' (used in information_schema queries, extname, etc.)
  content=$(echo "$content" | sed "s/'pgq'/'pgque'/g")

  # 4. Standalone pgq as schema name in CREATE/DROP SCHEMA, GRANT ON SCHEMA, etc.
  #    "schema pgq" -> "schema pgque"
  content=$(echo "$content" | sed -E "s/schema pgq([^a-zA-Z0-9_])/schema pgque\\1/g")
  content=$(echo "$content" | sed -E "s/schema pgq$/schema pgque/g")

  # 5. pgq prefix on function/table names within the schema (e.g., in comments
  #    that say "pgq.something" -- already handled by rule 2)

  echo "$content"
}

apply_txid_function_renames() {
  # Replace legacy txid_* functions with PG14+ equivalents.
  local content="$1"

  content=$(echo "$content" | sed \
    -e 's/txid_current_snapshot()/pg_current_snapshot()/g' \
    -e 's/txid_current()/pg_current_xact_id()/g' \
    -e 's/txid_snapshot_xmax(/pg_snapshot_xmax(/g' \
    -e 's/txid_snapshot_xmin(/pg_snapshot_xmin(/g' \
    -e 's/txid_snapshot_xip(/pg_snapshot_xip(/g' \
    -e 's/txid_visible_in_snapshot(/pg_visible_in_snapshot(/g')

  echo "$content"
}

apply_txid_snapshot_type_rename() {
  # Replace txid_snapshot type with pg_snapshot in column defs and signatures.
  local content="$1"
  content=$(echo "$content" | sed 's/txid_snapshot/pg_snapshot/g')
  echo "$content"
}

apply_bigint_to_xid8() {
  # Replace bigint -> xid8 ONLY for txid-related columns:
  #   queue_switch_step1, queue_switch_step2, ev_txid
  # These are the columns whose defaults reference txid_current()
  # (now pg_current_xact_id() after the function rename).
  local content="$1"

  # queue_switch_step1 and queue_switch_step2 column definitions
  content=$(echo "$content" | sed -E \
    's/(queue_switch_step1[[:space:]]+)bigint/\1xid8/g')
  content=$(echo "$content" | sed -E \
    's/(queue_switch_step2[[:space:]]+)bigint/\1xid8/g')

  # ev_txid column definition in event_template
  content=$(echo "$content" | sed -E \
    's/(ev_txid[[:space:]]+)bigint/\1xid8/g')

  echo "$content"
}

apply_search_path_to_security_definer() {
  # Add SET search_path = pgque, pg_catalog to SECURITY DEFINER functions
  # that don't already have it. Handles the pattern:
  #   $$ language plpgsql security definer;
  # and variations with trailing comments.
  local content="$1"

  # Match lines ending with "security definer;" (with optional comment)
  # and inject SET search_path before the semicolon.
  # The pattern handles:
  #   $$ language plpgsql security definer;
  #   $$ language plpgsql security definer; -- comment
  content=$(echo "$content" | sed -E \
    's/^(\$\$ language plpgsql) security definer;(.*)$/\1 security definer set search_path = pgque, pg_catalog;\2/I')

  echo "$content"
}

remove_queue_per_tx_limit() {
  # Remove queue_per_tx_limit column definition and references.
  # Also fix trailing commas left on the line before the removed reference.
  local content="$1"

  # Use awk to remove lines containing queue_per_tx_limit and fix the
  # trailing comma on the preceding line when the removed line was the
  # last item in a SELECT or column list.
  content=$(echo "$content" | awk '
    { lines[NR] = $0; count = NR }
    END {
      # Pass 1: find lines to remove and fix trailing commas
      for (i = 1; i <= count; i++) {
        if (lines[i] ~ /queue_per_tx_limit/ || lines[i] ~ /--.*queue_per_tx_limit.*Max number of events/) {
          skip[i] = 1
          # Only fix trailing comma on previous line if the removed line
          # was the last item in a list (does not itself end with a comma).
          # If the removed line ends with comma, items follow it and the
          # previous comma is still needed.
          if (lines[i] !~ /,[[:space:]]*$/) {
            for (j = i - 1; j >= 1; j--) {
              if (lines[j] !~ /^[[:space:]]*$/ && !(j in skip)) {
                gsub(/,[[:space:]]*$/, "", lines[j])
                break
              }
            }
          }
        }
      }
      # Pass 2: print non-skipped lines
      for (i = 1; i <= count; i++) {
        if (!(i in skip)) print lines[i]
      }
    }
  ')

  echo "$content"
}

remove_default_with_oids() {
  # Remove "set default_with_oids = 'off';" line.
  local content="$1"
  content=$(echo "$content" | sed "/set default_with_oids/d")
  echo "$content"
}

remove_pgq_node_londiste_hooks() {
  # Remove pgq_node and Londiste maintenance hooks from maint_operations.
  # This removes the entire block from the comment introducing it to the
  # end of the londiste.periodic_maintenance section.
  local content="$1"

  # Use awk to remove the pgq_node/londiste block.
  # The block starts at the comment "--" followed by "pgq_node & londiste"
  # and ends just before "return;" near the end of the function.
  content=$(echo "$content" | awk '
    skipping && /^[[:space:]]*return;/ {
      skipping = 0
      print
      next
    }
    skipping { next }
    /^[[:space:]]*--$/ { hold = $0; next }
    /pgq_node & londiste/ {
      if (hold != "") {
        # We found the start of the block, skip until "return;"
        skipping = 1
        hold = ""
        next
      }
    }
    {
      if (hold != "") {
        print hold
        hold = ""
      }
      print
    }
    END {
      if (hold != "") print hold
    }
  ')

  echo "$content"
}

# -- Main transformation pipeline --------------------------------------------

echo "=== PgQ -> PgQue transformation pipeline ==="
echo "Source: ${VENDOR_DIR}"
echo "Output: ${OUTPUT_DIR}"
echo ""

file_count=0
for src_file in "${SOURCE_FILES[@]}"; do
  src_path="${VENDOR_DIR}/${src_file}"

  if [[ ! -f "${src_path}" ]]; then
    echo "WARNING: source file not found: ${src_file}" >&2
    continue
  fi

  # Determine output filename (rename pgq. prefix in filenames)
  out_file=$(echo "${src_file}" | sed 's/pgq\./pgque./g')
  out_path="${OUTPUT_DIR}/${out_file}"

  # Read source
  content=$(cat "${src_path}")

  # Apply transformations in order
  content=$(apply_txid_function_renames "$content")
  content=$(apply_txid_snapshot_type_rename "$content")
  content=$(apply_bigint_to_xid8 "$content")
  content=$(apply_schema_rename "$content")
  content=$(apply_search_path_to_security_definer "$content")

  # File-specific transformations
  case "${src_file}" in
    structure/tables.sql)
      content=$(remove_queue_per_tx_limit "$content")
      content=$(remove_default_with_oids "$content")
      ;;
    lowlevel_pl/insert_event.sql)
      content=$(remove_queue_per_tx_limit "$content")
      ;;
    functions/pgq.maint_operations.sql)
      content=$(remove_pgq_node_londiste_hooks "$content")
      ;;
  esac

  echo "$content" > "${out_path}"
  file_count=$((file_count + 1))
done

echo "Transformed ${file_count} files."
echo ""

# -- Self-verification --------------------------------------------------------

echo "=== Self-verification ==="

errors=0

# Check for remaining pgq. schema references (excluding comments about PgQ project)
# We look for pgq. followed by a letter/underscore (schema-qualified name pattern)
remaining_pgq=$(grep -rn 'pgq\.[a-zA-Z_]' "${OUTPUT_DIR}" \
  | grep -v '^\s*--' \
  | grep -v '^[^:]*:[0-9]*:\s*--' \
  || true)

if [[ -n "${remaining_pgq}" ]]; then
  echo "FAIL: Found remaining pgq. schema references:"
  echo "${remaining_pgq}"
  errors=$((errors + 1))
else
  echo "PASS: No remaining pgq. schema references"
fi

# Check for remaining txid_ function/type references.
# We check for the specific legacy functions and types that must be renamed.
# Column names like ev_txid and derived index names (e.g. _txid_idx) are NOT
# legacy references -- they are valid identifiers referencing the ev_txid column.
remaining_txid=$(grep -rn -E 'txid_(current|snapshot|visible)' "${OUTPUT_DIR}" || true)

if [[ -n "${remaining_txid}" ]]; then
  echo "FAIL: Found remaining txid_ function/type references:"
  echo "${remaining_txid}"
  errors=$((errors + 1))
else
  echo "PASS: No remaining txid_ function/type references"
fi

# Verify all SECURITY DEFINER functions have SET search_path
missing_search_path=$(grep -rn -i 'security definer' "${OUTPUT_DIR}" \
  | grep -iv 'set search_path' || true)

if [[ -n "${missing_search_path}" ]]; then
  echo "FAIL: SECURITY DEFINER functions missing SET search_path:"
  echo "${missing_search_path}"
  errors=$((errors + 1))
else
  echo "PASS: All SECURITY DEFINER functions have SET search_path"
fi

# Verify queue_per_tx_limit is gone
remaining_per_tx=$(grep -rn 'queue_per_tx_limit' "${OUTPUT_DIR}" || true)

if [[ -n "${remaining_per_tx}" ]]; then
  echo "FAIL: Found remaining queue_per_tx_limit references:"
  echo "${remaining_per_tx}"
  errors=$((errors + 1))
else
  echo "PASS: No remaining queue_per_tx_limit references"
fi

# Verify default_with_oids is gone
remaining_oids=$(grep -rn 'default_with_oids' "${OUTPUT_DIR}" || true)

if [[ -n "${remaining_oids}" ]]; then
  echo "FAIL: Found remaining default_with_oids references:"
  echo "${remaining_oids}"
  errors=$((errors + 1))
else
  echo "PASS: No remaining default_with_oids references"
fi

# Verify pgq_node/londiste hooks are gone from maint_operations
remaining_hooks=$(grep -n 'pgq_node\|londiste' "${OUTPUT_DIR}/functions/pgque.maint_operations.sql" \
  | grep -v '^\s*--' \
  | grep -v '^[0-9]*:\s*--' \
  || true)

if [[ -n "${remaining_hooks}" ]]; then
  echo "FAIL: Found remaining pgq_node/londiste hooks in maint_operations:"
  echo "${remaining_hooks}"
  errors=$((errors + 1))
else
  echo "PASS: No pgq_node/londiste hooks in maint_operations"
fi

# Verify _pgq_ev_ magic column names are preserved (should NOT be renamed)
preserved_magic=$(grep -rn '_pgq_ev_' "${OUTPUT_DIR}" || true)

if [[ -z "${preserved_magic}" ]]; then
  echo "FAIL: _pgq_ev_ magic column names were incorrectly removed"
  errors=$((errors + 1))
else
  echo "PASS: _pgq_ev_ magic column names preserved ($(echo "${preserved_magic}" | wc -l) occurrences)"
fi

echo ""
if [[ ${errors} -eq 0 ]]; then
  echo "=== ALL CHECKS PASSED ==="
else
  echo "=== ${errors} CHECK(S) FAILED ==="
  exit 1
fi
