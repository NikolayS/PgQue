#!/usr/bin/env bash
set -Eeuo pipefail

# transform.sh -- Mechanical transformation of PgQ sources into pgque
#
# Reads PgQ PL-only source files from pgq/ and applies:
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
PGQ_DIR="${REPO_ROOT}/pgq"
OUTPUT_DIR="${SCRIPT_DIR}/output"

# -- Validate prerequisites --------------------------------------------------

if [[ ! -d "${PGQ_DIR}" ]]; then
  echo "ERROR: pgq/ not found. Run: git submodule update --init" >&2
  exit 1
fi

if [[ ! -f "${PGQ_DIR}/structure/tables.sql" ]]; then
  echo "ERROR: pgq/structure/tables.sql not found. Submodule may be empty." >&2
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

  # Rename txid_* functions to pg_* equivalents.
  # pg_snapshot_xmin/xmax return xid8 (not bigint like the old txid_ versions).
  # Wrap them with ::text::bigint to preserve PgQ's bigint arithmetic.
  content=$(echo "$content" | sed \
    -e 's/txid_current_snapshot()/pg_current_snapshot()/g' \
    -e 's/txid_current()/pg_current_xact_id()/g' \
    -e 's/txid_snapshot_xmax(\([^)]*\))/pg_snapshot_xmax(\1)::text::bigint/g' \
    -e 's/txid_snapshot_xmin(\([^)]*\))/pg_snapshot_xmin(\1)::text::bigint/g' \
    -e 's/txid_snapshot_xip(\([^)]*\))/pg_snapshot_xip(\1)/g' \
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
  # DISABLED: Keep columns as bigint (same as PgQ) instead of converting to xid8.
  #
  # Rationale: pg_current_xact_id() returns xid8, but xid8 lacks comparison
  # and arithmetic operators that PgQ's rotation/batching code relies on:
  #   - int8 <= xid8  (no operator — breaks maint_rotate_tables_step1)
  #   - xid8 - integer (no operator — breaks batch_event_sql)
  #   - xid8 -> int8   (no implicit cast — breaks variable assignment)
  #
  # Instead, we add ::text::bigint casts where pg_current_xact_id() is used
  # as a column default. The pg_snapshot_xmin/xmax functions return xid8 but
  # PL/pgSQL record fields auto-cast to the comparison type, and the batch
  # SQL is built as text with ::text coercions.
  #
  # This preserves ALL PgQ's arithmetic and comparison logic unchanged.
  local content="$1"

  # Keep ev_txid as xid8 (required for pg_visible_in_snapshot).
  content=$(echo "$content" | sed -E \
    's/(ev_txid[[:space:]]+)bigint([[:space:]]+not null default)/\1xid8\2/g')

  # Cast pg_current_xact_id() to ::text::bigint ONLY in switch_step columns
  # and PL/pgSQL code (comparisons, assignments). ev_txid default stays as
  # raw xid8 since the column is xid8.
  # Strategy: cast ALL calls, then UN-cast the ev_txid default.
  content=$(echo "$content" | sed -E \
    's/pg_current_xact_id\(\)([^:])/pg_current_xact_id()::text::bigint\1/g' \
    | sed -E 's/pg_current_xact_id\(\)$/pg_current_xact_id()::text::bigint/g')

  # Undo the cast for ev_txid default (needs raw xid8, not bigint):
  content=$(echo "$content" | sed -E \
    's/(ev_txid[[:space:]]+xid8[[:space:]]+not null default )pg_current_xact_id\(\)::text::bigint/\1pg_current_xact_id()/g')

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
    's/^(\$\$ language plpgsql) security definer;(.*)$/\1 security definer set search_path = pgque, pg_catalog;\2/')

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
echo "Source: ${PGQ_DIR}"
echo "Output: ${OUTPUT_DIR}"
echo ""

file_count=0
for src_file in "${SOURCE_FILES[@]}"; do
  src_path="${PGQ_DIR}/${src_file}"

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

  printf '%s\n' "$content" > "${out_path}"
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

# -- Post-transform patch: LISTEN/NOTIFY in ticker ----------------------------

echo ""
echo "=== Applying post-transform patches ==="

# Cross-platform sed -i (macOS BSD sed requires '', GNU sed does not)
sedi() {
  if [[ "$(uname -s)" == "Darwin" ]]; then
    sed -i '' "$@"  # BSD sed (macOS)
  else
    sed -i "$@"     # GNU sed (Linux)
  fi
}

TICKER_FILE="${OUTPUT_DIR}/functions/pgque.ticker.sql"

# Patch the ticker: inject pg_notify before return statements (awk for portability)
awk '
/^    return i_tick_id;$/ {
  print ""
  print "    -- pgque: notify listeners after tick"
  print "    perform pg_notify('"'"'pgque_'"'"' || i_queue_name, i_tick_id::text);"
}
/^    return currval\(q\.queue_tick_seq\);$/ {
  print ""
  print "    -- pgque: notify listeners after tick"
  print "    perform pg_notify('"'"'pgque_'"'"' || i_queue_name, currval(q.queue_tick_seq)::text);"
}
{ print }
' "${TICKER_FILE}" > "${TICKER_FILE}.tmp" && mv "${TICKER_FILE}.tmp" "${TICKER_FILE}"

echo "PASS: pg_notify injected into ticker function"

CREATE_QUEUE_FILE="${OUTPUT_DIR}/functions/pgque.create_queue.sql"
awk '
/^    if i_queue_name is null then$/ {
  print
  in_null_check = 1
  next
}
in_null_check && /^    end if;$/ {
  q = "\047"
  print
  print ""
  print "    -- pg_notify channel names are limited to 63 bytes by PostgreSQL."
  print "    -- PgQue prefixes them with " q "pgque_" q " (6 bytes), leaving 57 bytes for"
  print "    -- the queue name.  Reject names that would overflow the channel name"
  print "    -- before any state is written, so callers get a clear error."
  print "    if octet_length(i_queue_name) > 57 then"
  print "        raise exception " q "queue name too long: % bytes (max 57). " q
  print "            " q "pg_notify channel " q q "pgque_<queue_name>" q q " must fit in 63 bytes." q ","
  print "            octet_length(i_queue_name);"
  print "    end if;"
  in_null_check = 0
  next
}
in_null_check { print; next }
{ print }
' "${CREATE_QUEUE_FILE}" > "${CREATE_QUEUE_FILE}.tmp" && mv "${CREATE_QUEUE_FILE}.tmp" "${CREATE_QUEUE_FILE}"

echo "PASS: create_queue queue name length check added"

# Fix inherited PgQ copy-paste bug: sqltriga comment says logutriga
sedi 's/Function: pgque.logutriga()/Function: pgque.sqltriga()/' "${OUTPUT_DIR}/lowlevel_pl/sqltriga.sql"

echo "PASS: sqltriga comment header fixed"

# Remove debug comments from ticker
sedi 's/ -- unsure about access//' "${OUTPUT_DIR}/functions/pgque.ticker.sql"

echo "PASS: debug comments removed from ticker"

# Fix ev_txid (xid8) comparisons in batch_event_sql dynamic SQL.
# ev_txid is xid8, but the dynamic SQL builds "ev.ev_txid >= 855" where 855
# is a plain integer. PostgreSQL has no xid8 >= integer operator.
# Fix: wrap values in '...'::xid8 casts in the generated SQL.
BATCH_SQL_FILE="${OUTPUT_DIR}/functions/pgque.batch_event_sql.sql"
sedi "s/|| ' and ev.ev_txid >= ' || batch.tx_start::text/|| ' and ev.ev_txid >= ''' || batch.tx_start::text || '''::xid8'/" \
  "${BATCH_SQL_FILE}"
sedi "s/|| ' and ev.ev_txid <= ' || batch.tx_end::text/|| ' and ev.ev_txid <= ''' || batch.tx_end::text || '''::xid8'/" \
  "${BATCH_SQL_FILE}"
# Also fix the IN() list for older tx-es
sedi "s/arr := rec.id1::text;/arr := '''' || rec.id1::text || '''::xid8';/" \
  "${BATCH_SQL_FILE}"
sedi "s/arr := arr || ',' || rec.id1::text;/arr := arr || ',''' || rec.id1::text || '''::xid8';/" \
  "${BATCH_SQL_FILE}"

# Fix the arithmetic compare: bigint - 100 <= xid8 has no operator in PG18.
# rec.id1 comes from pg_snapshot_xip() (SETOF xid8); cast through text to bigint.
sedi "s|if batch.tx_start - 100 <= rec.id1 then|if batch.tx_start - 100 <= rec.id1::text::bigint then|" \
  "${BATCH_SQL_FILE}"
sedi "s|batch.tx_start := rec.id1;|batch.tx_start := rec.id1::text::bigint;|" \
  "${BATCH_SQL_FILE}"

echo "PASS: xid8 casts added to batch_event_sql for ev_txid comparisons"

# Fix upstream upgrade_schema(): use explicit grants instead of "in role" (the
# original "create role pgque_admin in role pgque_reader, pgque_writer" works
# but is harder to read and the role-creation case in PR review surfaced it as
# inverted-looking). Also return the actual mutation count instead of a
# hard-coded 0.
UPGRADE_SCHEMA_FILE="${OUTPUT_DIR}/functions/pgque.upgrade_schema.sql"
awk '
/create role pgque_admin in role pgque_reader, pgque_writer;/ {
  sub(/create role pgque_admin in role pgque_reader, pgque_writer;/, "create role pgque_admin;\n        grant pgque_reader to pgque_admin;\n        grant pgque_writer to pgque_admin;")
}
{ print }
' "${UPGRADE_SCHEMA_FILE}" > "${UPGRADE_SCHEMA_FILE}.tmp" && mv "${UPGRADE_SCHEMA_FILE}.tmp" "${UPGRADE_SCHEMA_FILE}"
sedi 's|^    return 0;$|    return cnt;|' "${UPGRADE_SCHEMA_FILE}"

echo "PASS: upgrade_schema role grants flattened, return cnt"

# Fix upstream insert_event_raw(): raise a clear "queue not found" error when
# the queue lookup misses, instead of falling through to a NULL-deref later.
INSERT_EVENT_FILE="${OUTPUT_DIR}/lowlevel_pl/insert_event.sql"
awk '
/from pgque\.queue q where q\.queue_name = _qname into qstate;/ {
  print
  print ""
  print "    if not found then"
  print "        raise exception '\''queue not found: %'\'', _qname;"
  print "    end if;"
  next
}
{ print }
' "${INSERT_EVENT_FILE}" > "${INSERT_EVENT_FILE}.tmp" && mv "${INSERT_EVENT_FILE}.tmp" "${INSERT_EVENT_FILE}"

echo "PASS: insert_event_raw raises clear error on unknown queue"

BATCH_RETRY_FILE="${OUTPUT_DIR}/functions/pgque.batch_retry.sql"
sedi 's/b\.ev_id, b\.ev_time, NULL::int8, _s\.sub_id,/b.ev_id, b.ev_time, NULL::xid8, _s.sub_id,/' \
  "${BATCH_RETRY_FILE}"

echo "PASS: batch_retry NULL::int8 -> NULL::xid8 for ev_txid (xid8) column"

# Inject SECURITY header on get_batch_cursor(4-arg). The upstream PgQ source
# concatenates i_extra_where into dynamic SQL verbatim, so the parameter is a
# trusted-SQL fragment rather than a value. Both overloads are admin-only via
# the grants block, but the contract must travel with the function body so
# anyone reading sql/pgque.sql understands why.
GET_BATCH_CURSOR_FILE="${OUTPUT_DIR}/functions/pgque.get_batch_cursor.sql"
awk '
BEGIN { header_done = 0; param_done = 0 }
!header_done && /^create or replace function pgque\.get_batch_cursor\(/ {
  print "-- ----------------------------------------------------------------------"
  print "-- Advanced PgQ-compatible primitive. Application roles should use"
  print "-- pgque.receive(); get_batch_cursor is kept admin-only in the grants block."
  print "--"
  print "-- SECURITY: i_extra_where is concatenated into dynamic SQL verbatim. It is a"
  print "-- trusted-SQL fragment, NOT a parameter. A caller can inject arbitrary"
  print "-- predicates (including UNION ALL) and forge rows in the returned stream."
  print "-- This behavior is inherited from upstream PgQ; it is acceptable here only"
  print "-- because both overloads are revoked from public, pgque_reader, and"
  print "-- pgque_writer and granted to pgque_admin only. NEVER pass user-controlled"
  print "-- input as i_extra_where, even from admin code paths."
  print "-- ----------------------------------------------------------------------"
  header_done = 1
}
!param_done && /^--      i_extra_where   - optional where clause to filter events$/ {
  print "--      i_extra_where   - optional where clause to filter events."
  print "--                        Trusted SQL fragment, not a parameter; never pass"
  print "--                        user-controlled text. Function is admin-only."
  param_done = 1
  next
}
{ print }
END {
  if (!header_done) { print "ERROR: get_batch_cursor 4-arg signature not found" > "/dev/stderr"; exit 1 }
  if (!param_done)  { print "ERROR: i_extra_where parameter doc line not found"   > "/dev/stderr"; exit 1 }
}
' "${GET_BATCH_CURSOR_FILE}" > "${GET_BATCH_CURSOR_FILE}.tmp" && mv "${GET_BATCH_CURSOR_FILE}.tmp" "${GET_BATCH_CURSOR_FILE}"

echo "PASS: get_batch_cursor SECURITY header injected (extra_where is trusted SQL)"

# -- Assembly: build sql/pgque.sql ------------------------------------

echo ""
echo "=== Assembling sql/pgque.sql ==="

SQL_DIR="${REPO_ROOT}/sql"
INSTALL_FILE="${SQL_DIR}/pgque.sql"
ADDITIONS_DIR="${SQL_DIR}/pgque-additions"

apply_idempotency_guards() {
  # Make DDL statements idempotent:
  # - CREATE TABLE -> CREATE TABLE IF NOT EXISTS
  # - CREATE SEQUENCE -> CREATE SEQUENCE IF NOT EXISTS
  # - CREATE INDEX -> CREATE INDEX IF NOT EXISTS
  # Functions already use CREATE OR REPLACE FUNCTION (verified above).
  # No CREATE TYPE statements exist in the transformed output.
  local content="$1"

  # CREATE TABLE (but not "CREATE TABLE IF NOT EXISTS" which already exists)
  content=$(echo "$content" | sed -E \
    's/^([[:space:]]*)create table ([^(])/\1create table if not exists \2/' \
    | sed -E 's/if not exists if not exists/if not exists/g')

  # CREATE SEQUENCE
  content=$(echo "$content" | sed -E \
    's/^([[:space:]]*)create sequence /\1create sequence if not exists /' \
    | sed -E 's/if not exists if not exists/if not exists/g')

  # CREATE INDEX
  content=$(echo "$content" | sed -E \
    's/^([[:space:]]*)create index /\1create index if not exists /' \
    | sed -E 's/if not exists if not exists/if not exists/g')

  echo "$content"
}

# Start with header
cat > "${INSTALL_FILE}" << 'HEADER'
-- pgque.sql -- PgQ Universal Edition
-- Version: 0.2.0-rc.1
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- Install: \i pgque.sql
-- Start:   SELECT pgque.start();
-- Usage:   See https://github.com/NikolayS/pgque

HEADER

# Schema creation
echo "create schema if not exists pgque;" >> "${INSTALL_FILE}"
echo "" >> "${INSTALL_FILE}"

# Section 1: Tables
echo "-- ======================================================================" >> "${INSTALL_FILE}"
echo "-- Section 1: Tables (derived from PgQ)" >> "${INSTALL_FILE}"
echo "-- Origin: pgq/structure/tables.sql" >> "${INSTALL_FILE}"
echo "--" >> "${INSTALL_FILE}"
echo "-- PgQue transformations applied:" >> "${INSTALL_FILE}"
echo "--   1. Schema rename: pgq → pgque (all identifiers, grants, references)" >> "${INSTALL_FILE}"
echo "--   2. txid_current() → pg_current_xact_id()::text::bigint (PG14+ API)" >> "${INSTALL_FILE}"
echo "--   3. txid_snapshot → pg_snapshot (type rename)" >> "${INSTALL_FILE}"
echo "--   4. ev_txid kept as xid8 (required by pg_visible_in_snapshot)" >> "${INSTALL_FILE}"
echo "--   5. queue_per_tx_limit column removed (not supported without C)" >> "${INSTALL_FILE}"
echo "--   6. set default_with_oids removed (deprecated since PG 12)" >> "${INSTALL_FILE}"
echo "--   7. CREATE TABLE → CREATE TABLE IF NOT EXISTS (idempotent install)" >> "${INSTALL_FILE}"
echo "-- ======================================================================" >> "${INSTALL_FILE}"
echo "" >> "${INSTALL_FILE}"

tables_content=$(cat "${OUTPUT_DIR}/structure/tables.sql")
# Remove the original "create schema pgque;" line (we already have IF NOT EXISTS above)
tables_content=$(echo "$tables_content" | sed '/^create schema pgque;$/d')
# Remove "set client_min_messages" — install script should not change session settings
tables_content=$(echo "$tables_content" | sed "/^set client_min_messages/d")
# Remove commented-out "drop schema" line
tables_content=$(echo "$tables_content" | sed '/^-- drop schema if exists/d')
tables_content=$(apply_idempotency_guards "$tables_content")
echo "$tables_content" >> "${INSTALL_FILE}"
echo "" >> "${INSTALL_FILE}"

# Section 2: Internal functions (in correct order from SOURCE_FILES)
echo "-- ======================================================================" >> "${INSTALL_FILE}"
echo "-- Section 2: Internal functions (derived from PgQ)" >> "${INSTALL_FILE}"
echo "-- Origin: pgq/functions/*.sql" >> "${INSTALL_FILE}"
echo "--" >> "${INSTALL_FILE}"
echo "-- PgQue transformations applied:" >> "${INSTALL_FILE}"
echo "--   1. Schema rename: pgq → pgque" >> "${INSTALL_FILE}"
echo "--   2. txid_* → pg_* function renames (PG14+ snapshot API)" >> "${INSTALL_FILE}"
echo "--   3. pg_snapshot_xmin/xmax wrapped with ::text::bigint (xid8→bigint)" >> "${INSTALL_FILE}"
echo "--   4. pg_current_xact_id() cast to ::text::bigint (xid8→bigint)" >> "${INSTALL_FILE}"
echo "--   5. SECURITY DEFINER functions get SET search_path = pgque, pg_catalog" >> "${INSTALL_FILE}"
echo "--   6. pgq_node/Londiste hooks removed from maint_operations" >> "${INSTALL_FILE}"
echo "--   7. pg_notify() injected into ticker for LISTEN/NOTIFY wakeup" >> "${INSTALL_FILE}"
echo "--   8. create_queue() rejects queue names > 57 bytes (pg_notify limit)" >> "${INSTALL_FILE}"
echo "-- ======================================================================" >> "${INSTALL_FILE}"
echo "" >> "${INSTALL_FILE}"

FUNCTION_FILES=(
  pgque.upgrade_schema.sql
  pgque.batch_event_sql.sql
  pgque.batch_event_tables.sql
  pgque.event_retry_raw.sql
  pgque.find_tick_helper.sql
  pgque.ticker.sql
  pgque.maint_retry_events.sql
  pgque.maint_rotate_tables.sql
  pgque.maint_tables_to_vacuum.sql
  pgque.maint_operations.sql
  pgque.grant_perms.sql
  pgque.tune_storage.sql
  pgque.force_tick.sql
  pgque.seq_funcs.sql
  pgque.quote_fqname.sql
  pgque.create_queue.sql
  pgque.drop_queue.sql
  pgque.set_queue_config.sql
  pgque.insert_event.sql
  pgque.current_event_table.sql
  pgque.register_consumer.sql
  pgque.unregister_consumer.sql
  pgque.next_batch.sql
  pgque.get_batch_events.sql
  pgque.get_batch_cursor.sql
  pgque.event_retry.sql
  pgque.batch_retry.sql
  pgque.finish_batch.sql
  pgque.get_queue_info.sql
  pgque.get_consumer_info.sql
  pgque.get_batch_info.sql
)

for func_file in "${FUNCTION_FILES[@]}"; do
  func_path="${OUTPUT_DIR}/functions/${func_file}"
  if [[ -f "${func_path}" ]]; then
    cat "${func_path}" >> "${INSTALL_FILE}"
    echo "" >> "${INSTALL_FILE}"
  else
    echo "WARNING: function file not found: ${func_file}" >&2
  fi
done

# Section 3: PL/pgSQL event insertion
echo "-- ======================================================================" >> "${INSTALL_FILE}"
echo "-- Section 3: PL/pgSQL event insertion (derived from PgQ)" >> "${INSTALL_FILE}"
echo "-- Origin: pgq/lowlevel_pl/insert_event.sql" >> "${INSTALL_FILE}"
echo "-- PgQue transformations: schema rename, txid→pg_* renames, search_path" >> "${INSTALL_FILE}"
echo "-- ======================================================================" >> "${INSTALL_FILE}"
echo "" >> "${INSTALL_FILE}"

cat "${OUTPUT_DIR}/lowlevel_pl/insert_event.sql" >> "${INSTALL_FILE}"
echo "" >> "${INSTALL_FILE}"

# Section 4: Trigger functions
echo "-- ======================================================================" >> "${INSTALL_FILE}"
echo "-- Section 4: Trigger functions (derived from PgQ)" >> "${INSTALL_FILE}"
echo "-- Origin: pgq/lowlevel_pl/{jsontriga,logutriga,sqltriga}.sql" >> "${INSTALL_FILE}"
echo "-- PgQue transformations: schema rename, search_path hardening" >> "${INSTALL_FILE}"
echo "-- ======================================================================" >> "${INSTALL_FILE}"
echo "" >> "${INSTALL_FILE}"

for triga_file in jsontriga.sql logutriga.sql sqltriga.sql; do
  cat "${OUTPUT_DIR}/lowlevel_pl/${triga_file}" >> "${INSTALL_FILE}"
  echo "" >> "${INSTALL_FILE}"
done

# Section 5: Default grants (from PgQ)
echo "-- ======================================================================" >> "${INSTALL_FILE}"
echo "-- Section 5: Default grants (derived from PgQ)" >> "${INSTALL_FILE}"
echo "-- Origin: pgq/structure/grants.sql" >> "${INSTALL_FILE}"
echo "-- PgQue transformations: pgq_reader/writer/admin → pgque_reader/writer/admin" >> "${INSTALL_FILE}"
echo "-- ======================================================================" >> "${INSTALL_FILE}"
echo "" >> "${INSTALL_FILE}"

cat "${OUTPUT_DIR}/structure/grants.sql" >> "${INSTALL_FILE}"
echo "" >> "${INSTALL_FILE}"

# Section 6: pgque additions
echo "-- ======================================================================" >> "${INSTALL_FILE}"
echo "-- Section 6: pgque additions (NEW — not derived from PgQ)" >> "${INSTALL_FILE}"
echo "-- ======================================================================" >> "${INSTALL_FILE}"
echo "" >> "${INSTALL_FILE}"

for addition_file in config.sql queue_max_retries.sql lifecycle.sql tick_helpers.sql roles.sql dlq.sql hardening.sql; do
  echo "-- pgque-additions/${addition_file}" >> "${INSTALL_FILE}"
  cat "${ADDITIONS_DIR}/${addition_file}" >> "${INSTALL_FILE}"
  echo "" >> "${INSTALL_FILE}"
done

# Section 7: pgque-api (default v0.1 API surface)
API_DIR="${SQL_DIR}/pgque-api"
if [[ -d "${API_DIR}" ]]; then
  echo "-- ======================================================================" >> "${INSTALL_FILE}"
  echo "-- Section 7: pgque-api (NEW — not derived from PgQ)" >> "${INSTALL_FILE}"
  echo "-- ======================================================================" >> "${INSTALL_FILE}"
  echo "" >> "${INSTALL_FILE}"

  DEFAULT_API_FILES=(
    maint.sql
    receive.sql
    cooperative_consumers.sql
    send.sql
  )

  for api_name in "${DEFAULT_API_FILES[@]}"; do
    api_file="${API_DIR}/${api_name}"
    if [[ -f "${api_file}" ]]; then
      echo "-- pgque-api/$(basename "${api_file}")" >> "${INSTALL_FILE}"
      cat "${api_file}" >> "${INSTALL_FILE}"
      echo "" >> "${INSTALL_FILE}"
    fi
  done
fi

# -- Inline transformation comments -------------------------------------------
# Add "PgQue transformation:" comments to specific lines in the output.
# These annotate each mechanical change so reviewers can trace what was modified.

# Column default transformations (appear once each in tables section)
sedi '/queue_switch_step1.*default pg_current_xact_id/s/$/ -- PgQue transformation: txid_current()→pg_current_xact_id()::text::bigint (PG14+)/' "${INSTALL_FILE}"

sedi '/ev_txid.*xid8.*default pg_current_xact_id/s/$/ -- PgQue transformation: bigint→xid8 (needed for pg_visible_in_snapshot)/' "${INSTALL_FILE}"

# LISTEN/NOTIFY injection (appears twice in ticker)
sedi '/perform pg_notify.*pgque_/s/$/ -- PgQue transformation: LISTEN\/NOTIFY wakeup (not in original PgQ)/' "${INSTALL_FILE}"

# search_path pinning — annotate first occurrence only via awk
awk '/set search_path = pgque, pg_catalog;/ && !sp_done {
  sp_done=1; sub(/;/, "; -- PgQue transformation: pin search_path (SECURITY DEFINER hardening)")
} {print}' "${INSTALL_FILE}" > "${INSTALL_FILE}.tmp" && mv "${INSTALL_FILE}.tmp" "${INSTALL_FILE}"

install_lines=$(wc -l < "${INSTALL_FILE}")
echo "Assembled ${INSTALL_FILE} (${install_lines} lines)"

# -- Assembly verification ----------------------------------------------------

echo ""
echo "=== Assembly verification ==="

asm_errors=0

# Verify header is present
if head -1 "${INSTALL_FILE}" | grep -q 'pgque.sql'; then
  echo "PASS: Install script header present"
else
  echo "FAIL: Install script header missing"
  asm_errors=$((asm_errors + 1))
fi

# Verify schema creation with IF NOT EXISTS
if grep -q 'create schema if not exists pgque;' "${INSTALL_FILE}"; then
  echo "PASS: Schema creation is idempotent"
else
  echo "FAIL: Missing idempotent schema creation"
  asm_errors=$((asm_errors + 1))
fi

# Verify no bare "create table" without "if not exists"
bare_create_table=$(grep -n 'create table ' "${INSTALL_FILE}" \
  | grep -iv 'if not exists' \
  | grep -v '^[0-9]*:[[:space:]]*--' \
  || true)
if [[ -z "${bare_create_table}" ]]; then
  echo "PASS: All CREATE TABLE statements are idempotent"
else
  echo "FAIL: Found bare CREATE TABLE without IF NOT EXISTS:"
  echo "${bare_create_table}"
  asm_errors=$((asm_errors + 1))
fi

# Verify no bare "create sequence" without "if not exists"
bare_create_seq=$(grep -n 'create sequence ' "${INSTALL_FILE}" \
  | grep -iv 'if not exists' \
  | grep -v '^[0-9]*:[[:space:]]*--' \
  || true)
if [[ -z "${bare_create_seq}" ]]; then
  echo "PASS: All CREATE SEQUENCE statements are idempotent"
else
  echo "FAIL: Found bare CREATE SEQUENCE without IF NOT EXISTS:"
  echo "${bare_create_seq}"
  asm_errors=$((asm_errors + 1))
fi

# Verify no bare "create index" without "if not exists"
# Exclude dynamic SQL (execute '...') inside function bodies
bare_create_idx=$(grep -n 'create index ' "${INSTALL_FILE}" \
  | grep -iv 'if not exists' \
  | grep -v '^[0-9]*:[[:space:]]*--' \
  | grep -v 'execute' \
  || true)
if [[ -z "${bare_create_idx}" ]]; then
  echo "PASS: All CREATE INDEX statements are idempotent"
else
  echo "FAIL: Found bare CREATE INDEX without IF NOT EXISTS:"
  echo "${bare_create_idx}"
  asm_errors=$((asm_errors + 1))
fi

# Verify pg_notify is in the ticker section
if grep -q 'pg_notify' "${INSTALL_FILE}"; then
  echo "PASS: pg_notify present in install script"
else
  echo "FAIL: pg_notify missing from install script"
  asm_errors=$((asm_errors + 1))
fi

# Verify pgque additions and API layer are in the script
if grep -q 'lifecycle.sql\|pgque.version\|pgque.start\|pgque.stop' "${INSTALL_FILE}"; then
  echo "PASS: pgque additions present in install script"
else
  echo "FAIL: pgque additions not found in install script"
  asm_errors=$((asm_errors + 1))
fi

# Verify additions order: roles must exist before add-on files with explicit
# role grants. This catches assembly regressions before install-time failures
# like "ERROR: role \"pgque_reader\" does not exist".
roles_line=$(grep -n -- '-- pgque-additions/roles.sql' "${INSTALL_FILE}" | head -1 | cut -d: -f1 || true)
dlq_line=$(grep -n -- '-- pgque-additions/dlq.sql' "${INSTALL_FILE}" | head -1 | cut -d: -f1 || true)
if [[ -n "${roles_line}" && -n "${dlq_line}" && ${roles_line} -lt ${dlq_line} ]]; then
  echo "PASS: pgque additions order keeps roles.sql before dlq.sql"
else
  echo "FAIL: pgque additions order must place roles.sql before dlq.sql"
  echo "roles.sql line: ${roles_line:-missing}; dlq.sql line: ${dlq_line:-missing}"
  asm_errors=$((asm_errors + 1))
fi

# Verify default pgque-api functions are in the script
if grep -q 'pgque.receive\|pgque.ack\|pgque.send\|pgque.subscribe\|pgque.maint' "${INSTALL_FILE}"; then
  echo "PASS: default pgque-api surface present in install script"
else
  echo "FAIL: default pgque-api surface not found in install script"
  asm_errors=$((asm_errors + 1))
fi

# Verify experimental APIs are NOT in the default install script.
# DLQ (dlq_inspect / event_dead / pgque.dead_letter) was promoted to the default
# install — nack() depends on event_dead, so the DLQ has to ship by default.
if grep -q 'maint_deliver_delayed\|send_at\|delayed_events\|otel_metrics\|queue_stats' "${INSTALL_FILE}"; then
  echo "FAIL: experimental APIs leaked into default install script"
  asm_errors=$((asm_errors + 1))
else
  echo "PASS: experimental APIs excluded from default install script"
fi

# Verify pgque-api section is present (if default api files exist)
if [[ -d "${API_DIR}" ]]; then
  if grep -q 'Section 7: pgque-api' "${INSTALL_FILE}"; then
    echo "PASS: pgque-api section present in install script"
  else
    echo "FAIL: pgque-api section missing from install script"
    asm_errors=$((asm_errors + 1))
  fi
fi

# Verify the get_batch_cursor SECURITY contract reached the assembled file.
# The per-function awk patch above runs before assembly; this guards against
# a future assembly change that strips comments or reorders sections.
if grep -q 'SECURITY: i_extra_where is concatenated' "${INSTALL_FILE}" \
   && grep -q 'Trusted SQL fragment, not a parameter' "${INSTALL_FILE}"; then
  echo "PASS: get_batch_cursor SECURITY contract present in install script"
else
  echo "FAIL: get_batch_cursor SECURITY contract missing from install script"
  asm_errors=$((asm_errors + 1))
fi

echo ""
if [[ ${asm_errors} -eq 0 ]]; then
  echo "=== ASSEMBLY COMPLETE — ALL CHECKS PASSED ==="
else
  echo "=== ASSEMBLY: ${asm_errors} CHECK(S) FAILED ==="
  exit 1
fi

# -- pg_tle packaging ---------------------------------------------------------
# Wrap sql/pgque.sql as a pg_tle (Trusted Language Extension) so users on
# managed Postgres can install via create extension pgque and get extension
# membership / drop extension cascade for free. The output sql/pgque-tle.sql
# is a single self-contained file that works in any SQL client (psql, GUI
# tools like DBeaver, JDBC, libpq-direct callers).

echo ""
echo "=== Packaging pg_tle install script ==="

PGTLE_FILE="${SQL_DIR}/pgque-tle.sql"
PGTLE_DOLLAR_TAG="pgque_extension_body"

# Sanity check: neither dollar-quote tag (the body tag and the outer wrapper
# tag) may appear in the body content, otherwise the literal terminates early
# and pg_tle.install_extension receives a truncated body. Use grep -F so the
# `$` characters are matched literally instead of as end-of-line anchors.
for tag in "${PGTLE_DOLLAR_TAG}" wrapper; do
  if grep -qF "\$${tag}\$" "${INSTALL_FILE}"; then
    echo "FAIL: dollar-quote tag \$${tag}\$ collides with body content" >&2
    exit 1
  fi
done

# Extract the version string from pgque.version() in lifecycle.sql so the
# pg_tle install always advertises the same version pgque.version() returns at
# runtime. Anchor to the function so unrelated `return '...'` lines added later
# cannot shadow it; skip comment lines so a stray `-- return 'old'` cannot
# match. Validate the extracted literal so a malformed value cannot break the
# heredoc that embeds it into the generated SQL.
PGTLE_VERSION=$(awk '
  /create or replace function pgque\.version/ { in_fn=1; next }
  in_fn && $0 !~ /^[[:space:]]*--/ && match($0, /return '\''[^'\'']+'\''/) {
    s=substr($0, RSTART+8, RLENGTH-9); print s; exit
  }
' "${ADDITIONS_DIR}/lifecycle.sql")

if [[ -z "${PGTLE_VERSION}" ]]; then
  echo "FAIL: could not extract pgque version from pgque.version() in lifecycle.sql" >&2
  exit 1
fi

if ! [[ "${PGTLE_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.]+)?$ ]]; then
  echo "FAIL: pgque version '${PGTLE_VERSION}' is not a valid semver string" >&2
  exit 1
fi

cat > "${PGTLE_FILE}" << HEADER
-- pgque-tle.sql -- Install PgQue as a pg_tle (Trusted Language Extension).
-- Auto-generated by build/transform.sh from sql/pgque.sql. Do not edit by hand.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- pg_tle (Trusted Language Extensions, https://github.com/aws/pg_tle) lets
-- PgQue install via create extension without a C extension binary on disk.
-- After loading this script, the extension is registered with pg_tle and can
-- be created or dropped via the standard create / drop extension commands.
--
-- This file is self-contained: it works in psql, GUI tools (DBeaver, etc.),
-- JDBC, libpq-direct callers — anywhere that accepts SQL.
--
-- Prerequisites:
--   1. pg_tle is installed in this database:
--          create extension if not exists pg_tle;
--   2. The current role is a member of pgtle_admin and has CREATEROLE.
--      Postgres roles are database-cluster-global and cannot be created from
--      inside a TLE install body, so this script pre-creates them.
--
-- Usage:
--   psql -d mydb -f sql/pgque-tle.sql
--   psql -d mydb -c 'create extension pgque;'
--
-- Uninstall:
--   psql -d mydb -f sql/pgque-tle-uninstall.sql

\set ON_ERROR_STOP on

-- Step 1: confirm pg_tle is loaded.
do \$\$
begin
    if not exists (select 1 from pg_catalog.pg_extension where extname = 'pg_tle') then
        raise exception 'pg_tle is not available in this database. '
            'Add pg_tle to shared_preload_libraries (managed providers: '
            'parameter group + reboot; self-hosted: alter system + restart), '
            'then run: create extension pg_tle; '
            'and grant pgtle_admin to the current role.';
    end if;
end \$\$;

-- Step 2: pre-create the pgque_* roles. Roles are cluster-global and cannot
-- be created from inside a tle install body, so the body's idempotent role
-- creation only succeeds when the roles already exist.
do \$\$
begin
    if not exists (select 1 from pg_catalog.pg_roles where rolname = 'pgque_reader') then
        create role pgque_reader;
    end if;
    if not exists (select 1 from pg_catalog.pg_roles where rolname = 'pgque_writer') then
        create role pgque_writer;
    end if;
    if not exists (select 1 from pg_catalog.pg_roles where rolname = 'pgque_admin') then
        create role pgque_admin;
    end if;
end \$\$;

-- Step 3: register the extension body with pg_tle.
-- Same version already registered -> no-op (so deployment scripts can rerun).
-- Different version already registered -> raise so the user goes through the
-- explicit uninstall + reinstall path; pg_tle has no managed upgrade path
-- between unrelated registrations of an extension.
do \$wrapper\$
declare
    existing_version text;
begin
    select default_version into existing_version
    from pgtle.available_extensions()
    where name = 'pgque';

    if existing_version = '${PGTLE_VERSION}' then
        raise notice 'pgque ${PGTLE_VERSION} already registered with pg_tle; skipping install_extension().';
        return;
    end if;

    if existing_version is not null then
        raise exception 'pgque is already registered with pg_tle at version % '
            'but this script registers version %. Run '
            'sql/pgque-tle-uninstall.sql first to remove the existing '
            'registration, then re-run this script.',
            existing_version, '${PGTLE_VERSION}';
    end if;

    perform pgtle.install_extension(
        'pgque',
        '${PGTLE_VERSION}',
        'PgQue — PgQ Universal Edition (zero-bloat Postgres queue)',
\$${PGTLE_DOLLAR_TAG}\$
HEADER

cat "${INSTALL_FILE}" >> "${PGTLE_FILE}"

cat >> "${PGTLE_FILE}" << FOOTER
\$${PGTLE_DOLLAR_TAG}\$
    );
end \$wrapper\$;

\\echo ''
\\echo 'PgQue ${PGTLE_VERSION} registered with pg_tle.'
\\echo 'Run create extension pgque; to materialise the schema in this database.'
FOOTER

pgtle_lines=$(wc -l < "${PGTLE_FILE}")
echo "Wrote ${PGTLE_FILE} (${pgtle_lines} lines, version ${PGTLE_VERSION})"

# Self-check on the generated file.
pgtle_errors=0

if ! grep -q 'pgtle.install_extension(' "${PGTLE_FILE}"; then
  echo "FAIL: pg_tle install script missing pgtle.install_extension() call"
  pgtle_errors=$((pgtle_errors + 1))
fi

if ! grep -q 'create schema if not exists pgque' "${PGTLE_FILE}"; then
  echo "FAIL: pg_tle install script body missing pgque schema creation"
  pgtle_errors=$((pgtle_errors + 1))
fi

if [[ ${pgtle_errors} -eq 0 ]]; then
  echo "=== pg_tle PACKAGING COMPLETE ==="
else
  echo "=== pg_tle PACKAGING: ${pgtle_errors} CHECK(S) FAILED ==="
  exit 1
fi
