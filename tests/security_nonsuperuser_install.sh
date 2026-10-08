#!/usr/bin/env bash
# Verify partitioned delivery under a non-superuser install owner.
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
set -Eeuo pipefail

# Usage:
#   PGQUE_TEST_SUPERUSER_DSN='dbname=postgres user=postgres' \
#     tests/security_nonsuperuser_install.sh
#
# The superuser connection creates disposable roles, two databases, and any
# missing pgque_* app-role fixtures. Each SQL install runs with SET ROLE as a
# non-superuser owner without app-role grants. App-role bootstrap is separate:
# PostgreSQL 16+ gives a non-superuser role creator automatic ADMIN membership,
# which would invalidate this test's membership-independent ownership proof.
# Bare reader/writer roles must complete keyed delivery but cannot read slot
# catalogs or call the admin-only get_batch_cursor overloads directly.
#
# The negative control changes receive_partitioned's owner and grants its
# other private helpers. Delivery must then fail at get_batch_cursor with
# SQLSTATE 42501. This isolates the shared-owner requirement (partition-keys
# SPEC section 6). CI runs this separately from the single-database SQL suite.

if [[ -z "${PGQUE_TEST_SUPERUSER_DSN:-}" ]]; then
  echo "PGQUE_TEST_SUPERUSER_DSN is required (a superuser connection)" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

psql_super=(psql --no-psqlrc -v ON_ERROR_STOP=1 "${PGQUE_TEST_SUPERUSER_DSN}")

# PIDs can repeat across hosts/containers that share a PostgreSQL cluster.
# Keep identifiers below PostgreSQL's 63-byte limit with a random run token.
suffix="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
[[ "${suffix}" =~ ^[0-9a-f]{32}$ ]] || { echo "cannot generate run token" >&2; exit 1; }
installer="pgque_nsu_installer_${suffix}"
other_owner="pgque_nsu_other_${suffix}"
reader_app="pgque_nsu_reader_${suffix}"
writer_app="pgque_nsu_writer_${suffix}"
db_main="pgque_nsu_main_${suffix}"
db_negctl="pgque_nsu_negctl_${suffix}"
workdir="$(mktemp -d)"
created_databases=()
created_roles=()

cleanup() {
  local status=$? name failed=0
  trap - EXIT
  # One psql call per statement: DROP DATABASE refuses to run inside the
  # implicit transaction a multi-statement -c would create, and one failing
  # drop must not abort the rest. Leave the cluster-wide pgque_* app roles
  # intact for other databases. A planned name is NOT proof of ownership:
  # bootstrap can stop at a collision after creating only some fixtures.
  for name in "${created_databases[@]}"; do
    "${psql_super[@]}" -qAtc "drop database if exists ${name} with (force)" \
      >/dev/null 2>&1 || {
        echo "FAIL: cleanup could not drop owned database ${name}" >&2
        failed=1
      }
  done
  for name in "${created_roles[@]}"; do
    "${psql_super[@]}" -qAtc "drop role if exists ${name}" \
      >/dev/null 2>&1 || {
        echo "FAIL: cleanup could not drop owned role ${name}" >&2
        failed=1
      }
  done
  if (( failed )); then
    echo "WARNING: test cleanup incomplete for owned fixtures: ${created_databases[*]} ${created_roles[*]}" >&2
  fi
  rm -rf "${workdir}" || {
    echo "FAIL: cleanup could not remove work directory ${workdir}" >&2
    failed=1
  }
  # A cleanup error must fail a successful body, but never mask the body's
  # original failure. Emit terminal success only after all cleanup succeeds.
  if (( status == 0 && failed )); then
    status=1
  fi
  if (( status == 0 )); then
    echo "PASS: security_nonsuperuser_install -- co-ownership invariant holds under a non-superuser, non-pgque_admin install owner (and the harness detects its absence)"
  fi
  exit "${status}"
}
trap cleanup EXIT

print_debug() {
  for f in "${workdir}"/*.out "${workdir}"/*.err; do
    [[ -e "${f}" ]] || continue
    echo "--- ${f##*/} ---" >&2
    cat "${f}" >&2
  done
}

# run_step <name> <sql-file>: run against the superuser DSN (scripts \connect
# and `set role` themselves), capture output, fail loudly.
run_step() {
  local name="$1" file="$2"
  if ! "${psql_super[@]}" -f "${file}" \
      >"${workdir}/${name}.out" 2>"${workdir}/${name}.err"; then
    echo "FAIL: step ${name}" >&2
    print_debug
    exit 1
  fi
  echo "ok: ${name}"
}

# One CREATE per call: record ownership immediately after that CREATE
# succeeds, before any later bootstrap command can fail. Do not adopt an
# existing resource or add its name to the cleanup lists after a collision.
create_fixture() {
  local kind="$1" name="$2" options="${3:-}"
  local step="00_create_${name}"
  printf 'create %s %s %s;\n' "${kind}" "${name}" "${options}" >"${workdir}/${step}.sql"
  run_step "${step}" "${workdir}/${step}.sql"
  if [[ "${kind}" == database ]]; then
    created_databases+=("${name}")
  else
    created_roles=("${name}" "${created_roles[@]}")
  fi
}

# --- 1. bootstrap: roles + installer-owned databases (superuser) ------------
# Create missing app-role fixtures as superuser so the install owner has no
# app-role membership on fresh clusters either. Do not revoke creator grants:
# their dependent grants could change the role hierarchy we intend to test.
# Preserve existing roles and ensure the hierarchy expected by the installer.
# This tests SQL installation and co-ownership, not app-role creation rights.
create_fixture role "${installer}" createrole
create_fixture role "${other_owner}"
create_fixture role "${reader_app}"
create_fixture role "${writer_app}"
cat >"${workdir}/00_bootstrap.sql" <<SQL
do \$\$
begin
  if not exists (select 1 from pg_roles where rolname = 'pgque_reader') then
    create role pgque_reader;
  end if;
  if not exists (select 1 from pg_roles where rolname = 'pgque_writer') then
    create role pgque_writer;
  end if;
  if not exists (select 1 from pg_roles where rolname = 'pgque_admin') then
    create role pgque_admin;
  end if;
  if not pg_has_role('pgque_admin', 'pgque_reader', 'member') then
    grant pgque_reader to pgque_admin;
  end if;
  if not pg_has_role('pgque_admin', 'pgque_writer', 'member') then
    grant pgque_writer to pgque_admin;
  end if;
end \$\$;
SQL
run_step 00_bootstrap "${workdir}/00_bootstrap.sql"
create_fixture database "${db_main}" "owner ${installer}"
create_fixture database "${db_negctl}" "owner ${installer}"

# Required oracles use IF/RAISE, not ASSERT: plpgsql.check_asserts can be off
# in any session, including sessions created by \connect. NULL must fail too.

# --- 2. install as the NON-superuser owner (both databases) -----------------
for db in "${db_main}" "${db_negctl}"; do
  cat >"${workdir}/10_install_${db}.sql" <<SQL
\\connect ${db}
set role ${installer};
do \$\$
begin
  if (current_user = '${installer}') is distinct from true then
    raise exception '%', format('install must run as the test installer, got %s', current_user);
  end if;
  if (not (select rolsuper from pg_roles where rolname = current_user)) is distinct from true then
    raise exception '%', 'installer must NOT be superuser';
  end if;
end \$\$;
begin;
\\i devel/sql/pgque.sql
commit;
/* The invariant is co-ownership, not privilege: re-assert post-install that
   the installer is neither superuser nor a pgque_admin member. */
do \$\$
begin
  if (not pg_has_role(current_user, 'pgque_admin', 'member')) is distinct from true then
    raise exception '%', 'installer must NOT be a pgque_admin member';
  end if;
  if (not pg_has_role(current_user, 'pgque_reader', 'member')) is distinct from true then
    raise exception '%', 'installer must NOT be a pgque_reader member';
  end if;
  if (not pg_has_role(current_user, 'pgque_writer', 'member')) is distinct from true then
    raise exception '%', 'installer must NOT be a pgque_writer member';
  end if;
end \$\$;
SQL
  if ! "${psql_super[@]}" -f "${workdir}/10_install_${db}.sql" \
      >"${workdir}/10_install_${db}.out" 2>"${workdir}/10_install_${db}.err"; then
    echo "FAIL: FINDING -- devel/sql/pgque.sql does not install as a non-superuser owner (db=${db})" >&2
    print_debug
    exit 1
  fi
  echo "ok: 10_install_${db} (non-superuser install succeeded)"
done

# --- app-role grants (cluster-wide; roles exist after the install) ----------
cat >"${workdir}/15_grants.sql" <<SQL
grant pgque_reader to ${reader_app};
grant pgque_writer to ${writer_app};
SQL
run_step 15_grants "${workdir}/15_grants.sql"

# --- ownership sanity: partition functions co-owned with get_batch_cursor ---
cat >"${workdir}/20_ownership.sql" <<SQL
\\connect ${db_main}
do \$\$
declare
  v_bad text;
begin
  select string_agg(p.proname || ' owner=' || r.rolname, ', ') into v_bad
  from pg_proc as p
  join pg_roles as r on r.oid = p.proowner
  where p.pronamespace = 'pgque'::regnamespace
    and p.proname in ('get_batch_cursor', 'receive_partitioned',
                      'ack_partitioned', 'nack_partitioned',
                      'subscribe_slot', 'claim_slot', 'release_slot')
    and r.rolname <> '${installer}';
  if (v_bad is null) is distinct from true then
    raise exception '%', format('co-ownership broken out of the box: %s', v_bad);
  end if;
end \$\$;
SQL
run_step 20_ownership "${workdir}/20_ownership.sql"

# --- 3+4+6. end-to-end as bare app roles in the main database ---------------
cat >"${workdir}/30_flow_main.sql" <<SQL
\\connect ${db_main}

-- Queue creation is admin surface: done by the (non-superuser) install owner.
set role ${installer};
select pgque.create_queue('nsu_q');
reset role;

-- A bare reader subscribes both slots (n = 2).
set role ${reader_app};
select pgque.subscribe_slot('nsu_q', 'c', 0, 2);
select pgque.subscribe_slot('nsu_q', 'c', 1, 2);
reset role;

-- A bare writer does the keyed sends.
set role ${writer_app};
do \$\$
declare
  i int;
  k text;
begin
  for i in 1..2 loop
    foreach k in array array['k-a', 'k-b', 'k-c'] loop
      perform pgque.send('nsu_q', 'ev', format('payload-%s-%s', k, i), k);
    end loop;
  end loop;
end \$\$;
reset role;

-- Ticker: install owner (admin surface).
set role ${installer};
select pgque.force_next_tick('nsu_q');
select pgque.ticker();
reset role;

-- Bare reader: claim -> receive_partitioned -> ack, both slots, end to end.
set role ${reader_app};
create temp table nsu_got (slot int not null, msg_id bigint not null, key text);
do \$\$
declare
  v_slot int;
  v_msg pgque.message;
  v_epoch bigint;
begin
  for v_slot in 0..1 loop
    v_epoch := pgque.claim_slot('nsu_q', 'c', v_slot, 'w0');
    if (v_epoch is not null) is distinct from true then
      raise exception '%', format('reader: claim of free slot %s must return an epoch', v_slot);
    end if;
    for v_msg in
      select * from pgque.receive_partitioned('nsu_q', 'c', v_slot, 2, 'w0', 100)
    loop
      insert into nsu_got (slot, msg_id, key)
      values (v_slot, v_msg.msg_id, v_msg.extra1);
    end loop;
    perform pgque.ack_partitioned('nsu_q', 'c', v_slot, 2, 'w0');
  end loop;
end \$\$;
do \$\$
declare
  v_total int;
begin
  select count(*) into v_total from nsu_got;
  if (v_total = 6) is distinct from true then
    raise exception '%', format('reader must drain all 6 keyed events, got %s', v_total);
  end if;
  perform 1
  from (
    select key from nsu_got group by key having count(distinct slot) > 1
  ) as x;
  if (not found) is distinct from true then
    raise exception '%', 'each key must be delivered by exactly one slot';
  end if;
  perform 1
  from nsu_got
  where slot <> (pg_catalog.hashtextextended(key, 0) % 2 + 2) % 2;
  if (not found) is distinct from true then
    raise exception '%', 'delivered slot must match hash routing';
  end if;
  raise notice 'PASS: bare pgque_reader end-to-end (subscribe/claim/receive_partitioned/ack) under a non-superuser install owner';
end \$\$;

-- The reader must still be BLOCKED from the trusted-SQL sink itself.
do \$\$
declare
  v_state text;
begin
  begin
    perform pgque.get_batch_cursor(1::bigint, 'nsu_probe3', 0);
    raise exception 'reader must not call get_batch_cursor/3';
  exception
    when insufficient_privilege then v_state := sqlstate;
  end;
  if (v_state = '42501') is distinct from true then
    raise exception '%', format('expected 42501 for reader on get_batch_cursor/3, got %s', v_state);
  end if;

  v_state := null;
  begin
    perform pgque.get_batch_cursor(1::bigint, 'nsu_probe4', 0, 'true');
    raise exception 'reader must not call get_batch_cursor/4';
  exception
    when insufficient_privilege then v_state := sqlstate;
  end;
  if (v_state = '42501') is distinct from true then
    raise exception '%', format('expected 42501 for reader on get_batch_cursor/4, got %s', v_state);
  end if;
  raise notice 'PASS: reader blocked from get_batch_cursor/3 and /4 (42501)';
end \$\$;

-- Lease/N state stays server-side: not readable by the reader...
do \$\$
declare
  v_state text;
begin
  begin
    perform 1 from pgque.partition_consumer;
    raise exception 'reader must not read pgque.partition_consumer';
  exception
    when insufficient_privilege then v_state := sqlstate;
  end;
  if (v_state = '42501') is distinct from true then
    raise exception '%', 'expected 42501 reading partition_consumer as reader';
  end if;

  v_state := null;
  begin
    perform 1 from pgque.partition_slot;
    raise exception 'reader must not read pgque.partition_slot';
  exception
    when insufficient_privilege then v_state := sqlstate;
  end;
  if (v_state = '42501') is distinct from true then
    raise exception '%', 'expected 42501 reading partition_slot as reader';
  end if;
  raise notice 'PASS: partition tables not readable by pgque_reader';
end \$\$;
reset role;

-- ...nor by the writer.
set role ${writer_app};
do \$\$
declare
  v_state text;
begin
  begin
    perform 1 from pgque.partition_consumer;
    raise exception 'writer must not read pgque.partition_consumer';
  exception
    when insufficient_privilege then v_state := sqlstate;
  end;
  if (v_state = '42501') is distinct from true then
    raise exception '%', 'expected 42501 reading partition_consumer as writer';
  end if;

  v_state := null;
  begin
    perform 1 from pgque.partition_slot;
    raise exception 'writer must not read pgque.partition_slot';
  exception
    when insufficient_privilege then v_state := sqlstate;
  end;
  if (v_state = '42501') is distinct from true then
    raise exception '%', 'expected 42501 reading partition_slot as writer';
  end if;
  raise notice 'PASS: partition tables not readable by pgque_writer';
end \$\$;
reset role;
SQL
run_step 30_flow_main "${workdir}/30_flow_main.sql"

# --- 5. NEGATIVE CONTROL in the second database ------------------------------
# Phase A: the identical flow works in the untouched install.
cat >"${workdir}/40_negctl_pre.sql" <<SQL
\\connect ${db_negctl}
set role ${installer};
select pgque.create_queue('negq');
reset role;
set role ${reader_app};
select pgque.subscribe_slot('negq', 'c', 0, 1);
reset role;
set role ${writer_app};
select pgque.send('negq', 'ev', 'payload-1', 'k-a');
reset role;
set role ${installer};
select pgque.force_next_tick('negq');
select pgque.ticker();
reset role;
set role ${reader_app};
do \$\$
declare
  v_cnt int := 0;
  v_msg pgque.message;
begin
  perform pgque.claim_slot('negq', 'c', 0, 'w0');
  for v_msg in
    select * from pgque.receive_partitioned('negq', 'c', 0, 1, 'w0', 100)
  loop
    v_cnt := v_cnt + 1;
  end loop;
  if (v_cnt = 1) is distinct from true then
    raise exception '%', format('negctl pre-flip: expected 1 event, got %s', v_cnt);
  end if;
  perform pgque.ack_partitioned('negq', 'c', 0, 1, 'w0');
  raise notice 'PASS: negctl pre-flip reader flow works';
end \$\$;
reset role;
SQL
run_step 40_negctl_pre "${workdir}/40_negctl_pre.sql"

# Phase B: break ONLY the co-ownership. The new owner is deliberately given
# every OTHER privilege receive_partitioned's body needs (pgque_reader
# membership for next_batch, execute on the internal helpers) so the flow
# fails precisely at the admin-only get_batch_cursor -- isolating ownership
# as the load-bearing mechanism.
cat >"${workdir}/50_negctl_flip.sql" <<SQL
\\connect ${db_negctl}
alter function pgque.receive_partitioned(text, text, int, int, text, int)
  owner to ${other_owner};
grant pgque_reader to ${other_owner};
grant execute on function pgque._slot_guard(text, text, int, int, text) to ${other_owner};
grant execute on function pgque._slot_batch(text, text, int, int) to ${other_owner};
grant execute on function pgque._slot_name(text, int, int) to ${other_owner};
grant execute on function pgque._assert_unpaged(bigint) to ${other_owner};
SQL
run_step 50_negctl_flip "${workdir}/50_negctl_flip.sql"

# Phase C: the same reader flow must now FAIL 42501 on get_batch_cursor.
cat >"${workdir}/55_negctl_post.sql" <<SQL
\\connect ${db_negctl}
-- The helper-identity oracle below compares the complete server diagnostic.
-- Set its language before leaving the superuser session role.
set lc_messages = 'C';
set role ${writer_app};
select pgque.send('negq', 'ev', 'payload-2', 'k-a');
reset role;
set role ${installer};
select pgque.force_next_tick('negq');
select pgque.ticker();
reset role;
set role ${reader_app};
do \$\$
declare
  v_state text;
  v_msg text;
begin
  perform pgque.claim_slot('negq', 'c', 0, 'w0');
  begin
    perform 1 from pgque.receive_partitioned('negq', 'c', 0, 1, 'w0', 100);
    raise exception 'NEGATIVE CONTROL HAS NO TEETH: receive_partitioned still works with a foreign owner';
  exception
    when insufficient_privilege then
      get stacked diagnostics
        v_state = returned_sqlstate,
        v_msg = message_text;
  end;
  if (v_state = '42501') is distinct from true then
    raise exception '%', format('negctl: expected 42501, got %s', v_state);
  end if;
  if v_msg is distinct from 'permission denied for function get_batch_cursor' then
    raise exception '%', format('negctl: expected the denial to be on get_batch_cursor, got: %s', v_msg);
  end if;
  raise notice 'PASS: negative control -- ownership flip breaks the reader flow with 42501 on get_batch_cursor';
end \$\$;
reset role;
SQL
run_step 55_negctl_post "${workdir}/55_negctl_post.sql"

grep -h 'NOTICE:.*PASS' \
  "${workdir}/30_flow_main.err" \
  "${workdir}/40_negctl_pre.err" \
  "${workdir}/55_negctl_post.err" 2>/dev/null || true
