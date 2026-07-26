#!/usr/bin/env bash
# Prove the partition-keys SECURITY DEFINER CO-OWNERSHIP invariant under a
# NON-superuser install owner (blueprints/partition-keys/SPEC.md section 6,
# T-security in section 9).
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
set -Eeuo pipefail

# Usage:
#   PGQUE_TEST_SUPERUSER_DSN='dbname=postgres user=postgres' \
#     tests/security_nonsuperuser_install.sh
#
# Run from anywhere; the harness cds to the repo root itself. The DSN must be
# a SUPERUSER connection: role and database creation need a bootstrap
# superuser. The INSTALL itself is then executed under `set role` as a
# non-superuser, non-pgque_admin role -- `set role` applies that role's
# privileges to every permission and ownership check, so the install runs
# with genuinely non-superuser rights while the harness needs no passwords.
#
# NOT wired into tests/run_all.sql: run_all runs inside one database as one
# role, while this harness needs a superuser bootstrap, two throwaway
# databases, and cluster-level roles. CI wiring is a follow-up.
#
# What the invariant is (SPEC section 6): receive_partitioned reaches the
# admin-only pgque.get_batch_cursor(4) trusted-SQL hook NOT via any grant but
# because both functions share an OWNER -- the role that ran devel/sql/
# pgque.sql (a function owner may execute its own functions regardless of
# grants). The invariant is "installed by the pgque install owner"; it must
# NOT require that owner to be superuser or a pgque_admin member. Every prior
# test ran against a superuser-owned install, where ownership is masked by
# superuser privilege -- this harness is the first to exercise the invariant
# as designed.
#
# Steps:
#   1. superuser: bootstrap roles (installer with CREATEROLE, one reader app,
#      one writer app, one negative-control owner) + two databases OWNED BY
#      the installer.
#   2. AS THE INSTALLER (not superuser): `\i devel/sql/pgque.sql` into the
#      main database. Any failure here is itself a finding.
#   3. end-to-end: installer creates a queue; a bare pgque_reader subscribes
#      slots, claims a lease, receives the hash-filtered stream, and acks; a
#      bare pgque_writer does the keyed sends.
#   4. the reader calling get_batch_cursor directly still fails 42501 (both
#      overloads -- mirrors tests/test_security_get_batch_cursor.sql).
#   5. NEGATIVE CONTROL (proves the harness has teeth): in a second identical
#      install, the superuser reassigns receive_partitioned's OWNER to a role
#      that has every OTHER privilege the function body needs (pgque_reader
#      membership + execute on the internal helpers) but is not the install
#      owner. The previously-working reader flow must now fail 42501 on
#      get_batch_cursor -- co-ownership, not grants, is load-bearing.
#   6. partition_consumer / partition_slot are not readable by reader or
#      writer app roles.

if [[ -z "${PGQUE_TEST_SUPERUSER_DSN:-}" ]]; then
  echo "PGQUE_TEST_SUPERUSER_DSN is required (a superuser connection)" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

psql_super=(psql --no-psqlrc -v ON_ERROR_STOP=1 "${PGQUE_TEST_SUPERUSER_DSN}")

suffix="${$}"
installer="pgque_nsu_installer_${suffix}"
other_owner="pgque_nsu_other_${suffix}"
reader_app="pgque_nsu_reader_${suffix}"
writer_app="pgque_nsu_writer_${suffix}"
db_main="pgque_nsu_main_${suffix}"
db_negctl="pgque_nsu_negctl_${suffix}"
workdir="$(mktemp -d)"

cleanup() {
  local stmt failed=0
  # One psql call per statement: DROP DATABASE refuses to run inside the
  # implicit transaction a multi-statement -c would create, and one failing
  # drop must not abort the rest. Best-effort: on a cluster where the pgque_*
  # roles did not pre-exist, the installer created them and (on PG16+) is
  # recorded as grantor of their memberships, which can block DROP ROLE --
  # warn, don't fail the run over cleanup.
  for stmt in \
    "drop database if exists ${db_main} with (force)" \
    "drop database if exists ${db_negctl} with (force)" \
    "drop role if exists ${reader_app}" \
    "drop role if exists ${writer_app}" \
    "drop role if exists ${other_owner}" \
    "drop role if exists ${installer}"; do
    "${psql_super[@]}" -qAtc "${stmt}" >/dev/null 2>&1 || failed=1
  done
  if (( failed )); then
    echo "WARNING: test cleanup incomplete (drop pgque_nsu_*_${suffix} databases/roles manually)" >&2
  fi
  rm -rf "${workdir}"
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

# --- 1. bootstrap: roles + installer-owned databases (superuser) ------------
# If the cluster-wide pgque_* roles pre-exist (created by an earlier install),
# make sure the admin memberships the install script conditionally grants are
# already in place -- a non-superuser installer cannot grant membership in
# roles it does not administer. On a fresh cluster the installer CREATES the
# roles itself (that is what CREATEROLE is for) and the grants are its own.
cat >"${workdir}/00_bootstrap.sql" <<SQL
create role ${installer} createrole;
create role ${other_owner};
create role ${reader_app};
create role ${writer_app};
do \$\$
begin
  if exists (select 1 from pg_roles where rolname = 'pgque_admin') then
    if not pg_has_role('pgque_admin', 'pgque_reader', 'member') then
      grant pgque_reader to pgque_admin;
    end if;
    if not pg_has_role('pgque_admin', 'pgque_writer', 'member') then
      grant pgque_writer to pgque_admin;
    end if;
  end if;
end \$\$;
create database ${db_main} owner ${installer};
create database ${db_negctl} owner ${installer};
SQL
run_step 00_bootstrap "${workdir}/00_bootstrap.sql"

# --- 2. install as the NON-superuser owner (both databases) -----------------
for db in "${db_main}" "${db_negctl}"; do
  cat >"${workdir}/10_install_${db}.sql" <<SQL
\\connect ${db}
set role ${installer};
do \$\$
begin
  assert current_user = '${installer}',
    format('install must run as the test installer, got %s', current_user);
  assert not (select rolsuper from pg_roles where rolname = current_user),
    'installer must NOT be superuser';
end \$\$;
begin;
\\i devel/sql/pgque.sql
commit;
/* The invariant is co-ownership, not privilege: re-assert post-install that
   the installer is neither superuser nor a pgque_admin member. */
do \$\$
begin
  assert not pg_has_role(current_user, 'pgque_admin', 'member'),
    'installer must NOT be a pgque_admin member';
  assert not pg_has_role(current_user, 'pgque_reader', 'member'),
    'installer must NOT be a pgque_reader member';
  assert not pg_has_role(current_user, 'pgque_writer', 'member'),
    'installer must NOT be a pgque_writer member';
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
  assert v_bad is null,
    format('co-ownership broken out of the box: %s', v_bad);
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
    assert v_epoch is not null,
      format('reader: claim of free slot %s must return an epoch', v_slot);
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
  assert v_total = 6,
    format('reader must drain all 6 keyed events, got %s', v_total);
  perform 1
  from (
    select key from nsu_got group by key having count(distinct slot) > 1
  ) as x;
  assert not found, 'each key must be delivered by exactly one slot';
  perform 1
  from nsu_got
  where slot <> (pg_catalog.hashtextextended(key, 0) % 2 + 2) % 2;
  assert not found, 'delivered slot must match hash routing';
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
  assert v_state = '42501',
    format('expected 42501 for reader on get_batch_cursor/3, got %s', v_state);

  v_state := null;
  begin
    perform pgque.get_batch_cursor(1::bigint, 'nsu_probe4', 0, 'true');
    raise exception 'reader must not call get_batch_cursor/4';
  exception
    when insufficient_privilege then v_state := sqlstate;
  end;
  assert v_state = '42501',
    format('expected 42501 for reader on get_batch_cursor/4, got %s', v_state);
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
  assert v_state = '42501', 'expected 42501 reading partition_consumer as reader';

  v_state := null;
  begin
    perform 1 from pgque.partition_slot;
    raise exception 'reader must not read pgque.partition_slot';
  exception
    when insufficient_privilege then v_state := sqlstate;
  end;
  assert v_state = '42501', 'expected 42501 reading partition_slot as reader';
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
  assert v_state = '42501', 'expected 42501 reading partition_consumer as writer';

  v_state := null;
  begin
    perform 1 from pgque.partition_slot;
    raise exception 'writer must not read pgque.partition_slot';
  exception
    when insufficient_privilege then v_state := sqlstate;
  end;
  assert v_state = '42501', 'expected 42501 reading partition_slot as writer';
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
  assert v_cnt = 1,
    format('negctl pre-flip: expected 1 event, got %s', v_cnt);
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
SQL
run_step 50_negctl_flip "${workdir}/50_negctl_flip.sql"

# Phase C: the same reader flow must now FAIL 42501 on get_batch_cursor.
cat >"${workdir}/55_negctl_post.sql" <<SQL
\\connect ${db_negctl}
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
  assert v_state = '42501',
    format('negctl: expected 42501, got %s', v_state);
  assert position('get_batch_cursor' in v_msg) > 0,
    format('negctl: expected the denial to be on get_batch_cursor, got: %s', v_msg);
  raise notice 'PASS: negative control -- ownership flip breaks the reader flow with 42501 on get_batch_cursor';
end \$\$;
reset role;
SQL
run_step 55_negctl_post "${workdir}/55_negctl_post.sql"

grep -h 'NOTICE:.*PASS' \
  "${workdir}/30_flow_main.err" \
  "${workdir}/40_negctl_pre.err" \
  "${workdir}/55_negctl_post.err" 2>/dev/null || true
echo "PASS: security_nonsuperuser_install -- co-ownership invariant holds under a non-superuser, non-pgque_admin install owner (and the harness detects its absence)"
