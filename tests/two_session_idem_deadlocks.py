#!/usr/bin/env python3
"""Prove live-duplicate send_idem has no AB-BA deadlocks and drop_queue fails fast.

Requires only Python's standard library and psql. The target must already have
devel/sql/pgque.sql installed. Run with PGQUE_TEST_DSN set to a disposable test
database; use PGQUE_ALLOW_TEST_MUTATION=1 for other disposable database names.
"""
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

import json
import os
import re
import signal
import subprocess
import sys
import time
import uuid


def main():
    dsn = os.environ.get("PGQUE_TEST_DSN")
    if not dsn:
        print("PGQUE_TEST_DSN is required", file=sys.stderr)
        return 2

    command = ["psql", "--no-psqlrc", "--quiet", "--no-align", "--tuples-only",
               "--set=ON_ERROR_STOP=1", "--dbname=" + dsn]
    run_id = uuid.uuid4().hex[:16]
    queues = []
    applications = []
    processes = []
    failures = 0

    def sql(statement):
        result = subprocess.run(
            command, input="set statement_timeout='15s';\n" + statement,
            text=True, capture_output=True, timeout=20,
            env={**os.environ, "PAGER": "cat"})
        if result.returncode:
            raise RuntimeError(result.stderr.strip() or "psql failed")
        return result.stdout.strip()

    def start(name, statement):
        applications.append(name)
        process = subprocess.Popen(
            command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True,
            env={**os.environ, "PAGER": "cat", "PGAPPNAME": name})
        processes.append(process)
        process.stdin.write("set statement_timeout='15s';\n" + statement + "\n")
        process.stdin.flush()
        return process

    def finish(process, tail="\\q\n", expect_ok=True):
        output, error = process.communicate(tail, timeout=20)
        if expect_ok and process.returncode:
            raise RuntimeError(error.strip() or "psql backend failed")
        return process.returncode, output.strip(), error.strip()

    def barrier(statement, description, process):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            value = sql(statement)
            if value:
                print("barrier: " + description + " (backend " + value + ")", flush=True)
                return int(value)
            if process.poll() is not None:
                raise RuntimeError(description + ": backend exited early")
            time.sleep(0.05)
        raise RuntimeError(description + ": observed-state barrier timed out")

    database = sql("select current_database();")
    if (not re.search(r"(^|_)test($|_)", database)
            and os.environ.get("PGQUE_ALLOW_TEST_MUTATION") != "1"):
        print("FAIL: refusing concurrency test mutations in database " + repr(database),
              file=sys.stderr)
        return 2

    def interrupt(signum, frame):
        raise InterruptedError("received signal " + str(signum))

    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, interrupt)

    try:
        # Scenario 1: Concurrent AB-BA multi-key live duplicates must not deadlock.
        q_abba = "idem_abba_" + run_id
        queues.append(q_abba)
        sql(f"select pgque.create_queue('{q_abba}');")
        seed = json.loads(sql(f"""
            with s1 as (
                select event_id from pgque.send_idem('{q_abba}', 'ev', '{{}}'::text, 'k1', interval '1 hour')
            ), s2 as (
                select event_id from pgque.send_idem('{q_abba}', 'ev', '{{}}'::text, 'k2', interval '1 hour')
            )
            select json_build_array((select event_id from s1), (select event_id from s2));
        """))
        k1_id, k2_id = seed

        app_a = "idem_abba_a_" + run_id
        app_b = "idem_abba_b_" + run_id
        sess_a = start(app_a, f"""
            begin;
            set local role pgque_writer;
            select json_build_array(event_id, deduped)
            from pgque.send_idem('{q_abba}', 'ev', '{{}}'::text, 'k1', interval '1 hour');
        """)
        sess_b = start(app_b, f"""
            begin;
            set local role pgque_writer;
            select json_build_array(event_id, deduped)
            from pgque.send_idem('{q_abba}', 'ev', '{{}}'::text, 'k2', interval '1 hour');
        """)

        barrier(f"""
            select pid from pg_stat_activity
            where application_name = '{app_a}'
              and state = 'idle in transaction'
              and query like '%send_idem%';
        """, "session A holds k1 live duplicate", sess_a)
        barrier(f"""
            select pid from pg_stat_activity
            where application_name = '{app_b}'
              and state = 'idle in transaction'
              and query like '%send_idem%';
        """, "session B holds k2 live duplicate", sess_b)

        # Now request the opposite key in both transactions simultaneously.
        sess_a.stdin.write(f"""
            select json_build_array(event_id, deduped)
            from pgque.send_idem('{q_abba}', 'ev', '{{}}'::text, 'k2', interval '1 hour');
            commit;
            \\q
        """)
        sess_a.stdin.flush()
        sess_b.stdin.write(f"""
            select json_build_array(event_id, deduped)
            from pgque.send_idem('{q_abba}', 'ev', '{{}}'::text, 'k1', interval '1 hour');
            commit;
            \\q
        """)
        sess_b.stdin.flush()

        rc_a, out_a, err_a = finish(sess_a, "", expect_ok=False)
        rc_b, out_b, err_b = finish(sess_b, "", expect_ok=False)
        if rc_a != 0 or rc_b != 0:
            failures += 1
            print(f"FAIL: AB-BA live duplicate failed: A(rc={rc_a}, err={err_a}) B(rc={rc_b}, err={err_b})",
                  flush=True)
        else:
            lines_a = [json.loads(line) for line in out_a.splitlines() if line.strip()]
            lines_b = [json.loads(line) for line in out_b.splitlines() if line.strip()]
            if lines_a != [[k1_id, True], [k2_id, True]] or lines_b != [[k2_id, True], [k1_id, True]]:
                failures += 1
                print(f"FAIL: AB-BA unexpected outputs: A={lines_a} B={lines_b}", flush=True)
            else:
                print("PASS: concurrent AB-BA multi-key live duplicates succeeded without deadlock", flush=True)

        # Scenario 2: Two open transactions both deduplicate a live key (holding
        # FOR KEY SHARE), wait for its TTL to expire, then both retry send_idem
        # before committing. Because ON CONFLICT DO UPDATE acquires FOR NO KEY
        # UPDATE (compatible with FOR KEY SHARE), there must be zero 40P01
        # upgrade deadlocks: one takes over and the second deduplicates to it.
        q_exp = "idem_exp_upgrade_" + run_id
        queues.append(q_exp)
        sql(f"select pgque.create_queue('{q_exp}');")
        seed_exp = json.loads(sql(f"""
            select json_build_array(event_id, deduped)
            from pgque.send_idem('{q_exp}', 'ev', '{{}}'::text, 'k-exp', interval '1 hour');
        """))
        orig_exp_id = seed_exp[0]

        app_u1 = "idem_upg_a_" + run_id
        app_u2 = "idem_upg_b_" + run_id
        u1 = start(app_u1, f"""
            begin;
            set local role pgque_writer;
            select json_build_array(event_id, deduped)
            from pgque.send_idem('{q_exp}', 'ev', '{{}}'::text, 'k-exp', interval '1 hour');
        """)
        u2 = start(app_u2, f"""
            begin;
            set local role pgque_writer;
            select json_build_array(event_id, deduped)
            from pgque.send_idem('{q_exp}', 'ev', '{{}}'::text, 'k-exp', interval '1 hour');
        """)
        barrier(f"""
            select pid from pg_stat_activity
            where application_name = '{app_u1}'
              and state = 'idle in transaction'
              and query like '%send_idem%';
        """, "session U1 holds FOR KEY SHARE on live k-exp", u1)
        barrier(f"""
            select pid from pg_stat_activity
            where application_name = '{app_u2}'
              and state = 'idle in transaction'
              and query like '%send_idem%';
        """, "session U2 holds FOR KEY SHARE on live k-exp", u2)

        # Expire the claim while both transactions remain open and hold FOR KEY SHARE.
        sql(f"""
            update pgque.idem as k
            set expires_at = clock_timestamp() - interval '1 second'
            from pgque.queue as q
            where q.queue_id = k.queue_id and q.queue_name = '{q_exp}'
              and k.idem_key = 'k-exp';
        """)

        # Now U1 retries (takes over with FOR NO KEY UPDATE without blocking on U2's
        # FOR KEY SHARE), while U2 also retries and commits after U1 commits.
        u1.stdin.write(f"""
            select json_build_array(event_id, deduped)
            from pgque.send_idem('{q_exp}', 'ev', '{{}}'::text, 'k-exp', interval '1 hour');
            commit;
            \\q
        """)
        u1.stdin.flush()
        u2.stdin.write(f"""
            select json_build_array(event_id, deduped)
            from pgque.send_idem('{q_exp}', 'ev', '{{}}'::text, 'k-exp', interval '1 hour');
            commit;
            \\q
        """)
        u2.stdin.flush()

        rc_u1, out_u1, err_u1 = finish(u1, "", expect_ok=False)
        rc_u2, out_u2, err_u2 = finish(u2, "", expect_ok=False)
        if rc_u1 != 0 or rc_u2 != 0:
            failures += 1
            print(f"FAIL: expired-retry upgrade deadlocked/failed: U1(rc={rc_u1}, err={err_u1}) U2(rc={rc_u2}, err={err_u2})",
                  flush=True)
        else:
            r1 = [json.loads(line) for line in out_u1.splitlines() if line.strip()]
            r2 = [json.loads(line) for line in out_u2.splitlines() if line.strip()]
            second_calls = [r1[1], r2[1]]
            new_inserts = [c for c in second_calls if c[1] is False]
            dedups = [c for c in second_calls if c[1] is True]
            if (r1[0] != [orig_exp_id, True] or r2[0] != [orig_exp_id, True]
                    or len(new_inserts) != 1 or len(dedups) != 1
                    or new_inserts[0][0] == orig_exp_id
                    or dedups[0][0] != new_inserts[0][0]):
                failures += 1
                print(f"FAIL: expired-retry unexpected outcomes: orig={orig_exp_id} U1={r1} U2={r2}", flush=True)
            else:
                print(f"PASS: expired retry by two open FOR KEY SHARE sessions succeeded without deadlock (new_id={new_inserts[0][0]})",
                      flush=True)

        # Scenario 3: drop_queue(q, true) racing an active idem row lock
        # (FOR KEY SHARE held by an open live-duplicate send_idem, which does
        # not lock pgque.queue) must fail fast with SQLSTATE 40001 on the
        # pgque.idem NOWAIT check instead of blocking or deadlocking.
        q_drop = "idem_drop_race_" + run_id
        queues.append(q_drop)
        sql(f"""
            select pgque.create_queue('{q_drop}');
            select * from pgque.send_idem('{q_drop}', 'ev', '{{}}'::text, 'k-drop', interval '1 hour');
        """)

        app_holder = "idem_drop_holder_" + run_id
        holder = start(app_holder, f"""
            begin;
            set local role pgque_writer;
            select event_id, deduped
            from pgque.send_idem('{q_drop}', 'ev', '{{}}'::text, 'k-drop', interval '1 hour');
        """)
        barrier(f"""
            select pid from pg_stat_activity
            where application_name = '{app_holder}'
              and state = 'idle in transaction'
              and query like '%send_idem%';
        """, "open duplicate holds idem FOR KEY SHARE lock", holder)

        drop_state = sql(f"""
            create temp table _drop_res (state text);
            do $$
            declare
                v_state text := 'OK';
            begin
                set local role pgque_admin;
                set local statement_timeout = '3s';
                begin
                    perform pgque.drop_queue('{q_drop}', true);
                exception when others then
                    v_state := sqlstate;
                end;
                reset role;
                insert into _drop_res values (v_state);
            end $$;
            select state from _drop_res;
        """)

        finish(holder, "commit;\n\\q\n")

        if drop_state != "40001":
            failures += 1
            print(f"FAIL: drop_queue(q, true) against active idem lock returned SQLSTATE {drop_state!r}, expected '40001'",
                  flush=True)
        else:
            print("PASS: drop_queue(q, true) failed fast with 40001 against active idem lock",
                  flush=True)

    except (RuntimeError, subprocess.SubprocessError, InterruptedError) as error:
        failures += 1
        print("FAIL: " + str(error), file=sys.stderr, flush=True)
    finally:
        try:
            if applications:
                names = ",".join("'" + name + "'" for name in applications)
                sql(f"""
                    select pg_terminate_backend(pid) from pg_stat_activity
                    where pid <> pg_backend_pid() and application_name in ({names});
                """)
            for process in processes:
                if process.poll() is None:
                    process.communicate(timeout=5)
            for queue in queues:
                sql(f"""
                    select pgque.drop_queue('{queue}', true)
                    where exists (select 1 from pgque.queue where queue_name = '{queue}');
                """)
            print("cleanup: no scoped queues remain", flush=True)
        except (RuntimeError, subprocess.SubprocessError) as error:
            failures += 1
            print("FAIL: cleanup: " + str(error), file=sys.stderr, flush=True)
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.communicate()

    if failures:
        print(f"FAIL: idempotency deadlock suite had {failures} violation(s)", file=sys.stderr)
        return 1
    print("PASS: idempotency live-duplicate concurrency and drop_queue NOWAIT guards verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
