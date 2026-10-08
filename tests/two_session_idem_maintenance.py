#!/usr/bin/env python3
"""Prove cleanup rechecks expiry after waiting for an idempotency takeover.

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

    def finish(process, tail="\\q\n"):
        output, error = process.communicate(tail, timeout=20)
        if process.returncode:
            raise RuntimeError(error.strip() or "psql backend failed")
        return output.strip()

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
        for scope in ("scoped", "global"):
            queue = "idem_maintenance_" + scope + "_" + run_id
            queues.append(queue)
            sender_app = "idem_gc_sender_" + scope + "_" + run_id
            maint_app = "idem_gc_maint_" + scope + "_" + run_id
            sql(f"""
                select pgque.create_queue('{queue}');
                select * from pgque.send_idem(
                    '{queue}', 'seed', '{{}}'::text, 'takeover', interval '1 hour');
                select * from pgque.send_idem(
                    '{queue}', 'expired', '{{}}'::text, 'expired-control', interval '1 hour');
                update pgque.idem as k
                set expires_at = clock_timestamp() - interval '1 second'
                from pgque.queue as q
                where q.queue_id = k.queue_id and q.queue_name = '{queue}';
            """)

            # The live replacement is uncommitted. Cleanup's statement snapshot
            # sees the expired predecessor and must wait on this writer's row.
            sender = start(sender_app, f"""
                begin;
                set local role pgque_writer;
                select json_build_array(event_id, deduped)
                from pgque.send_idem(
                    '{queue}', 'renewed', '{{}}'::text, 'takeover', interval '1 hour');
            """)
            sender_pid = barrier(f"""
                select pid from pg_stat_activity
                where application_name = '{sender_app}'
                    and state = 'idle in transaction'
                    and query like '%send_idem%';
            """, scope + " takeover completed with its transaction still open", sender)

            operation = f"pgque.maint_idem('{queue}')" if scope == "scoped" else "pgque.maint_idem()"
            maintenance = start(maint_app, f"set role pgque_admin;\nselect {operation};")
            barrier(f"""
                select pid from pg_stat_activity
                where application_name = '{maint_app}'
                    and wait_event_type = 'Lock'
                    and {sender_pid} = any(pg_blocking_pids(pid));
            """, scope + " cleanup blocked by the takeover writer", maintenance)

            takeover_id, deduped = json.loads(finish(sender, "commit;\n\\q\n"))
            if deduped:
                raise RuntimeError(scope + " fixture did not replace the expired claim")
            finish(maintenance)

            claim = sql(f"""
                select json_build_array(k.event_id, k.expires_at > clock_timestamp() + interval '30 minutes')
                from pgque.idem as k
                join pgque.queue as q on q.queue_id = k.queue_id
                where q.queue_name = '{queue}' and k.idem_key = 'takeover';
            """)
            expired_count = int(sql(f"""
                select count(*) from pgque.idem as k
                join pgque.queue as q on q.queue_id = k.queue_id
                where q.queue_name = '{queue}' and k.idem_key = 'expired-control';
            """))
            retry = json.loads(sql(f"""
                set role pgque_writer;
                select json_build_array(event_id, deduped)
                from pgque.send_idem(
                    '{queue}', 'retry', '{{}}'::text, 'takeover', interval '1 hour');
            """))
            if (not claim or json.loads(claim) != [takeover_id, True]
                    or retry != [takeover_id, True] or expired_count != 0):
                failures += 1
                print(f"FAIL: {scope}: takeover={takeover_id}, claim={claim or 'missing'}, "
                      f"immediate_retry={retry}, expired_controls={expired_count}", flush=True)
            else:
                print(f"PASS: {scope}: cleanup retained live claim {takeover_id}, "
                      "immediate retry deduplicated, expired control removed", flush=True)
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
            if queues:
                names = ",".join("'" + queue + "'" for queue in queues)
                residue = int(sql(f"select count(*) from pgque.queue where queue_name in ({names});"))
                if residue:
                    raise RuntimeError("cleanup left " + str(residue) + " scoped queues")
            print("cleanup: no scoped queues remain", flush=True)
        except (RuntimeError, subprocess.SubprocessError) as error:
            failures += 1
            print("FAIL: cleanup: " + str(error), file=sys.stderr, flush=True)
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.communicate()
    if failures:
        print(f"FAIL: idempotency cleanup has {failures} violation(s)", file=sys.stderr)
        return 1
    print("PASS: both maintenance entry points recheck expiry after a takeover lock wait")
    return 0


if __name__ == "__main__":
    sys.exit(main())
