#!/usr/bin/env python3
"""Check new-claim TTL after a conflicting insert rolls back or delete commits.

Uses only psql and Python's standard library. PGQUE_TEST_DSN must select an
installed disposable test database. Both waits use observed backend blockers.
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
from decimal import Decimal


def main():
    dsn = os.environ.get("PGQUE_TEST_DSN")
    if not dsn:
        print("PGQUE_TEST_DSN is required", file=sys.stderr)
        return 2
    command = ["psql", "--no-psqlrc", "--quiet", "--no-align", "--tuples-only",
               "--set=ON_ERROR_STOP=1", "--dbname=" + dsn]
    env = {**os.environ, "PAGER": "cat"}
    run_id = uuid.uuid4().hex[:16]
    queues, applications, processes, failures = [], [], [], []

    def sql(statement):
        result = subprocess.run(command, input="set statement_timeout='15s';\n" + statement,
                                text=True, capture_output=True, timeout=20, env=env)
        if result.returncode:
            raise RuntimeError(result.stderr.strip() or "psql failed")
        return result.stdout.strip()

    # Validate the target before installing any mutating cleanup handler.
    database = sql("select current_database();")
    if (not re.search(r"(^|_)test($|_)", database)
            and os.environ.get("PGQUE_ALLOW_TEST_MUTATION") != "1"):
        print("FAIL: refusing concurrency test mutations in database " + repr(database),
              file=sys.stderr)
        return 2

    def start(name, statement):
        applications.append(name)
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True,
                                   env={**env, "PGAPPNAME": name})
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

    def interrupt(signum, frame):
        raise InterruptedError("received signal " + str(signum))

    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, interrupt)

    try:
        for scenario in ("insert_rollback", "delete_commit"):
            queue = "idem_claim_clock_" + scenario + "_" + run_id
            queues.append(queue)
            holder_name = "idem_claim_holder_" + scenario + "_" + run_id
            sender_name = "idem_claim_sender_" + scenario + "_" + run_id
            sql(f"""
                select pgque.create_queue('{queue}');
                select * from pgque.send_idem(
                    '{queue}', 'warmup', '{{}}'::text, 'warmup', interval '1 hour');
            """)
            if scenario == "delete_commit":
                sql(f"select * from pgque.send_idem('{queue}', 'old', '{{}}'::text, 'claim', interval '1 hour');")
                hold = f"""
                    delete from pgque.idem as k using pgque.queue as q
                    where k.queue_id = q.queue_id and q.queue_name = '{queue}'
                        and k.idem_key = 'claim';
                """
                release = "commit;"
            else:
                hold = f"""
                    set local role pgque_writer;
                    select * from pgque.send_idem(
                        '{queue}', 'rollback', '{{}}'::text, 'claim', interval '1 hour');
                """
                release = "rollback;"
            holder = start(holder_name, "begin;\n" + hold)
            holder_pid = barrier(f"""
                select pid from pg_stat_activity
                where application_name = '{holder_name}' and state = 'idle in transaction';
            """, scenario + " conflict transaction is open", holder)
            claim_read = f"""
                from pgque.idem as k join pgque.queue as q on q.queue_id = k.queue_id
                where q.queue_name = '{queue}' and k.idem_key = 'claim'
            """
            sender = start(sender_name, f"""
                begin;
                set local role pgque_writer;
                select json_build_array(event_id, deduped) from pgque.send_idem(
                    '{queue}', 'new', '{{}}'::text, 'claim', interval '2 seconds');
                reset role;
                select json_build_array(extract(epoch from k.expires_at),
                    extract(epoch from clock_timestamp())) {claim_read};
                set local role pgque_writer;
                select json_build_array(event_id, deduped) from pgque.send_idem(
                    '{queue}', 'duplicate', '{{}}'::text, 'claim', interval '2 seconds');
                reset role;
                select to_json(extract(epoch from k.expires_at)) {claim_read};
                commit;
            """)
            barrier(f"""
                select pid from pg_stat_activity
                where application_name = '{sender_name}' and wait_event_type = 'Lock'
                    and {holder_pid} = any(pg_blocking_pids(pid));
            """, scenario + " sender waits for this conflict transaction", sender)
            # Age the evaluated INSERT value only after observing its lock wait.
            sql("select pg_sleep(2.25);")
            released_after = Decimal(sql("select extract(epoch from clock_timestamp());"))
            finish(holder, release + "\n\\q\n")
            lines = finish(sender).splitlines()
            if len(lines) != 4:
                raise RuntimeError(scenario + ": unexpected sender output " + repr(lines))
            first, window, duplicate, final_expiry = [json.loads(x, parse_float=Decimal) for x in lines]
            expires_at, completed_at = window
            fresh = released_after + 2 <= expires_at <= completed_at + 2
            deduplicated = first[1] is False and duplicate == [first[0], True]
            unchanged = final_expiry == expires_at
            if not (fresh and deduplicated and unchanged):
                failures.append(scenario)
                print(f"FAIL: {scenario}: fresh claim TTL must start after unique-key wait; "
                      f"expiry_minus_release={expires_at-released_after}, "
                      f"first={first}, immediate_retry={duplicate}, "
                      f"dedup_preserves_expiry={unchanged}", flush=True)
            else:
                print(f"PASS: {scenario}: fresh TTL starts after conflict ends; "
                      "immediate retry returns the same event without extending expiry", flush=True)
    except (RuntimeError, subprocess.SubprocessError, InterruptedError) as error:
        failures.append("fixture")
        print("FAIL: fixture: " + str(error), file=sys.stderr, flush=True)
    finally:
        try:
            if applications:
                names = ",".join("'" + name + "'" for name in applications)
                sql(f"select pg_terminate_backend(pid) from pg_stat_activity where pid <> pg_backend_pid() and application_name in ({names});")
            for process in processes:
                if process.poll() is None:
                    process.communicate(timeout=5)
            for queue in queues:
                sql(f"select pgque.drop_queue('{queue}', true) where exists (select 1 from pgque.queue where queue_name = '{queue}');")
            if queues:
                names = ",".join("'" + queue + "'" for queue in queues)
                if int(sql(f"select count(*) from pgque.queue where queue_name in ({names});")):
                    raise RuntimeError("scoped queues remain")
            print("cleanup: no scoped queues remain", flush=True)
        except (RuntimeError, subprocess.SubprocessError) as error:
            failures.append("cleanup")
            print("FAIL: cleanup: " + str(error), file=sys.stderr, flush=True)
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.communicate()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
