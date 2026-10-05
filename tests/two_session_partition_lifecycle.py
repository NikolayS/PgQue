#!/usr/bin/env python3
"""Check atomic setup after teardown and fail-fast administrative force-drop.

Uses psql and Python's standard library. Row locks and pg_blocking_pids control
all interleavings; sleeps only poll backend state in a disposable test database.
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
               "--set=ON_ERROR_STOP=1", "--set=VERBOSITY=verbose", "--dbname=" + dsn]
    env = {**os.environ, "PAGER": "cat"}
    run_id = uuid.uuid4().hex[:16]
    queues, actors, failures = [], [], []

    def sql(statement):
        result = subprocess.run(command, input="set statement_timeout='20s';\n" + statement,
                                text=True, capture_output=True, timeout=25, env=env)
        if result.returncode:
            raise RuntimeError(result.stderr.strip() or "psql failed")
        return result.stdout.strip()

    database = sql("select current_database();")
    if (not re.search(r"(^|_)test($|_)", database)
            and os.environ.get("PGQUE_ALLOW_TEST_MUTATION") != "1"):
        print("FAIL: refusing concurrency test mutations in database " + repr(database),
              file=sys.stderr)
        return 2

    def send(process, statement):
        process.stdin.write(statement + "\n")
        process.stdin.flush()

    def start(label, statement):
        name = "lifecycle_" + label + "_" + run_id
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True,
                                   env={**env, "PGAPPNAME": name})
        actors.append((name, process))
        send(process, "set statement_timeout='20s'; set deadlock_timeout='5s';\n" + statement)
        return name, process

    def finish(process):
        output, error = process.communicate(timeout=25)
        print(json.dumps({"exit": process.returncode, "stdout": output.strip(),
                          "stderr": error.strip()}), flush=True)
        return process.returncode, output.strip(), error.strip()

    def observe(statement, description, process):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            value = sql(statement)
            if value:
                print("barrier: " + description + " = " + value, flush=True)
                return value
            if process.poll() is not None:
                _, _, error = finish(process)
                raise RuntimeError(description + ": backend exited early: " + error)
            time.sleep(0.05)
        raise RuntimeError(description + ": observed-state barrier timed out")

    def idle(name, process):
        return int(observe(f"""
            select pid from pg_stat_activity
            where application_name='{name}' and state='idle in transaction';
        """, name + " open transaction", process))

    def setup(label):
        queue = "lifecycle_" + label + "_" + run_id
        queues.append(queue)
        sql(f"select pgque.create_queue('{queue}');")
        sql(f"select pgque.subscribe_partitioned('{queue}', 'workers', 2);")
        return queue

    def counts(queue):
        return json.loads(sql(f"""
            select json_build_array(
                (select count(*) from pgque.queue where queue_name='{queue}'),
                (select count(*) from pgque.partition_consumer pc join pgque.queue q
                    on q.queue_id=pc.queue_id where q.queue_name='{queue}'),
                (select count(*) from pgque.partition_slot ps join pgque.queue q
                    on q.queue_id=ps.queue_id where q.queue_name='{queue}'),
                (select count(*) from pgque.subscription s join pgque.queue q
                    on q.queue_id=s.sub_queue where q.queue_name='{queue}')
            );
        """))

    def hold_parent(queue, label):
        name, process = start(label, f"""
            begin;
            select pc.n from pgque.partition_consumer pc join pgque.queue q
            on q.queue_id=pc.queue_id
            where q.queue_name='{queue}' and pc.co_name='workers'
            for no key update of pc;
        """)
        return process, idle(name, process)

    def setup_after_teardown():
        queue = setup("setup")
        assert counts(queue) == [1, 1, 2, 2], "fixture needs a complete two-slot consumer"
        holder, holder_pid = hold_parent(queue, "setup_holder")
        name, contender = start("setup_contender", f"""
            begin;
            select pgque.subscribe_partitioned('{queue}', 'workers', 2);
            commit;
            \\q
        """)
        observe(f"""
            select pid from pg_stat_activity where application_name='{name}'
                and {holder_pid}=any(pg_blocking_pids(pid));
        """, "setup waits on the existing parent row", contender)
        send(holder, f"""
            select pgque.unsubscribe_partitioned('{queue}', 'workers');
            commit;
            \\q
        """)
        status, _, error = finish(holder)
        assert status == 0, "teardown failed: " + error
        status, _, error = finish(contender)
        if status:
            assert "P0001" in error and "incomplete setup (0 of 2 slots" in error, error
            assert counts(queue) == [1, 0, 0, 0], "teardown must have committed"
            raise AssertionError("setup reported incomplete state after the consumer was removed")
        assert counts(queue) == [1, 1, 2, 2], "setup must recreate all slots atomically"
        assert sql(f"""
            select count(distinct s.sub_last_tick) from pgque.subscription s
            join pgque.queue q on q.queue_id=s.sub_queue where q.queue_name='{queue}';
        """) == "1", "recreated slots must share one start tick"
        print("PASS: setup retries a vanished parent and recreates complete shared-cursor state", flush=True)

    def force_drop_during_teardown():
        queue = setup("drop")
        before = counts(queue)
        assert before == [1, 1, 2, 2], "fixture needs a complete two-slot consumer"
        holder, holder_pid = hold_parent(queue, "drop_holder")
        name, dropper = start("dropper", "begin;")
        dropper_pid = idle(name, dropper)
        send(dropper, f"select pgque.drop_queue('{queue}', true); commit;\n\\q")
        deadline, blocked = time.monotonic() + 10, False
        while time.monotonic() < deadline:
            if dropper.poll() is not None:
                break
            blocked = sql(f"select {holder_pid}=any(pg_blocking_pids({dropper_pid}));") == "t"
            if blocked:
                print("barrier: force-drop waits on teardown's parent row", flush=True)
                break
            time.sleep(0.05)
        else:
            raise RuntimeError("force-drop neither completed nor reached the observed parent barrier")

        if blocked:
            send(holder, f"""
                select pgque.unsubscribe_partitioned('{queue}', 'workers');
                commit;
                \\q
            """)
            observe(f"""
                select 'parent/slot lock cycle'
                where {holder_pid}=any(pg_blocking_pids({dropper_pid}))
                    and {dropper_pid}=any(pg_blocking_pids({holder_pid}));
            """, "actual teardown and force-drop form a wait cycle", holder)
            holder_result = finish(holder)
            drop_result = finish(dropper)
            assert "40P01" in holder_result[2] + drop_result[2], "expected a diagnosed deadlock"
            raise AssertionError("force-drop entered a parent/slot deadlock instead of returning 40001")

        status, _, error = finish(dropper)
        assert status != 0 and "40001" in error, "busy force-drop must return 40001: " + error
        assert counts(queue) == before, "rejected force-drop must leave lifecycle state unchanged"
        send(holder, f"""
            select pgque.unsubscribe_partitioned('{queue}', 'workers');
            commit;
            \\q
        """)
        status, _, error = finish(holder)
        assert status == 0, "teardown failed after rejected force-drop: " + error
        assert counts(queue) == [1, 0, 0, 0], "teardown must remove the complete consumer"
        assert sql(f"select pgque.drop_queue('{queue}', true);") == "1"
        assert counts(queue) == [0, 0, 0, 0], "force-drop retry must remove the queue"
        print("PASS: busy force-drop returns 40001 without changes; teardown and retry succeed", flush=True)

    def interrupt(signum, frame):
        raise InterruptedError("received signal " + str(signum))

    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, interrupt)
    try:
        for test in (setup_after_teardown, force_drop_during_teardown):
            try:
                test()
            except (AssertionError, RuntimeError) as error:
                failures.append(test.__name__ + ": " + str(error))
                print("FAIL: " + failures[-1], file=sys.stderr, flush=True)
    finally:
        for name, process in actors:
            if process.poll() is None:
                sql(f"""select pg_terminate_backend(pid) from pg_stat_activity
                    where datname=current_database() and application_name='{name}';""")
                try:
                    process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate()
        for queue in queues:
            sql(f"""select pgque.drop_queue('{queue}', true)
                where exists (select 1 from pgque.queue where queue_name='{queue}');""")
        remaining = sql(f"""select count(*) from pg_stat_activity
            where datname=current_database() and application_name like 'lifecycle_%_{run_id}';""")
        assert remaining == "0", "lifecycle test left a backend alive"
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
