#!/usr/bin/env python3
"""Test cooperative expiry in one READ COMMITTED transaction.

Requires psql and PGQUE_TEST_DSN for a disposable installed test database.
Barriers use completed psql acknowledgments and the exact blocking backend.
"""
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path


def main():
    dsn = os.environ.get("PGQUE_TEST_DSN")
    if not dsn:
        print("PGQUE_TEST_DSN is required", file=sys.stderr)
        return 2
    env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    command = ["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "--dbname=" + dsn]
    prefix = "coop_dead_clock_" + uuid.uuid4().hex[:12]
    processes, queues, failures, applications = [], [], [], []

    def sql(statement):
        p = subprocess.run(command, input="set statement_timeout='15s';\n" + statement,
                           text=True, capture_output=True, env=env, timeout=20)
        if p.returncode:
            raise RuntimeError(p.stderr.strip())
        return p.stdout.strip()

    database = sql("select current_database();")
    if not re.search(r"(^|_)test($|_)", database):
        print("FAIL: test database name required", file=sys.stderr)
        return 2

    def wait_for(check, label):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if check():
                print("barrier: " + label, flush=True)
                return
            time.sleep(0.025)
        raise RuntimeError("barrier timed out: " + label)

    with tempfile.TemporaryDirectory(prefix=prefix) as tmp:
        def start(name, statement):
            path = Path(tmp) / name
            out = path.open("w+")
            p = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=out,
                                 stderr=subprocess.PIPE, text=True,
                                 env={**env, "PGAPPNAME": prefix + "_" + name})
            processes.append((p, out))
            applications.append(prefix + "_" + name)
            send(p, statement + "\n\\echo READY\n")
            wait_for(lambda: "READY\n" in path.read_text(), name + " completed statements")
            rows = path.read_text().splitlines()
            return p, path, json.loads(rows[0])

        def send(p, statement):
            p.stdin.write(statement + "\n")
            p.stdin.flush()

        def finish(p, tail="commit;"):
            _, error = p.communicate(tail + "\n\\q\n", timeout=20)
            if p.returncode:
                raise RuntimeError(error.strip())

        try:
            for scenario in ("elapsed", "lock_wait", "active_renewal", "disabled"):
                queue = prefix + "_" + scenario
                queues.append(queue)
                sql(f"""
                    select pgque.create_queue('{queue}');
                    select pgque.register_subconsumer('{queue}', 'main', 'old');
                    select pgque.register_subconsumer('{queue}', 'main', 'new');
                    select pgque.send('{queue}', 'clock', 'pending');
                    select pgque.force_next_tick('{queue}');
                    select pgque.ticker('{queue}');
                """)
                old = json.loads(sql(f"""
                    select json_build_object('status', status, 'token', page_token,
                        'batch', batch_id, 'number', page_number,
                        'event', ((messages)[1]).msg_id, 'payload', ((messages)[1]).payload)
                    from pgque.receive_page_coop('{queue}', 'main', 'old', 'old-worker',
                        1, interval '2 seconds', interval '2 seconds');
                """))
                if old['status'] != 'page':
                    raise RuntimeError("fixture did not issue a committed page")
                receiver, path, identity = start(scenario + "_receiver", """
                    set statement_timeout='15s';
                    begin isolation level read committed;
                    select json_build_array(pg_backend_pid(), now(), current_setting('transaction_isolation'));
                """)
                if identity[2] != 'read committed':
                    raise RuntimeError("fixture isolation is not READ COMMITTED")
                fresh = sql(f"""
                    select sub_active > '{identity[1]}'::timestamptz - interval '2 seconds'
                        and pending_lease_until > '{identity[1]}'::timestamptz
                    from pgque.subscription s join pgque.page_state p
                      on p.queue_id = s.sub_queue and p.consumer_id = s.sub_consumer
                    where p.pending_token = '{old['token']}';
                """)
                if fresh != 't':
                    raise RuntimeError("victim must be fresh when receiver transaction begins")
                holder = None
                if scenario in ('lock_wait', 'active_renewal'):
                    holder, _, holder_identity = start(scenario + "_holder", f"""
                        set statement_timeout='15s'; begin;
                        select json_build_array(pg_backend_pid());
                        select 1 from pgque.subscription s join pgque.queue q on q.queue_id=s.sub_queue
                        where q.queue_name='{queue}' and s.sub_role='coop_main' for update of s;
                    """)
                dead = "null" if scenario == 'disabled' else "interval '2 seconds'"
                receive = f"""
                    select json_build_object('status', p.status, 'token', p.page_token,
                        'batch', p.batch_id, 'number', p.page_number,
                        'event', ((p.messages)[1]).msg_id, 'payload', ((p.messages)[1]).payload,
                        'same_transaction', now() = '{identity[1]}'::timestamptz)
                    from pgque.receive_page_coop('{queue}', 'main', 'new', 'new-worker',
                        9, {dead}, interval '2 seconds') p;
                """
                if holder:
                    send(receiver, receive)
                    wait_for(lambda: sql(f"select {holder_identity[0]} = any(pg_blocking_pids({identity[0]}));") == 't',
                             scenario + " receiver blocked by main-lock holder")
                wait_for(lambda: sql(f"""
                    select sub_active < clock_timestamp() - interval '2 seconds'
                        and pending_lease_until < clock_timestamp()
                    from pgque.subscription s join pgque.page_state p
                      on p.queue_id=s.sub_queue and p.consumer_id=s.sub_consumer
                    where p.pending_token='{old['token']}';
                """) == 't', scenario + " both victim thresholds expired")
                if scenario == 'active_renewal':
                    sql(f"select pgque.renew_page('{old['token']}', 'old-worker');")
                    if sql(f"select pending_lease_until > clock_timestamp() from pgque.page_state where pending_token='{old['token']}';") != 't':
                        raise RuntimeError("renewal control did not commit a live lease")
                if holder:
                    finish(holder)
                else:
                    send(receiver, receive)
                finish(receiver)
                result = json.loads(path.read_text().splitlines()[-1])
                should_transfer = scenario in ('elapsed', 'lock_wait')
                passed = result['same_transaction'] and (
                    result['status'] == 'page' and result['token'] != old['token']
                    and result['batch'] != old['batch'] and result['number'] == old['number']
                    and result['event'] == old['event'] and result['payload'] == old['payload']
                    if should_transfer else result['status'] == 'idle')
                if passed and should_transfer:
                    sql(f"""do $$ begin
                        begin
                            perform pgque.ack_page('{old['token']}', 'old-worker');
                            raise exception 'old token unexpectedly accepted';
                        exception when sqlstate 'PQP01' then null;
                        end;
                    end $$;""")
                if passed and not should_transfer:
                    passed = sql(f"select count(*) from pgque.page_state where pending_token='{old['token']}' and active_batch_id={old['batch']};") == '1'
                print(('PASS: ' if passed else 'FAIL: ') + scenario + ': ' + json.dumps(result), flush=True)
                if not passed:
                    failures.append(scenario)
        except (RuntimeError, subprocess.SubprocessError, OSError) as error:
            failures.append('fixture')
            print('FAIL: fixture: ' + str(error), file=sys.stderr)
        finally:
            try:
                # A fixture failure may leave the receiver waiting on the holder.
                # End only these uniquely named backends before dropping queues.
                if applications:
                    names = ",".join("'" + name + "'" for name in applications)
                    sql(f"select pg_terminate_backend(pid) from pg_stat_activity "
                        f"where application_name in ({names}) and pid <> pg_backend_pid();")
                for p, out in processes:
                    if p.poll() is None:
                        p.communicate(timeout=20)
                    out.close()
                for queue in queues:
                    sql(f"select pgque.drop_queue('{queue}', true);")
                print('cleanup: scoped queues removed', flush=True)
            except (RuntimeError, subprocess.SubprocessError) as error:
                failures.append('cleanup')
                print('FAIL: cleanup: ' + str(error), file=sys.stderr)
    return int(bool(failures))


if __name__ == '__main__':
    sys.exit(main())
