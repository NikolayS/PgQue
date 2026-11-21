#!/usr/bin/env python3
"""Exercise the actual TTL harness barriers and their installed-SQL controls.

Run: PG_BINDIR=/path/to/postgres/bin python3 tests/test_idem_harness_barriers.py
No existing server or inherited libpq connection settings are used.
"""

import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from private_postgres import PrivatePostgres


PSQL_WRAPPER = r'''#!/usr/bin/env python3
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time

real = os.environ["BARRIER_REAL_PSQL"]
name = os.environ.get("PGAPPNAME", "")
control = Path(os.environ["BARRIER_CONTROL_DIR"])
if name.startswith("idem_ttl_contender_"):
    (control / "contender-delayed").write_text(name)
    time.sleep(3.2)
if not name.startswith("idem_claim_holder_"):
    os.execv(real, [real, *sys.argv[1:]])

process = subprocess.Popen([real, *sys.argv[1:]], stdin=subprocess.PIPE, text=True)
for line in sys.stdin:
    process.stdin.write(line)
    if line.strip().lower() == "begin;":
        # This client-side pause starts after PostgreSQL completes BEGIN.
        # The backend is idle in transaction, but has acquired no row lock.
        script = ("from pathlib import Path; import time; "
                  "p=Path(" + repr(str(control / name)) + "); "
                  "p.with_suffix('.paused').touch(); "
                  "deadline=time.monotonic()+10\n"
                  "while not p.with_suffix('.release').exists():\n"
                  " if time.monotonic()>deadline: raise SystemExit('pause timed out')\n"
                  " time.sleep(0.01)\n")
        # psql meta-commands occupy one physical line. repr keeps the Python
        # control script's newlines inside the -c argument instead of psql.
        process.stdin.write("\\! " + shlex.quote(sys.executable) + " -c " +
                            shlex.quote("exec(" + repr(script) + ")") + "\n")
    process.stdin.flush()
process.stdin.close()
sys.exit(process.wait())
'''


class TestIdemHarnessBarriers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pg = PrivatePostgres().__enter__()
        cls.addClassCleanup(cls.pg.__exit__, None, None, None)
        cls.repo = Path(__file__).resolve().parent.parent

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pgque-barrier-controls-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        wrapper = self.bin / "psql"
        wrapper.write_text(PSQL_WRAPPER)
        wrapper.chmod(0o700)
        self.database = "barrier_test_" + self._testMethodName.removeprefix("test_")
        self.pg.run("create database " + self.database)
        self.addCleanup(self.pg.run, "drop database " + self.database + " with (force)")
        self.pg.install(self.repo / "devel/sql/pgque.sql", self.database)
        self.env = dict(self.pg.environment(self.database),
                        BARRIER_REAL_PSQL=str(self.pg.bin / "psql"),
                        BARRIER_CONTROL_DIR=str(self.root))
        self.env["PATH"] = str(self.bin) + os.pathsep + self.env["PATH"]

    def wait_until(self, predicate, description, timeout=12):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(0.02)
        self.fail("timed out: " + description)

    def test_claim_ack_waits_for_conflict_statement(self):
        output = []
        process = subprocess.Popen(
            [sys.executable, str(self.repo / "tests/two_session_idem_claim_clock.py")],
            env=self.env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        reader = threading.Thread(target=lambda: output.extend(iter(process.stdout.readline, "")),
                                  daemon=True)
        reader.start()
        try:
            for index, scenario in enumerate(("insert_rollback", "delete_commit")):
                marker = self.wait_until(
                    lambda: next(iter(self.root.glob("idem_claim_holder_" + scenario + "_*.paused")), None),
                    scenario + " BEGIN pause")
                name = marker.stem
                observed = self.pg.query(
                    "select pid, state, lower(trim(query)) from pg_stat_activity "
                    f"where application_name = '{name}' and datname = current_database()",
                    self.database)
                self.assertRegex(observed, r"^\d+\|idle in transaction\|begin;$")
                holder_pid = int(observed.split("|", 1)[0])
                time.sleep(0.35)
                ready = [line for line in output if "conflict transaction is open" in line]
                self.assertEqual(index, len(ready),
                                 "holder readiness opened after BEGIN, before the conflict statement:\n" +
                                 observed + "\n" + "".join(output))
                marker.with_suffix(".release").touch()
                self.wait_until(lambda: any(scenario + " sender waits for this conflict transaction"
                                           in line for line in output), scenario + " sender blocker")
                blocked = self.pg.query(
                    "select pid from pg_stat_activity where application_name = "
                    f"'{name.replace('holder_', 'sender_', 1)}' and wait_event_type = 'Lock' "
                    f"and {holder_pid} = any(pg_blocking_pids(pid))", self.database)
                self.assertRegex(blocked, r"^\d+$", "sender must wait for the acknowledged holder")
                print(f"observed {scenario}: holder={holder_pid}, blocked_sender={blocked}", flush=True)
            self.assertEqual(0, process.wait(timeout=15))
            reader.join(timeout=2)
            self.assertEqual(2, sum(line.startswith("PASS:") for line in output), "".join(output))
            self.assertEqual("0", self.pg.query("select count(*) from pgque.queue", self.database))
            print("".join(output), end="", flush=True)
        finally:
            for marker in self.root.glob("*.paused"):
                marker.with_suffix(".release").touch()
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            reader.join(timeout=2)
            process.stdout.close()

    def run_takeover(self):
        result = subprocess.run(["bash", str(self.repo / "tests/two_session_idem_ttl.sh")],
                                env=self.env, capture_output=True, text=True, timeout=30)
        self.assertTrue((self.root / "contender-delayed").exists(), "startup-delay control did not run")
        print(result.stdout + result.stderr, end="", flush=True)
        self.assertEqual("0", self.pg.query("select count(*) from pgque.queue", self.database),
                         "harness left owned queues after success or its expected failure")
        self.assertEqual("0", self.pg.query(
            "select count(*) from pg_stat_activity where datname = current_database() "
            "and application_name like 'idem_ttl_%'", self.database),
            "harness left a holder or contender backend")
        return result

    def test_takeover_delay_rejects_stale_clock(self):
        # Mutate only the effective installed function in this owned database.
        definition = self.pg.query("select pg_get_functiondef("
            "'pgque.send_idem(text,text,text,text,interval,text)'::regprocedure)", self.database)
        self.assertGreaterEqual(definition.count("clock_timestamp()"), 3)
        stale = definition.replace("clock_timestamp()", "statement_timestamp()")
        self.pg.run(stale, self.database)
        result = self.run_takeover()
        self.assertNotEqual(0, result.returncode,
                            "stale statement clock falsely passed after delayed contender startup")
        self.assertIn("takeover TTL was not measured from lock acquisition", result.stderr)
        self.assertNotIn("did not wait on", result.stderr)
        self.assertNotIn("did not reach its lock barrier", result.stderr)

    def test_takeover_delay_accepts_fresh_clock(self):
        result = self.run_takeover()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("PASS: idempotency takeover TTL", result.stdout)
        match = re.search(r"observed_wait_seconds=([0-9.]+)", result.stdout)
        self.assertIsNotNone(match, "release must report its observed contender wait")
        self.assertGreater(float(match[1]), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
