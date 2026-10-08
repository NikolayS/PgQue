#!/usr/bin/env python3
"""No-database process spies for the non-superuser harness boundary.

Run this before test_nonsuperuser_cleanup.py. The fake PostgreSQL executables
only record calls; they never load libpq or connect to any server.
"""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import test_nonsuperuser_cleanup as cleanup


SPY = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import re
import sys

args = sys.argv[1:]
command = Path(sys.argv[0]).name
sql = ""
for i, arg in enumerate(args[:-1]):
    if arg in ("-f", "--file"):
        sql += Path(args[i + 1]).read_text()
    elif arg == "--command" or re.fullmatch(r"-[qAt]*c", arg):
        sql += args[i + 1]
with open(os.environ["NSU_SPY_LOG"], "a") as log:
    log.write(json.dumps({"command": command, "args": args, "sql": sql,
                          "env": {k: v for k, v in os.environ.items()
                                  if k.startswith("PG")}}) + "\n")
if command == "initdb":
    Path(args[args.index("-D") + 1]).mkdir()
elif command == "pg_ctl":
    pid = Path(args[args.index("-D") + 1]) / "postmaster.pid"
    if "start" in args:
        pid.touch()
    elif "stop" in args:
        pid.unlink()
elif command == "psql":
    if sql.startswith("drop ") and os.environ.get("NSU_SPY_FAIL_DROP", "!") in sql:
        sys.exit(17)
    if "55_negctl_post.sql" in " ".join(args) and os.environ.get("NSU_SPY_FAIL_BODY"):
        sys.exit(19)
'''


class ProcessSpyCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pgque-nsu-spy-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        for command in ("initdb", "pg_ctl", "psql"):
            path = self.bin / command
            path.write_text(SPY)
            path.chmod(0o700)
        self.log = self.root / "calls.jsonl"
        clean = {key: value for key, value in os.environ.items()
                 if not key.startswith("PG")}
        self.env = dict(clean, NSU_SPY_LOG=str(self.log),
                        PATH=str(self.bin) + os.pathsep + os.environ["PATH"])

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()]


class TestPrivateEnvironment(ProcessSpyCase):
    def test_all_children_strip_libpq_inputs_and_pin_private_socket_port(self):
        poison = {
            "PGHOST": "outside.invalid", "PGHOSTADDR": "192.0.2.1",
            "PGPORT": "1", "PGSERVICE": "outside",
            "PGSERVICEFILE": "/nonexistent/service",
            "PGSYSCONFDIR": "/nonexistent/config",
            "PGOPTIONS": "-c plpgsql.check_asserts=off",
            "PGDATABASE": "outside", "PGUSER": "outside",
            "PGPASSWORD": "not-a-secret", "PGPASSFILE": "/nonexistent/pass",
            "PGSSLKEY": "/nonexistent/key", "PGSSLMODE": "require",
            "PGTARGETSESSIONATTRS": "read-only",
            "PGQUE_TEST_SUPERUSER_DSN": "host=outside.invalid",
        }

        class Fixture(cleanup.TestNonsuperuserCleanup):
            pass

        with mock.patch.dict(os.environ, dict(self.env, **poison,
                                              PG_BINDIR=str(self.bin))):
            try:
                Fixture.setUpClass()
                Fixture().run_harness("none")
                expected_dsn = Fixture.dsn
                expected_socket = str(Fixture.socket)
            finally:
                Fixture.doClassCleanups()
        calls = self.calls()
        self.assertEqual({"initdb", "pg_ctl", "psql"},
                         {call["command"] for call in calls})
        psql_calls = [call for call in calls if call["command"] == "psql"]
        self.assertGreater(len(psql_calls), 20)
        # Includes wrapper subprocesses, exec'd psql, and the final snapshot.
        for call in calls:
            with self.subTest(command=call["command"], args=call["args"]):
                self.assertEqual({}, {k: v for k, v in call["env"].items()
                                      if k != "PGQUE_TEST_SUPERUSER_DSN"})
                if call["command"] == "psql":
                    self.assertTrue(any(argument in (
                        expected_dsn,
                        expected_dsn + " options='-c plpgsql.check_asserts=off'",
                    ) for argument in call["args"]), call["args"])
        start = next(call for call in calls
                     if call["command"] == "pg_ctl" and "start" in call["args"])
        options = start["args"][start["args"].index("-o") + 1]
        self.assertIn("listen_addresses=''", options)
        self.assertIn(f"unix_socket_directories='{expected_socket}'", options)
        self.assertRegex(expected_dsn, r"(?:^| )port=\d+(?: |$)")
        port = next(item.split("=", 1)[1] for item in expected_dsn.split()
                    if item.startswith("port="))
        self.assertIn(f"-p {port}", options)


class TestCleanupExit(ProcessSpyCase):
    def run_case(self, body_failure=False, drop_failure=None, body_status=None):
        harness = Path(cleanup.__file__).with_name("security_nonsuperuser_install.sh")
        if body_status is not None:
            replacement = self.root / "tests" / harness.name
            replacement.parent.mkdir()
            source = harness.read_text()
            marker = "grep -h 'NOTICE:.*PASS'"
            self.assertEqual(1, source.count(marker), "mutation must match exactly once")
            replacement.write_text(source.replace(marker, f"exit {body_status}\n\n{marker}"))
            harness = replacement
        env = dict(self.env, PGQUE_TEST_SUPERUSER_DSN="dbname=spy-only")
        if body_failure:
            env["NSU_SPY_FAIL_BODY"] = "1"
        if drop_failure:
            env["NSU_SPY_FAIL_DROP"] = drop_failure
        result = subprocess.run(["bash", str(harness)], env=env,
                                capture_output=True, text=True, timeout=20)
        drops = [call["sql"] for call in self.calls() if call["sql"].startswith("drop ")]
        self.assertEqual(6, len(drops), result.stdout + result.stderr)
        self.assertEqual(2, sum(sql.startswith("drop database ") for sql in drops))
        self.assertEqual(4, sum(sql.startswith("drop role ") for sql in drops))
        return result, drops

    def test_success_requires_successful_cleanup(self):
        result, _ = self.run_case()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("PASS: security_nonsuperuser_install", result.stdout)

    def test_failed_database_drop_fails_body_success_and_attempts_all_drops(self):
        result, drops = self.run_case(drop_failure="pgque_nsu_main_")
        self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertNotIn("PASS: security_nonsuperuser_install", result.stdout)
        self.assertIn(drops[0].split()[4], result.stderr)

    def test_failed_role_drop_fails_body_success_and_attempts_later_drops(self):
        result, _ = self.run_case(drop_failure="pgque_nsu_writer_")
        self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertNotIn("PASS: security_nonsuperuser_install", result.stdout)

    def test_body_failure_survives_successful_cleanup(self):
        result, _ = self.run_case(body_failure=True)
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertNotIn("PASS: security_nonsuperuser_install", result.stdout)

    def test_body_failure_survives_failed_cleanup(self):
        result, _ = self.run_case(body_failure=True, drop_failure="pgque_nsu_main_")
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)

    def test_original_nonstandard_failure_status_survives_failed_cleanup(self):
        result, _ = self.run_case(body_status=23, drop_failure="pgque_nsu_main_")
        self.assertEqual(23, result.returncode, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
