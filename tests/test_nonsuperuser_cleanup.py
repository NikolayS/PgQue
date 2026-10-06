#!/usr/bin/env python3
"""Exercise fixture collisions against a private PostgreSQL cluster.

Run: PG_BINDIR=/path/to/postgres/bin python3 tests/test_nonsuperuser_cleanup.py
No existing server or PGQUE_TEST_SUPERUSER_DSN is used. Requires a non-root
user and local initdb/pg_ctl/psql binaries. Only this test's cluster is stopped.
"""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


# Intercept the first bootstrap command, create real conflicting resources,
# then pass all commands (including the failed CREATE and cleanup) to psql.
# There is no fake database result and no deterministic-name production hook.
PSQL_WRAPPER = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import re
import subprocess
import sys

args = sys.argv[1:]
state_path = Path(os.environ["NSU_STATE"])
sql = ""
for i, arg in enumerate(args[:-1]):
    if arg in ("-f", "--file"):
        sql += Path(args[i + 1]).read_text()
    elif arg == "--command" or (arg.startswith("-") and arg.endswith("c")):
        sql += args[i + 1]
match = re.search(r"create role pgque_nsu_installer_([a-zA-Z0-9_]+)", sql, re.I)
if match and not state_path.exists():
    suffix = match.group(1)
    roles = ["pgque_nsu_" + kind + "_" + suffix
             for kind in ("installer", "other", "reader", "writer")]
    databases = ["pgque_nsu_" + kind + "_" + suffix
                 for kind in ("main", "negctl")]
    mode = os.environ["NSU_COLLISION"]
    if mode == "first_role":
        protected_roles, protected_dbs = roles, databases
    elif mode == "partial_roles":
        protected_roles, protected_dbs = roles[-1:], databases
    elif mode == "partial_databases":
        protected_roles, protected_dbs = [], databases[-1:]
    elif mode == "none":
        protected_roles, protected_dbs = [], []
    else:
        raise RuntimeError("unknown collision mode")
    psql = [os.environ["NSU_REAL_PSQL"], "-X", "-v", "ON_ERROR_STOP=1",
            os.environ["PGQUE_TEST_SUPERUSER_DSN"], "-qAtc"]
    for role in protected_roles:
        subprocess.run(psql + ["create role " + role], check=True)
    for db in protected_dbs:
        subprocess.run(psql + ["create database " + db], check=True)
    snapshot_sql = (
        "select 'role', rolname, oid from pg_roles where rolname in (" +
        ",".join("'" + name + "'" for name in roles) + ") union all " +
        "select 'database', datname, oid from pg_database where datname in (" +
        ",".join("'" + name + "'" for name in databases) + ") order by 1, 2"
    )
    before = subprocess.check_output(psql + [snapshot_sql], text=True)
    state_path.write_text(json.dumps({"suffix": suffix, "before": before,
                                      "snapshot_sql": snapshot_sql}))
os.execv(os.environ["NSU_REAL_PSQL"], [os.environ["NSU_REAL_PSQL"], *args])
'''


class TestNonsuperuserCleanup(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        configured = os.environ.get("PG_BINDIR")
        if configured:
            cls.pg_bin = Path(configured)
        else:
            cls.pg_bin = Path(subprocess.check_output(
                ["pg_config", "--bindir"], text=True).strip())
        for command in ("initdb", "pg_ctl", "psql"):
            if not (cls.pg_bin / command).is_file():
                raise RuntimeError(f"missing {command}; set PG_BINDIR")
        cls.temp = tempfile.TemporaryDirectory(prefix="pgque-nsu-cleanup-")
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name)
        cls.socket = cls.root / "socket"
        cls.socket.mkdir()
        cls.data = cls.root / "data"
        subprocess.run([str(cls.pg_bin / "initdb"), "-D", str(cls.data),
                        "-A", "trust", "-U", "postgres", "--no-locale"],
                       check=True, stdout=subprocess.DEVNULL)
        # Register the stop before start: even an interrupted start must not
        # leave a server behind. The private data directory proves ownership.
        cls.addClassCleanup(cls.stop_cluster)
        subprocess.run([
            str(cls.pg_bin / "pg_ctl"), "-D", str(cls.data), "-w", "start",
            "-l", str(cls.root / "postgres.log"), "-o",
            f"-c listen_addresses='' -c unix_socket_directories='{cls.socket}'",
        ], check=True, stdout=subprocess.DEVNULL)
        cls.dsn = f"host={cls.socket} user=postgres dbname=postgres"
        cls.wrapper_bin = cls.root / "bin"
        cls.wrapper_bin.mkdir()
        wrapper = cls.wrapper_bin / "psql"
        wrapper.write_text(PSQL_WRAPPER)
        wrapper.chmod(0o700)
        cls.harness = Path(__file__).with_name("security_nonsuperuser_install.sh")

    @classmethod
    def stop_cluster(cls):
        if (cls.data / "postmaster.pid").exists():
            subprocess.run([str(cls.pg_bin / "pg_ctl"), "-D", str(cls.data),
                            "-m", "immediate", "-w", "stop"], check=True,
                           stdout=subprocess.DEVNULL)

    def run_harness(self, mode, label=None):
        state = self.root / ((label or mode) + ".json")
        env = dict(os.environ, PGQUE_TEST_SUPERUSER_DSN=self.dsn,
                   NSU_REAL_PSQL=str(self.pg_bin / "psql"),
                   NSU_STATE=str(state), NSU_COLLISION=mode,
                   PATH=str(self.wrapper_bin) + os.pathsep + os.environ["PATH"])
        result = subprocess.run(["bash", str(self.harness)], env=env,
                                capture_output=True, text=True, timeout=90)
        self.assertTrue(state.exists(), result.stdout + result.stderr)
        recorded = json.loads(state.read_text())
        after = subprocess.check_output([
            str(self.pg_bin / "psql"), "-X", "-v", "ON_ERROR_STOP=1",
            self.dsn, "-qAtc", recorded["snapshot_sql"],
        ], text=True)
        self.assertEqual(0 if mode == "none" else 1, result.returncode,
                         result.stdout + result.stderr)
        # Same names AND OIDs: preserve every pre-existing fixture, and remove
        # every newly created one, including resources before a failed CREATE.
        self.assertEqual(recorded["before"], after,
                         "cleanup changed pre-existing resources or leaked owned "
                         "resources\n" + result.stdout + result.stderr)
        return recorded, result

    def test_first_role_collision_preserves_all_foreign_fixtures(self):
        self.run_harness("first_role")

    def test_partial_role_bootstrap_removes_only_created_roles(self):
        self.run_harness("partial_roles")

    def test_partial_database_bootstrap_preserves_existing_database(self):
        self.run_harness("partial_databases")

    def test_fresh_and_existing_roles_use_distinct_random_names(self):
        suffixes = []
        for label in ("fresh", "existing"):
            recorded, result = self.run_harness("none", label)
            suffixes.append(recorded["suffix"])
            self.assertRegex(recorded["suffix"], r"^[0-9a-f]{32}$")
            self.assertIn("PASS: security_nonsuperuser_install", result.stdout)
            self.assertIn("ownership flip breaks the reader flow with 42501",
                          result.stdout)
        self.assertNotEqual(*suffixes)


if __name__ == "__main__":
    unittest.main(verbosity=2)
