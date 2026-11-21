#!/usr/bin/env python3
"""Exercise fixture collisions and mandatory oracles on a private cluster.

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
connection_check = os.environ["NSU_CONNECTION_CHECK"]
sql = ""
for i, arg in enumerate(args[:-1]):
    if arg in ("-f", "--file"):
        path = Path(args[i + 1])
        source = path.read_text()
        sql += source
        # Every psql reconnect gets the same mandatory identity check, before
        # SET ROLE. These are generated files in the harness's own workdir.
        source = re.sub(r"(?m)^(\\connect[^\n]*\n)",
                        lambda match: match[1] + connection_check + "\n", source)
        path.write_text(source)
    elif arg == "--command" or re.fullmatch(r"-[qAt]*c", arg):
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
            os.environ["PGQUE_TEST_SUPERUSER_DSN"], "-q", "-c", connection_check,
            "-Atc"]
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
os.execv(os.environ["NSU_REAL_PSQL"],
         [os.environ["NSU_REAL_PSQL"], "-q", "-c", connection_check, *args])
'''


class TestNonsuperuserCleanup(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        configured = os.environ.get("PG_BINDIR")
        # This suite owns its server. No inherited libpq routing, service,
        # authentication, or session option may choose another destination.
        # PG_BINDIR is a test input above, not a child connection setting.
        cls.env = {key: value for key, value in os.environ.items()
                   if not key.startswith("PG")}
        if configured:
            cls.pg_bin = Path(configured)
        else:
            cls.pg_bin = Path(subprocess.check_output(
                ["pg_config", "--bindir"], env=cls.env, text=True).strip())
        for command in ("initdb", "pg_ctl", "psql"):
            if not (cls.pg_bin / command).is_file():
                raise RuntimeError(f"missing {command}; set PG_BINDIR")
        cls.temp = tempfile.TemporaryDirectory(prefix="pgque-nsu-cleanup-")
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name)
        cls.socket = cls.root / "socket"
        cls.socket.mkdir()
        cls.data = cls.root / "data"
        # The socket directory is unique and TCP is disabled, so this fixed
        # port cannot collide with another cluster's socket or TCP listener.
        cls.port = 65432
        subprocess.run([str(cls.pg_bin / "initdb"), "-D", str(cls.data),
                        "-A", "trust", "-U", "postgres", "--no-locale"],
                       env=cls.env, check=True, stdout=subprocess.DEVNULL)
        # Register the stop before start: even an interrupted start must not
        # leave a server behind. The private data directory proves ownership.
        cls.addClassCleanup(cls.stop_cluster)
        subprocess.run([
            str(cls.pg_bin / "pg_ctl"), "-D", str(cls.data), "-w", "start",
            "-l", str(cls.root / "postgres.log"), "-o",
            f"-p {cls.port} -c listen_addresses='' "
            f"-c unix_socket_directories='{cls.socket}' "
            "-c plpgsql.check_asserts=off",
        ], env=cls.env, check=True, stdout=subprocess.DEVNULL)
        cls.dsn = (f"host={cls.socket} port={cls.port} hostaddr='' "
                   "user=postgres dbname=postgres")
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
                           env=cls.env, stdout=subprocess.DEVNULL)

    def run_harness(self, mode, label=None, *, harness=None, expected_status=None,
                    check_asserts="off"):
        state = self.root / ((label or mode) + ".json")
        dsn = self.dsn + f" options='-c plpgsql.check_asserts={check_asserts}'"
        connection_check = self.connection_check(check_asserts)
        env = dict(self.env, PGQUE_TEST_SUPERUSER_DSN=dsn,
                   NSU_REAL_PSQL=str(self.pg_bin / "psql"),
                   NSU_STATE=str(state), NSU_COLLISION=mode,
                   NSU_CONNECTION_CHECK=connection_check,
                   PATH=str(self.wrapper_bin) + os.pathsep + self.env["PATH"])
        result = subprocess.run(["bash", str(harness or self.harness)], env=env,
                                capture_output=True, text=True, timeout=90)
        self.assertTrue(state.exists(), result.stdout + result.stderr)
        recorded = json.loads(state.read_text())
        after = subprocess.check_output([
            str(self.pg_bin / "psql"), "-X", "-v", "ON_ERROR_STOP=1",
            dsn, "-q", "-c", connection_check, "-Atc", recorded["snapshot_sql"],
        ], env=self.env, text=True)
        if expected_status is None:
            expected_status = 0 if mode == "none" else 1
        self.assertEqual(expected_status, result.returncode,
                         result.stdout + result.stderr)
        # Same names AND OIDs: preserve every pre-existing fixture, and remove
        # every newly created one, including resources before a failed CREATE.
        self.assertEqual(recorded["before"], after,
                         "cleanup changed pre-existing resources or leaked owned "
                         "resources\n" + result.stdout + result.stderr)
        return recorded, result

    def connection_check(self, check_asserts):
        """Prove all sessions use our Unix socket, owned server, and test GUC."""
        # Values come only from this fixture, not inherited connection inputs.
        def literal(value):
            return "'" + str(value).replace("'", "''") + "'"

        return f"""do $$ begin
  if inet_server_addr() is not null
     or current_setting('data_directory') is distinct from {literal(self.data)}
     or current_setting('unix_socket_directories') is distinct from {literal(self.socket)}
     or current_setting('port') is distinct from {literal(self.port)}
     or current_setting('plpgsql.check_asserts') is distinct from {literal(check_asserts)}
  then raise exception 'private NSU connection identity or assertion setting mismatch';
  end if;
end $$;"""

    def mutated_harness(self, label, old, new):
        """Change only this owned test copy; the installer source is shared read-only."""
        directory = self.root / label
        (directory / "tests").mkdir(parents=True)
        (directory / "devel").symlink_to(self.harness.parent.parent / "devel",
                                         target_is_directory=True)
        source = self.harness.read_text()
        self.assertEqual(1, source.count(old), "mutation must match exactly once")
        path = directory / "tests" / self.harness.name
        path.write_text(source.replace(old, new))
        return path

    def test_first_role_collision_preserves_all_foreign_fixtures(self):
        self.run_harness("first_role")

    def test_private_connection_identity_check_rejects_a_wrong_server_identity(self):
        # The connection never changes. A wrong expected identity must fail
        # on this owned server, without trying any alternate destination.
        check = self.connection_check("off")
        self.assertEqual(1, check.count(str(self.data)))
        wrong = check.replace(str(self.data), str(self.root / "not-our-data"))
        result = subprocess.run([
            str(self.pg_bin / "psql"), "-X", "-v", "ON_ERROR_STOP=1",
            self.dsn, "-qAtc", wrong,
        ], env=self.env, capture_output=True, text=True, timeout=10)
        self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("private NSU connection identity or assertion setting mismatch",
                      result.stderr)

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

    def test_negative_control_rejects_wrong_helper_with_asserts_on_and_off(self):
        harness = self.mutated_harness(
            "wrong-helper",
            "grant execute on function pgque._slot_guard(text, text, int, int, text) to ${other_owner};",
            "-- Intentionally omit one non-target helper grant in this test copy.")
        for setting in ("off", "on"):
            with self.subTest(check_asserts=setting):
                _, result = self.run_harness(
                    "none", "wrong-helper-" + setting, harness=harness,
                    expected_status=1, check_asserts=setting)
                self.assertIn("FAIL: step 55_negctl_post", result.stderr)
                self.assertIn("expected the denial to be on get_batch_cursor", result.stderr)
                self.assertIn("permission denied for function _slot_guard", result.stderr)
                self.assertNotIn("PASS: security_nonsuperuser_install", result.stdout)

    def test_required_count_oracle_with_asserts_on_and_off(self):
        harness = self.mutated_harness(
            "missing-events", "array['k-a', 'k-b', 'k-c']", "array['k-a', 'k-b']")
        for setting in ("off", "on"):
            with self.subTest(check_asserts=setting):
                _, result = self.run_harness(
                    "none", "missing-events-" + setting, harness=harness,
                    expected_status=1, check_asserts=setting)
                self.assertIn("FAIL: step 30_flow_main", result.stderr)
                self.assertIn("reader must drain all 6 keyed events, got 4", result.stderr)

    def test_required_ownership_oracle_with_asserts_on_and_off(self):
        # Inject after this script's connection, before its ownership check.
        marker = 'cat >"${workdir}/20_ownership.sql" <<SQL\n\\\\connect ${db_main}\n'
        harness = self.mutated_harness("wrong-owner", marker, marker +
            "alter function pgque.ack_partitioned(text, text, int, int, text) "
            "owner to ${other_owner};\n")
        for setting in ("off", "on"):
            with self.subTest(check_asserts=setting):
                _, result = self.run_harness(
                    "none", "wrong-owner-" + setting, harness=harness,
                    expected_status=1, check_asserts=setting)
                self.assertIn("FAIL: step 20_ownership", result.stderr)
                self.assertIn("co-ownership broken out of the box", result.stderr)

    def test_intended_negative_control_with_asserts_enabled(self):
        _, result = self.run_harness("none", "intended-on", check_asserts="on")
        self.assertIn("42501 on get_batch_cursor", result.stdout)
        self.assertIn("PASS: security_nonsuperuser_install", result.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
