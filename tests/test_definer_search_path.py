"""Installed path configuration checks in an owned socket-only PostgreSQL.

No shadow objects or function execution probes. Negative controls change only
catalog attributes inside rolled-back transactions or the required manifest.
"""

from pathlib import Path
import unittest

from private_postgres import PrivatePostgres


ROOT = Path(__file__).resolve().parents[1]


class DefinerSearchPathTest(unittest.TestCase):
    def test_clean_install_reinstall_and_mandatory_oracles(self):
        check = (ROOT / "tests/test_paged_search_path.sql").read_text()
        # The required checks must not depend on PL/pgSQL ASSERT being enabled.
        check = "set plpgsql.check_asserts = off;\n" + check
        with PrivatePostgres() as pg:
            print("Private server:", pg.query(
                "select version(), current_setting('data_directory'), "
                "current_setting('unix_socket_directories'), current_setting('port')"),
                flush=True)
            pg.run("create database definer_config")
            for phase in ("clean install", "reinstall"):
                pg.install(ROOT / "devel/sql/pgque.sql", "definer_config")
                result = pg.run(check, "definer_config", check=False)
                print(phase, "exit", result.returncode, flush=True)
                print(result.stdout + result.stderr, flush=True)
                self.assertEqual(result.returncode, 0, phase + " configuration failed")
                self.assertIn("checked all 101 PgQue SECURITY DEFINER routines", result.stderr)

            # A catalog-wide scan must catch a definer even when it is absent
            # from the required-signature list. Use an existing function; do
            # not create a shadow object or call the altered function.
            without_version = check.replace("        'pgque.version()'", "")
            # version() is the last alphabetic entry, so remove its separator.
            without_version = without_version.replace(",\n\n    ] loop", "\n    ] loop")
            self.assertNotEqual(check, without_version)
            result = pg.run(
                "begin; alter function pgque.version() "
                "set search_path = pgque, pg_catalog;\n" + without_version,
                "definer_config", check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("1 definer routine(s) have an unsafe configured search_path", result.stderr)
            self.assertIn("pgque.version()", result.stderr)
            print("PASS: unlisted definer with omitted pg_temp is rejected", flush=True)

            missing = check.replace("'pgque.version()'", "'pgque._missing_required_definer()'")
            result = pg.run(missing, "definer_config", check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("expected definer function missing: pgque._missing_required_definer()",
                          result.stderr)
            print("PASS: missing required signature is rejected", flush=True)

            result = pg.run("begin; alter function pgque.version() security invoker;\n" + check,
                            "definer_config", check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("expected SECURITY DEFINER function: pgque.version()", result.stderr)
            print("PASS: lost required SECURITY DEFINER flag is rejected", flush=True)

            # Failed transactional controls must leave the installed config intact.
            pg.run(check, "definer_config")
            print("PASS: all configuration controls rolled back cleanly", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
