"""No-database negative controls for the final-assembly configuration guard."""

from pathlib import Path
import re
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[1]


class FinalAssemblySearchPathTest(unittest.TestCase):
    def check_sql(self, sql):
        return subprocess.run(
            ["awk", "-f", str(ROOT / "build/check-definer-search-path.awk")],
            input=sql, text=True, capture_output=True, timeout=10)

    def test_generated_installer_and_late_override_negative_control(self):
        sql = (ROOT / "devel/sql/pgque.sql").read_text()
        result = self.check_sql(sql)
        self.assertEqual(result.returncode, 0, result.stderr)

        # version() comes from canonical lifecycle.sql, after the early PgQ
        # transformation check. That earlier check cannot see this mutation.
        mutant, changed = re.subn(
            r"(create or replace function pgque\.version\(\).*?"
            r"\$\$ language plpgsql security definer set search_path = pgque, pg_catalog), pg_temp;",
            r"\1;", sql, flags=re.DOTALL)
        self.assertEqual(changed, 1)
        result = self.check_sql(mutant)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must end with SET search_path = pgque, pg_catalog, pg_temp", result.stderr)

    def test_body_text_and_comments_cannot_supply_the_required_path(self):
        result = self.check_sql("""
-- security definer set search_path = pgque, pg_catalog, pg_temp
create function pgque.guard_probe() returns text as $body$
begin
    return 'security definer set search_path = pgque, pg_catalog, pg_temp';
end;
$body$ language plpgsql security /* nested /* comment */ still comment */ definer;
""")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("has no SET search_path", result.stderr)

    def test_extra_schema_and_missing_definers_are_rejected(self):
        result = self.check_sql("""
create function pgque.guard_probe() returns text as $$ select 'value' $$
language sql security definer set search_path = pgque, pg_catalog, pg_temp, public;
""")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must end with SET search_path", result.stderr)
        result = self.check_sql("select 'security definer';")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("no SECURITY DEFINER declarations", result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
