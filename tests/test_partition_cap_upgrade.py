#!/usr/bin/env python3
"""Check the over-cap upgrade guard and recovery from the actual old install.

Run with local PostgreSQL binaries via PG_BINDIR. Requires the pinned historical
commit in this checkout; it never fetches a source or connects to an existing DB.
"""

from pathlib import Path
import re
import subprocess
import unittest

from private_postgres import PrivatePostgres


OLD_COMMIT = "58d754456ed9ca36f67dff66d40b05ddd446839e"
SNAPSHOT = """
select json_build_object(
  'version', pgque.version(),
  'functions', (select json_agg(json_build_array(p.oid, p.proowner,
                    pg_get_functiondef(p.oid)) order by p.oid)
                from pg_proc p where p.pronamespace = 'pgque'::regnamespace),
  'relations', (select json_agg(json_build_array(c.oid, c.relname, c.relkind,
                    c.relowner, c.relnatts) order by c.oid)
                from pg_class c where c.relnamespace = 'pgque'::regnamespace),
  'constraints', (select json_agg(json_build_array(c.oid, c.conrelid, c.conname,
                    pg_get_constraintdef(c.oid)) order by c.oid)
                  from pg_constraint c where c.connamespace = 'pgque'::regnamespace),
  'queues', (select json_agg(q order by queue_id) from pgque.queue q),
  'consumers', (select json_agg(c order by co_id) from pgque.consumer c),
  'subscriptions', (select json_agg(s order by sub_id) from pgque.subscription s),
  'partition_consumers', (select json_agg(c order by queue_id, co_name)
                          from pgque.partition_consumer c),
  'partition_slots', (select json_agg(s order by queue_id, co_name, slot)
                      from pgque.partition_slot s)
);
"""


class TestPartitionCapUpgrade(unittest.TestCase):
    def test_old_api_recovery_order_and_transactional_guard(self):
        repo = Path(__file__).resolve().parent.parent
        with PrivatePostgres() as pg:
            database = "partition_cap_upgrade_test"
            pg.run("create database " + database)
            old = pg.root / "old.sql"
            # safe.directory is scoped to this one read-only git invocation,
            # also allowing the CI postgres OS user to read the root checkout.
            old.write_bytes(subprocess.check_output([
                "git", "-c", "safe.directory=" + str(repo), "show",
                OLD_COMMIT + ":devel/sql/pgque.sql",
            ], cwd=repo))
            pg.install(old, database)
            pg.run("select pgque.create_queue('cap_upgrade_q')", database)
            pg.run("do $$ begin for s in 0..256 loop "
                   "perform pgque.subscribe_slot('cap_upgrade_q', 'over_cap', s, 257); "
                   "end loop; end $$", database)
            # These committed events are newer than the old subscriptions.
            # The recovery below deliberately accepts loss of pending work;
            # it does not claim that unsubscribing preserves old cursor state.
            pg.run("select pgque.send('cap_upgrade_q', 'pending', 'before upgrade', 'k')", database)
            pg.run("select pgque.force_next_tick('cap_upgrade_q'); select pgque.ticker()", database)
            api = "select to_regprocedure('pgque.subscribe_partitioned(text,text,integer)') is null"
            self.assertEqual("t", pg.query(api, database))
            self.assertEqual("257|257", pg.query(
                "select n, (select count(*) from pgque.partition_slot) "
                "from pgque.partition_consumer where co_name = 'over_cap'", database))
            event_table = pg.query("select pgque.current_event_table('cap_upgrade_q')", database)
            self.assertRegex(event_table, r"^pgque\.event_[0-9]+_[0-9]+$")
            events_sql = f"select row_to_json(e) from {event_table} e order by ev_id"
            pending = pg.query(events_sql, database)
            self.assertIn("before upgrade", pending)
            before = pg.query(SNAPSHOT, database)
            # Observe API availability inside the upgrade transaction, just
            # before the guard call. This copy adds only a read-only lookup
            # and diagnostic; all generated installer statements remain.
            candidate = (repo / "devel/sql/pgque.sql").read_text()
            marker = "perform pgque._partition_n_cap_guard();"
            self.assertEqual(1, candidate.count(marker))
            observed = pg.root / "candidate-observed.sql"
            observed.write_text(candidate.replace(marker,
                "raise warning 'cap_guard_new_api=%', case when "
                "to_regprocedure('pgque.subscribe_partitioned(text,text,integer)') is null "
                "then 'absent' else 'present' end;\n    " + marker))
            rejected = subprocess.run(
                pg.command(database) + ["--single-transaction", "-f", str(observed)],
                env=pg.env, text=True, capture_output=True, timeout=30)
            self.assertNotEqual(0, rejected.returncode, rejected.stdout + rejected.stderr)
            self.assertIn("cannot apply the 256-slot cap", rejected.stderr)
            self.assertIn("over_cap on queue cap_upgrade_q has n=257", rejected.stderr)
            self.assertIn("cap_guard_new_api=absent", rejected.stderr,
                          "new API was already available when the guard ran")
            self.assertEqual("t", pg.query(api, database), "new API appeared despite the guard")
            self.assertEqual(before, pg.query(SNAPSHOT, database),
                             "rejected transactional upgrade changed old functions, schema, or queue state")
            self.assertEqual(pending, pg.query(events_sql, database),
                             "rejected upgrade changed pending event data")
            print("guard: rejected n=257; new API absent before guard call; old schema and state unchanged", flush=True)

            # Use only the API available before the failed upgrade. Explicitly
            # accept pending-work loss here; real operators can drain first.
            pg.run("do $$ begin for s in 0..256 loop "
                   "perform pgque.unsubscribe_slot('cap_upgrade_q', 'over_cap', s); "
                   "end loop; end $$", database)
            self.assertEqual("0|0", pg.query(
                "select (select count(*) from pgque.partition_consumer), "
                "(select count(*) from pgque.partition_slot)", database))
            self.assertEqual("t", pg.query(api, database))
            pg.install(repo / "devel/sql/pgque.sql", database)
            self.assertEqual("f", pg.query(api, database))
            pg.run("select pgque.subscribe_partitioned('cap_upgrade_q', 'over_cap', 2)", database)
            self.assertEqual("2|2", pg.query(
                "select n, (select count(*) from pgque.partition_slot) "
                "from pgque.partition_consumer where co_name = 'over_cap'", database))
            print("recovery: accepted pending loss; unsubscribed old slots; installed; recreated n=2", flush=True)

            diagnostic = next(line for line in rejected.stderr.splitlines()
                              if "cannot apply the 256-slot cap" in line)
            print(diagnostic, flush=True)
            with self.subTest(oracle="pending-work warning"):
                self.assertRegex(diagnostic, r"drain.*pending.*explicitly accept.*loss")
            with self.subTest(oracle="old API cleanup before install"):
                self.assertLess(diagnostic.index("pgque.unsubscribe_slot()"),
                                diagnostic.index("re-run the install"))
            with self.subTest(oracle="new API only after install"):
                self.assertLess(diagnostic.index("re-run the install"),
                                diagnostic.index("pgque.subscribe_partitioned()"))
            self.assertIn("n <= 256", diagnostic)
            canonical = (repo / "devel/sql/pgque-api/partition_keys.sql").read_text()
            message = re.search(r"raise exception '(cannot apply the 256-slot cap[^']+)'", canonical)[1]
            for installer in ("pgque.sql", "pgque-tle.sql"):
                self.assertIn(message, (repo / "devel/sql" / installer).read_text())


if __name__ == "__main__":
    unittest.main(verbosity=2)
