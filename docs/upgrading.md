# Upgrading PgQue

PgQue upgrades are SQL-file upgrades. Install the new release over the existing
schema by re-running `sql/pgque.sql` as the schema owner or a superuser, from
the repository root:

```bash
psql --single-transaction -v ON_ERROR_STOP=1 -d mydb -f sql/pgque.sql
```

The installer is idempotent: it preserves queues, consumers, subscriptions,
retry rows, DLQ rows, and existing event tables while adding new functions,
columns, grants, and constraints required by the target release.

## v0.2.0 to v0.2.1

Re-run the installer using the command above. This maintenance release makes
`pgque.receive()` and `pgque.receive_coop()` reject batches larger than
`max_return`, rather than returning a truncated result that could be
acknowledged as a complete batch. The upgrade replaces functions only; it does not
change tables or queue state.

Applications do not need client-library updates for this server-side fix.
An undersized receive ceiling now causes an error: roll back the failed
transaction and retry with a sufficient ceiling within your resource budget.
Never acknowledge after a receive error. Ticker thresholds do not cap batch
size, and repeated receive calls are not pagination.

For a pg_tle-managed v0.2.0 installation, register the update path and apply
it through Postgres extension management:

```sql
\i sql/pgque-tle.sql
alter extension pgque update to '0.2.1';
select extversion from pg_extension where extname = 'pgque';
select pgque.version();
```

Both version queries must return `0.2.1`. The update replaces functions only;
do not drop or unregister the existing extension before upgrading.

## v0.1.0 to v0.2.0

The supported v0.1.0 → v0.2.0 path is the same re-install procedure:

```bash
cd /path/to/pgque
psql --single-transaction -v ON_ERROR_STOP=1 -d mydb -f sql/pgque.sql
```

v0.2.0 renames several public API argument names to the documented API
names (`queue_name`, `type_name`, `payload`, `payloads`, `queue`, `consumer`) so named-argument calls
are stable going forward. PostgreSQL does not allow `CREATE OR REPLACE FUNCTION`
to rename input arguments in-place, so the installer drops and recreates only
those wrapper functions before defining the v0.2.0 versions. Data tables and
queue state are not dropped.

After upgrading, verify the installed version:

```sql
select pgque.version();
-- 0.2.1, or the exact release you installed
```

You can also run the idempotency smoke test from the repository:

```bash
psql -v ON_ERROR_STOP=1 -d mydb -f tests/test_install_idempotency.sql
```

## CI coverage

The repository CI includes a dedicated `upgrade v0.1.0 to HEAD` job for PostgreSQL
14 and 18. It installs v0.1.0, creates representative queue state, reinstalls
HEAD's `sql/pgque.sql` transactionally, verifies the state survived, checks the
post-upgrade grants/security posture, and runs the install idempotency test after
the upgrade.
