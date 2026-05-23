# Upgrading PgQue

PgQue upgrades are SQL-file upgrades. Install the new release over the existing
schema by re-running `sql/pgque.sql` as the schema owner or a superuser:

```sql
begin;
\i sql/pgque.sql
commit;
```

The installer is idempotent: it preserves queues, consumers, subscriptions,
retry rows, DLQ rows, and existing event tables while adding new functions,
columns, grants, and constraints required by the target release.

## v0.1.0 to v0.2.0

The supported v0.1.0 → v0.2.0 path is the same re-install procedure:

```bash
cd /path/to/pgque
psql -v ON_ERROR_STOP=1 -d mydb -f sql/pgque.sql
```

v0.2.0 renames several public publishing argument names to the documented API
names (`queue_name`, `type_name`, `payload`, `payloads`) so named-argument calls
are stable going forward. PostgreSQL does not allow `CREATE OR REPLACE FUNCTION`
to rename input arguments in-place, so the installer drops and recreates only
those wrapper functions before defining the v0.2.0 versions. Data tables and
queue state are not dropped.

After upgrading, verify the installed version:

```sql
select pgque.version();
```

You can also run the idempotency smoke test from the repository:

```bash
psql -v ON_ERROR_STOP=1 -d mydb -f tests/test_install_idempotency.sql
```

## CI coverage

The repository CI includes a dedicated `upgrade-v0.1-to-head` job for PostgreSQL
14 and 18. It installs v0.1.0, creates representative queue state, reinstalls
HEAD's `sql/pgque.sql`, verifies the state survived, and runs the install
idempotency test after the upgrade.
