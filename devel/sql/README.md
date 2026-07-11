# PgQue — in-development version

This directory holds the **in-development** PgQue install, ahead of the last
released version. It is for testing upcoming features before they ship; expect
churn and validate against a throwaway database, not production.

The released, stable install lives in [`../../sql/`](../../sql/) — use that for
anything real.

Testing partition keys (ordered per-key processing via slot consumers)? Until
`docs/` coverage lands, follow
[`blueprints/partition-keys/SPEC.md`](../../blueprints/partition-keys/SPEC.md).

Layout:

- `pgque.sql`, `pgque-tle.sql` — generated single-file installs (built from the
  sources below by `build/transform.sh`; do not edit by hand).
- `pgque_uninstall.sql`, `pgque-tle-uninstall.sql` — uninstall scripts.
- `pgque-additions/`, `pgque-api/`, `experimental/` — the SQL sources the build
  reads. Edit these, then re-run `bash build/transform.sh` from the repo root.

## Install

**Requirements:** Postgres 14+, and something that calls `pgque.ticker()`
periodically (see Ticker below).

Run psql from the repo root so the relative path resolves:

```sql
begin;
\i devel/sql/pgque.sql
commit;
```

Or from the shell, in a single transaction:

```bash
PAGER=cat psql --no-psqlrc --single-transaction -d mydb -f devel/sql/pgque.sql
```

To uninstall: `\i devel/sql/pgque_uninstall.sql`.

## Ticker

PgQue does not deliver messages without a ticker: enqueueing works, but
consumers see nothing until ticks are created.

On a quiet queue, the ticker falls back to `queue_ticker_idle_period`
(default 60s), so a newly enqueued event can take up to that long to become
receivable. Run the ticker at your desired cadence to bound this latency.

With `pg_cron` in the same database, one call sets up the ticker and
maintenance jobs (10 ticks/sec by default):

```sql
select pgque.start();
```

Without `pg_cron`, drive it from your application or an external scheduler:

```bash
PAGER=cat psql --no-psqlrc -d mydb -c "select pgque.ticker()"              # at your tick period
PAGER=cat psql --no-psqlrc -d mydb -c "select pgque.maint_retry_events()"  # every 30 seconds
PAGER=cat psql --no-psqlrc -d mydb -c "select pgque.maint()"               # every 30 seconds
```

Skipping `maint_retry_events()` means nack'd events are never redelivered.

## Roles and grants

The install creates three roles. `pgque_reader` (consume) and `pgque_writer`
(produce) are siblings, not parent/child; `pgque_admin` is a member of both.
An app that **both** produces and consumes must be granted **both** roles.

```sql
-- Produce + consume in the same app: grant BOTH roles.
create user app_orders with password '...';
grant pgque_reader to app_orders;
grant pgque_writer to app_orders;

-- Pure producer.
create user app_webhook with password '...';
grant pgque_writer to app_webhook;

-- Pure consumer / dashboard / metrics.
create user metrics with password '...';
grant pgque_reader to metrics;
```

## Partitioned consumer setup

Create the whole partitioned consumer before publishing events. One call pins
the slot count and creates every slot subscription and lease row atomically at
the same starting tick:

```sql
select pgque.create_queue('orders');
select pgque.subscribe_partitioned('orders', 'workers', 4);
```

Calling `subscribe_partitioned` again with the same slot count is idempotent and
does not move existing cursors. `subscribe_slot` remains available for explicit
repair of an incomplete alpha setup; it is not the normal setup path because a
slot registered after events have been ticked cannot recover that earlier
history. Check `pgque.partition_slot_status.subscribed`: missing subscriptions
report `false`, with `pending_events` set to `NULL` because their lag is unknown.

The safety ceiling is 256 slots per partitioned consumer. It is not a target:
use the smallest slot count that provides the required parallelism. Every slot
scans the queue stream and must be polled, so read amplification and the cost of
a full polling cycle increase with the slot count.

## pg_tle install

To register PgQue as a `pg_tle` extension instead (requires `pg_tle` in
`shared_preload_libraries` and a role with `pgtle_admin` + `CREATEROLE`):

```sql
\i devel/sql/pgque-tle.sql
create extension pgque;
```

To uninstall: `\i devel/sql/pgque-tle-uninstall.sql`.
