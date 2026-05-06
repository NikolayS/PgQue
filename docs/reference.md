# Function reference

Every function shipped in the default install (`\i sql/pgque.sql`). Each entry lists the signature, return type, the role it is granted to, and the source file. A short code example appears where the signature alone leaves the call ambiguous.

If you are new to PgQue, start with [tutorial.md](tutorial.md) — it walks the end-to-end `send` / `receive` / `ack` loop. Use this as the lookup table.

Each entry takes this form:


#### `pgque.<name>(arg text, …) → returntype`

One-line description. Optional second line with a caveat.
Grant: `role_name`. Source: `sql/<path>`.



Functions shipped outside the default install are in the [Experimental](#experimental-not-in-default-install) section.

## Publishing

Single-message `send` wrappers delegate to `pgque.insert_event`; batch `send_batch` wrappers delegate to the internal set-based `pgque.insert_event_bulk` primitive. The `text` overloads are the fast path (bytes flow through verbatim); the `jsonb` overloads validate and canonicalize via Postgres before storing. Postgres `text` cannot store NUL (`\x00`), so raw binary must be base64/hex-encoded by the caller. See [SPECx.md §4.1](../blueprints/SPECx.md) for details on overload resolution.

### Publishing argument names and types

Argument names are part of the SQL API because PostgreSQL supports named calls (`arg := value`). Available publishing arguments:

| Argument | SQL type | Meaning |
|----------|----------|---------|
| `queue_name` | `text` | PgQue queue name. |
| `type_name` | `text` | Application event type stored in `ev_type` (`'default'` for 2-arg `send`). Free-form text such as `order.created`; this is not a PostgreSQL type. |
| `payload` | `text` or `jsonb` | Single event payload. `text` is opaque/verbatim; `jsonb` validates and stores canonical JSON text. |
| `payloads` | `text[]` or `jsonb[]` | Batch payload array. Result array positions correspond to input positions. |

Available publishing overloads:

| Function | Payload type | Return |
|----------|--------------|--------|
| `send(queue_name, payload)` | `text` or `jsonb` | `bigint` event id |
| `send(queue_name, type_name, payload)` | `text` or `jsonb` | `bigint` event id |
| `send_batch(queue_name, payloads)` | `text[]` or `jsonb[]` | `bigint[]` event ids, event type `'default'` |
| `send_batch(queue_name, type_name, payloads)` | `text[]` or `jsonb[]` | `bigint[]` event ids |

Use explicit casts (`::jsonb`, `::jsonb[]`, `::text[]`) when overload resolution would otherwise be ambiguous. Untyped string literals choose the `text` fast path.

**Named-argument note:** modern publishing argument names are `queue_name`, `type_name`, `payload`, and `payloads`. Positional calls are unchanged.

#### `pgque.send(queue_name text, payload jsonb) → bigint`

Inserts `payload` into `queue` with event type `'default'`. Returns the event id.
Grant: `pgque_writer`. Source: `sql/pgque-api/send.sql`.

```sql
select pgque.send('orders', '{"order_id": 42}'::jsonb);
```

#### `pgque.send(queue_name text, payload text) → bigint`

Fast-path send: stores the payload bytes verbatim, no JSON parse. Untyped string literals (`'…'`) resolve to this overload. Returns the event id.
Grant: `pgque_writer`. Source: `sql/pgque-api/send.sql`.

#### `pgque.send(queue_name text, type_name text, payload jsonb) → bigint`

Same as the 2-arg `jsonb` overload, but with an explicit event type. Returns the event id.
Grant: `pgque_writer`. Source: `sql/pgque-api/send.sql`.

```sql
select pgque.send('orders', 'order.created', '{"order_id": 42}'::jsonb);
```

#### `pgque.send(queue_name text, type_name text, payload text) → bigint`

Fast-path send with explicit event type. Returns the event id.
Grant: `pgque_writer`. Source: `sql/pgque-api/send.sql`.

#### `pgque.send_batch(queue_name text, payloads jsonb[]) → bigint[]`

Default-type JSON batch send. Equivalent to `pgque.send_batch(queue_name, 'default', payloads)`.
Grant: `pgque_writer`. Source: `sql/pgque-api/send.sql`.

#### `pgque.send_batch(queue_name text, type_name text, payloads jsonb[]) → bigint[]`

Set-based batch send for JSON payloads: validates each element as `jsonb`, stores its canonical text form, and returns event ids aligned to input order. Do not rely on the numeric ids being monotonically increasing inside one batch; use array position for input/result correlation. Empty arrays return `{}` without queue lookup; `NULL` arrays raise `payloads must not be null`. Non-empty batches still validate queue state once up front: unknown queues raise `queue not found: <queue>`, and write-disabled queues raise `Insert into queue disallowed`. NULL elements inside a non-null array are stored as NULL `ev_data`.
Grant: `pgque_writer`. Source: `sql/pgque-api/send.sql`.

```sql
select pgque.send_batch('orders', 'order.created',
    array['{"id":1}', '{"id":2}']::jsonb[]);

-- Named-argument calls are supported; argument names are part of the API.
select pgque.send_batch(
    queue_name := 'orders',
    type_name := 'order.created',
    payloads := array['{"id":1}', '{"id":2}']::jsonb[]
);
```

#### `pgque.send_batch(queue_name text, payloads text[]) → bigint[]`

Default-type text batch send. Equivalent to `pgque.send_batch(queue_name, 'default', payloads)`.
Grant: `pgque_writer`. Source: `sql/pgque-api/send.sql`.

#### `pgque.send_batch(queue_name text, type_name text, payloads text[]) → bigint[]`

Set-based fast-path batch send for opaque text payloads. Returns event ids aligned to input order. Do not rely on the numeric ids being monotonically increasing inside one batch; use array position for input/result correlation. Empty arrays return `{}` without queue lookup; `NULL` arrays raise `payloads must not be null`. Non-empty batches still validate queue state once up front: unknown queues raise `queue not found: <queue>`, and write-disabled queues raise `Insert into queue disallowed`. NULL elements inside a non-null array are stored as NULL `ev_data`.
Grant: `pgque_writer`. Source: `sql/pgque-api/send.sql`.

#### `pgque.insert_event_bulk(queue_name text, ev_type text, ev_data_list text[]) → bigint[]`

**Not directly callable by API roles.** Internal set-based primitive used by `send_batch`: resolves the queue/table once, allocates ids from the queue sequence, inserts all payloads with one `INSERT … SELECT`, and returns ids aligned to input order. It is `SECURITY DEFINER` so the public wrappers can use it, but EXECUTE is revoked from public API roles (including `pgque_admin`) to keep callers on the stable `send_batch` surface. The schema owner/superuser can still call it for install/debug work.
Grant: none (internal). Source: `sql/pgque-api/send.sql`.

## Consuming

The consume API wraps `pgque.next_batch`, `pgque.get_batch_events`, `pgque.finish_batch`, and `pgque.event_retry`. Typical loop: `receive` → process → `ack` (or `nack` on failure).

All consume-side functions (`receive`, `ack`, `nack`, `subscribe`, `unsubscribe`) are granted to `pgque_reader`, mirroring upstream PgQ's producer/consumer role split. Apps that both produce and consume must hold both `pgque_reader` and `pgque_writer` — `pgque_writer` does not inherit `pgque_reader`.

<a id="snapshot-rule"></a>**Snapshot rule.** `pgque.send` → `pgque.ticker` → `pgque.receive` must each run in its own committed transaction (the ticker's snapshot must be taken after `send` commits; `receive` only sees what committed before it). Same for `pgque.maint_retry_events` → `pgque.ticker` → `pgque.receive`. Go (`pgxpool`) and TypeScript (`pg.Pool`) satisfy this transparently; Python `pgque.connect()` is non-autocommit by default and needs explicit commit boundaries (the high-level Python `Consumer` handles this internally). The footgun in every driver is reaching for the underlying pool/connection (`Client.Pool()`, `client.rawPool`, `client.conn`) to wrap producer + consumer calls in one explicit transaction. See [pgq-concepts.md#snapshot-rule](pgq-concepts.md#snapshot-rule).

#### `pgque.receive(queue text, consumer text, max_return int default 100) → setof pgque.message`

Pulls the next batch for `consumer` on `queue` and streams up to `max_return` messages. `max_return` must be >= 1; passing 0 or a negative value raises an error. Returns an empty set if no batch is available. Each row is a `pgque.message` composite (see [§Message type](#message-type)).
Grant: `pgque_reader`. Source: `sql/pgque-api/receive.sql`.

```sql
select * from pgque.receive('orders', 'processor', 100);
```

**Batch-ownership caveat.** `max_return` limits the number of rows returned to the caller, but `ack(batch_id)` advances the consumer cursor past the entire underlying batch. If `max_return < ticker_max_count`, calling `ack()` after a partial receive will drop the unreturned rows from the consumer's perspective. Either consume the full batch before acking, or use `max_return >= ticker_max_count` for safe pagination.

#### `pgque.ack(batch_id bigint) → integer`

Closes the batch and advances the consumer position. Modern alias for `pgque.finish_batch`. Returns `1` on success, `0` if the batch was not found.
Grant: `pgque_reader`. Source: `sql/pgque-api/receive.sql`.

#### `pgque.nack(batch_id bigint, msg pgque.message, retry_after interval default '60 seconds', reason text default null) → integer`

Negative-acknowledges one message. Only `msg.msg_id` (and the `batch_id` argument) are honored from the composite — `type`, `payload`, `retry_count`, `created_at`, and the `extra*` fields are **ignored**. `nack()` re-queries the canonical event from the active batch and uses those server-side values for all decisions and writes.

- If the canonical `ev_retry` is below the queue's `max_retries`, re-queues after `retry_after` (via `pgque.event_retry`).
- If `ev_retry >= max_retries`, routes the canonical event to `pgque.dead_letter` (via `pgque.event_dead`). This is idempotent: repeated calls for the same terminal message produce exactly one DLQ row (the second call does nothing).
- If `msg.msg_id` is not present in the active batch — including a `NULL` msg_id or a msg_id from a different batch — raises `msg_id % not found in batch %`.
Grant: `pgque_reader`. Source: `sql/pgque-api/receive.sql`.

```sql
perform pgque.nack(msg.batch_id, msg, interval '5 minutes', 'validation failed');
```

## Cooperative consumers / subconsumers

**Experimental.** Function names, edge-case behavior, and client API shape may change before this feature is marked stable. Do not use this as the only processing path for critical workloads without idempotent handlers and stale-worker takeover tests.

Cooperative consumers let several subconsumers share one logical consumer cursor. The main consumer row (`sub_role = 'coop_main'`) owns the group cursor; member rows (`sub_role = 'coop_member'`) own active batches. The feature is bundled in the default SQL install, but downgrade after creating subconsumers is unsupported unless subconsumers are unregistered first.

#### `pgque.register_subconsumer(queue text, consumer text, subconsumer text, convert_normal boolean default false) → integer`

Registers `subconsumer` under logical `consumer`. Returns `1` for a new registration and `0` when already registered. If `consumer` already exists as a normal consumer, conversion is refused unless `convert_normal = true`, and active normal consumers cannot be converted.
Grant: `pgque_reader`. Source: `sql/pgque-api/cooperative_consumers.sql`.

#### `pgque.unregister_subconsumer(queue text, consumer text, subconsumer text, batch_handling integer default 0) → integer`

Unregisters one subconsumer. Active batches are rejected by default; `batch_handling = 1` routes active messages through retry/DLQ policy equivalent to `nack()` before unregistering. When the last member is removed, the main row returns to `sub_role = 'normal'`.
Grant: `pgque_reader`. Source: `sql/pgque-api/cooperative_consumers.sql`.

`pgque.subscribe_subconsumer(...)` and `pgque.unsubscribe_subconsumer(...)` are aliases for the two functions above.

#### `pgque.receive_coop(queue text, consumer text, subconsumer text, max_return int default 100, dead_interval interval default null) → setof pgque.message`

Receives messages for one subconsumer. `max_return` must be >= 1. `dead_interval` enables stale-batch takeover from another inactive member; takeover allocates a fresh `batch_id`, so old tokens cannot ack/nack the new owner's state. The cooperative group is a trust boundary: callers allowed to use the same `(queue, consumer)` can steal stale batches from each other by design, so do not share one cooperative group across mutually untrusted workers.

**Auto-registration.** If the logical `consumer` or `subconsumer` is not yet registered, `receive_coop()` registers them on the fly (creates the `coop_main` row on first call, then the `coop_member` row), so a worker can call `receive_coop()` cold without a prior `register_subconsumer`. Use the explicit `register_subconsumer(..., convert_normal => true)` call only when you need to convert an existing normal consumer into a cooperative main.

**Empty tick windows are auto-finished.** When the current batch's tick window holds no events, `receive_coop()` calls `finish_batch` internally and returns the empty set. Callers polling a quiet queue do not see (and do not need to ack) a `batch_id`; this differs from `receive()`, which still returns an active batch token even when the result set is empty.

**Batch-ownership caveat.** As with `receive()`, `max_return` limits only returned rows; `ack(batch_id)` advances the cooperative cursor past the whole underlying batch. Use `max_return >= ticker_max_count` or consume the full batch before acking.

**Throughput note.** Cooperative allocation serializes on a `FOR UPDATE` of the `coop_main` subscription row, so many workers polling tiny batches contend on a single hot row. If you scale workers, also tune `ticker_max_count` and tick cadence so each batch is large enough to amortize the lock.

Grant: `pgque_reader`. Source: `sql/pgque-api/cooperative_consumers.sql`.

#### `pgque.next_batch(queue text, consumer text, subconsumer text, dead_interval interval default null) → bigint`

Low-level cooperative batch allocation. Prefer `receive_coop()` unless the client explicitly needs PgQ-style batch primitives.
Grant: `pgque_reader`. Source: `sql/pgque-api/cooperative_consumers.sql`.

#### `pgque.next_batch_custom(queue text, consumer text, subconsumer text, min_lag interval, min_count int4, min_interval interval, dead_interval interval default null) → record`

Cooperative custom batch allocation. Out columns: `batch_id`, `prev_tick_id`, `next_tick_id`.
Grant: `pgque_reader`. Source: `sql/pgque-api/cooperative_consumers.sql`.

#### `pgque.touch_subconsumer(queue text, consumer text, subconsumer text) → integer`

Updates a registered subconsumer heartbeat without creating rows. Returns the number of rows touched.
Grant: `pgque_reader`. Source: `sql/pgque-api/cooperative_consumers.sql`.

**Cooperative-aware inherited functions.** `pgque.unregister_consumer()` refuses to unregister a cooperative main while subconsumers are registered; unregister subconsumers explicitly. Normal `pgque.next_batch*()` / `pgque.receive()` raise on cooperative rows in two cases: the named consumer is a `coop_main` with at least one registered member (the main row is the group cursor, not a per-worker token), or the named consumer is a `coop_member` row (member rows must go through `receive_coop()` / cooperative `next_batch()`). Both cases include a directive in the error message pointing to the cooperative form. `pgque.finish_batch()` rejects `coop_main` batches and clears member-owned cooperative batches on ack. Batch ids are bearer tokens, matching inherited PgQ behavior: a caller that learns a valid batch id can finish it, so keep batch ids inside trusted consumer code.

#### `pgque.subscribe(queue text, consumer text) → integer`

Registers `consumer` on `queue`. Modern alias for `pgque.register_consumer`. Returns `1` on new registration, `0` if already registered.
Grant: `pgque_reader`. Source: `sql/pgque-api/send.sql` (despite the file name, the grant is `pgque_reader` — subscription management is a consumer-side operation; the file historically co-locates produce wrappers and subscription wrappers).

#### `pgque.unsubscribe(queue text, consumer text) → integer`

Removes the consumer (and its retry-queue entries) from `queue`. Modern alias for `pgque.unregister_consumer`.
Grant: `pgque_reader`. Source: `sql/pgque-api/send.sql` (see note above).

## Queue management

#### `pgque.create_queue(queue text) → integer`

Creates a queue with default settings (3 rotation tables, built-in ticker). Returns `1` if created, `0` if a queue with that name already exists. Queue names are limited to 57 bytes (UTF-8); the `pgque_<name>` LISTEN/NOTIFY channel must fit within PostgreSQL's 63-byte identifier limit.
Grant: `pgque_admin`. Source: `sql/pgque.sql`.

#### `pgque.drop_queue(queue text) → integer`

Drops `queue`. Fails if consumers are still attached.
Grant: `pgque_admin`. Source: `sql/pgque.sql`.

#### `pgque.drop_queue(queue text, force bool) → integer`

Drops `queue`. When `force` is true, unregisters all attached consumers first.
Grant: `pgque_admin`. Source: `sql/pgque.sql`.

#### `pgque.set_queue_config(queue text, param text, value text) → integer`

Sets one queue parameter. Accepted `param` values (without the `queue_` prefix): `ticker_max_count`, `ticker_max_lag`, `ticker_idle_period`, `ticker_paused`, `rotation_period`, `external_ticker`, `max_retries`.
Grant: `pgque_admin`. Source: `sql/pgque.sql` (extended in `sql/pgque-additions/queue_max_retries.sql`).

Observable behavior: numeric/interval settings are range-checked (`max_retries >= 0`; ticker counts/lags/idle/rotation periods must be positive). Passing SQL `NULL` resets the column to its schema default.

```sql
select pgque.set_queue_config('orders', 'max_retries', '10');
```

## Lifecycle

Functions in this section are deny-by-default: the schema-wide blanket `revoke execute … from public` in `sql/pgque-additions/roles.sql` strips PUBLIC, and only `pgque_admin` retains `execute on all functions`. Grant explicitly to additional roles if your policy needs broader access. `uninstall()` is doubly locked down — also explicitly revoked from `pgque_admin` — so only the schema/install owner (typically a superuser) can run it.

#### `pgque.start() → void`

Schedules four pg_cron jobs in the current database: `pgque_ticker` (every 1 s), `pgque_retry_events` (every 30 s), `pgque_maint` (every 30 s), and `pgque_rotate_step2` (every 10 s). Requires the `pg_cron` extension — errors if missing. Idempotent: calls `stop()` first.

The `pgque_ticker` job calls `CALL pgque.ticker_loop()` (not `SELECT pgque.ticker()`). `ticker_loop` is the sub-second driver: pg_cron's minimum schedule is 1 s, but the procedure internally re-ticks every `pgque.config.tick_period_ms` (default 100 ms = 10 ticks/sec) and commits between iterations. To change the rate, call `pgque.set_tick_period_ms(ms)` — no need to call `start()` again.
Grant: `pgque_admin`. Source: `sql/pgque-additions/lifecycle.sql`.

#### `pgque.ticker_loop() → procedure`

Sub-second tick driver. Runs inside one pg_cron slot (1-second cadence) and re-invokes `pgque.ticker()` every `pgque.config.tick_period_ms` ms, committing between iterations so each tick gets its own transaction. Defined as a `PROCEDURE` (not a function) because Postgres only allows mid-flight `COMMIT` inside procedures — and forbids combining `COMMIT` with a `SET` clause, which is why the body is fully schema-qualified rather than pinning `search_path`.

Not normally called by hand; `pgque.start()` schedules it on pg_cron. Use `select pgque.set_tick_period_ms(ms)` to change cadence, or call `pgque.ticker()` directly to force a single tick out-of-band.
Grant: `pgque_admin`. Source: `sql/pgque-additions/lifecycle.sql`.

#### `pgque.set_tick_period_ms(ms integer) → integer`

Sets `pgque.config.tick_period_ms`. Default is 100 ms (10 ticks/sec). Allowed values are exact divisors of 1000 in the 1..1000 ms range. Returns the value that was set; raises if out of range, non-divisor, or NULL. Effective on the next pg_cron slot (≤1 s) without rescheduling.

```sql
select pgque.set_tick_period_ms(50);    -- 20 ticks/sec
select pgque.set_tick_period_ms(1000);  -- 1 tick/sec (the pg_cron floor; pgqd-compatible)
```

Trade-offs at higher rates: more WAL per second, more metadata-table churn, more NOTIFY traffic. See [docs/three-latencies.md](three-latencies.md) for the table.
Grant: `pgque_admin`. Source: `sql/pgque-additions/lifecycle.sql`.

#### `pgque.stop() → void`

Unschedules the pg_cron jobs set up by `start()` and clears the stored job IDs. Safe to call if `pg_cron` is absent.
Grant: `pgque_admin`. Source: `sql/pgque-additions/lifecycle.sql`.

#### `pgque.status() → table(component text, status text, detail text)`

Returns a diagnostic report with one row per component: Postgres version, PgQue version, ticker/maintenance job status, queue count, and consumer count.
Grant: `pgque_admin`. Source: `sql/pgque-additions/lifecycle.sql`.

```sql
select * from pgque.status();
```

#### `pgque.version() → text`

Returns the installed PgQue version string (set by `build/transform.sh` at assembly time; varies per build).
Grant: `pgque_reader`. Source: `sql/pgque-additions/lifecycle.sql`.

#### `pgque.maint() → integer`

Runs one maintenance cycle: rotation step 1 plus any queue extra-maint hooks registered via `pgque.queue_extra_maint`. Rotation step 2 is intentionally skipped (it must run in its own transaction and is scheduled separately by `start()`); retry-queue processing is **not** performed here — call `pgque.maint_retry_events()` as a separate scheduled job. Returns the total number of operations performed.
Grant: `pgque_admin`. Source: `sql/pgque-api/maint.sql`.

#### `pgque.maint_retry_events() → integer`

Moves due rows from `pgque.retry_queue` back into queue event tables so they appear in the next tick. Must be called periodically when using `nack()` with retry — `pgque.start()` schedules it as `pgque_retry_events` every 30 s. When driving the scheduler manually, call this alongside `pgque.maint()`. The re-inserted rows carry `pg_current_xact_id()` as their `ev_txid`, so the subsequent `pgque.ticker` call must run in a **separate** transaction — see the [snapshot rule](#snapshot-rule).

```sql
select pgque.maint_retry_events(); -- every 30 seconds, for nack/retry redelivery
select pgque.maint();              -- every 30 seconds, for rotation
```

Grant: `pgque_admin`. Source: `sql/pgque.sql`.

#### `pgque.ticker() → bigint`

Issues ticks for all unpaused, non-external queues. Returns the number of queues ticked. Each call must run in its own transaction (it records a `pg_snapshot` for batch visibility, and the snapshot must be committed before the next tick records its own).

Under `pg_cron`, this is invoked from `pgque.ticker_loop()` at the configured `tick_period_ms` cadence. When driving the scheduler manually, loop this at your target rate (default in PgQue's pg_cron path: every 100 ms).
Grant: `pgque_admin`. Source: `sql/pgque.sql`.

#### `pgque.ticker(queue text) → bigint`

Checks whether a tick is due for `queue` and inserts one if so. Returns the tick id (or `NULL` if no tick was created).
Grant: `pgque_admin`. Source: `sql/pgque.sql`.

> Note: a 4-argument `ticker(queue, tick_id, timestamp, event_seq)` overload exists for queues configured with `external_ticker = true` (pushing ticks from an external clock source). External ticks must be monotonic: `tick_id` must increase, and `event_seq` must not move backwards.

#### `pgque.force_next_tick(queue text) → bigint`

Forces the next `pgque.ticker()` call to materialise a tick for `queue` without waiting for the `ticker_max_count` or `ticker_max_lag` thresholds. Bumps `queue_event_seq` by `ticker_max_count * 2 + 1000` to simulate a burst of events. Does **not** insert a tick itself — call `pgque.ticker()` (or `pgque.ticker(queue)`) afterwards to materialise the tick. Returns the current last tick id (from the most recent existing tick, not a new one). Useful in tests and demos; not for production hot paths. Canonical idiom:

```sql
select pgque.force_next_tick('orders');
select pgque.ticker();
```

Grant: `pgque_admin`. Source: `sql/pgque-additions/tick_helpers.sql`.

#### `pgque.force_tick(queue text) → bigint`

Alias for `pgque.force_next_tick`. Retained for compatibility with upstream PgQ (the historical name); identical behavior. The name is misleading — the function does not insert a tick by itself, it only bumps the event sequence so the next `pgque.ticker()` call inserts one. Raises if the queue is missing, ticker-paused, or configured for an external ticker. Prefer `force_next_tick` in new code.
Grant: `pgque_admin`. Source: `sql/pgque.sql`.

> The `force_next_tick` → `ticker` → `receive` chain (or legacy `force_tick` alias) must run across separate transactions for the consumer to see the events you just sent. See the [snapshot rule](#snapshot-rule).

#### `pgque.uninstall() → void`

Calls `stop()` (if pg_cron is present) and then `drop schema pgque cascade`. Roles (`pgque_reader`, `pgque_writer`, `pgque_admin`) are not dropped and must be removed manually if desired. `execute` is revoked from both `pgque_admin` (explicit) and PUBLIC (via the schema-wide blanket revoke), so only the schema/install owner (typically a superuser) can run it.
Grant: superuser / schema owner only (revoked from both `pgque_admin` and PUBLIC). Source: `sql/pgque-additions/lifecycle.sql`.

## Observability

All observability functions here are granted to `pgque_reader`. They flow up to `pgque_admin` (which is a member of both `pgque_reader` and `pgque_writer`) but do **not** flow to `pgque_writer` — that role is producer-only and does not inherit reader privileges. Apps that produce + consume must hold both roles.

#### `pgque.get_queue_info() → setof record`

Returns one row per queue with ticker config and live stats. Grant: `pgque_reader`. Source: `sql/pgque.sql`.

| Out column                    | Type          |
|-------------------------------|---------------|
| `queue_name`                  | `text`        |
| `queue_ntables`               | `integer`     |
| `queue_cur_table`             | `integer`     |
| `queue_rotation_period`       | `interval`    |
| `queue_switch_time`           | `timestamptz` |
| `queue_external_ticker`       | `boolean`     |
| `queue_ticker_paused`         | `boolean`     |
| `queue_ticker_max_count`      | `integer`     |
| `queue_ticker_max_lag`        | `interval`    |
| `queue_ticker_idle_period`    | `interval`    |
| `ticker_lag`                  | `interval`    |
| `ev_per_sec`                  | `float8`      |
| `ev_new`                      | `bigint`      |
| `last_tick_id`                | `bigint`      |

#### `pgque.get_queue_info(queue text) → setof record`

Same columns as the 0-arg form, filtered to one queue. Grant: `pgque_reader`. Source: `sql/pgque.sql`.

#### `pgque.get_consumer_info() → setof record`

Returns one row per consumer across all queues. Grant: `pgque_reader`. Source: `sql/pgque.sql`.

| Out column        | Type       |
|-------------------|------------|
| `queue_name`      | `text`     |
| `consumer_name`   | `text`     |
| `lag`             | `interval` |
| `last_seen`       | `interval` |
| `last_tick`       | `bigint`   |
| `current_batch`   | `bigint`   |
| `next_tick`       | `bigint`   |
| `pending_events`  | `bigint`   |

#### `pgque.get_consumer_info(queue text) → setof record`

Same columns, filtered to one queue. Grant: `pgque_reader`. Source: `sql/pgque.sql`.

#### `pgque.get_consumer_info(queue text, consumer text) → setof record`

Same columns, filtered to one `(queue, consumer)` pair. Either argument may be `NULL` to widen the selection. Grant: `pgque_reader`. Source: `sql/pgque.sql`.

#### `pgque.get_batch_info(batch_id bigint) → record`

Inspects an active batch. Grant: `pgque_reader`. Source: `sql/pgque.sql`.

| Out column        | Type          |
|-------------------|---------------|
| `queue_name`      | `text`        |
| `consumer_name`   | `text`        |
| `batch_start`     | `timestamptz` |
| `batch_end`       | `timestamptz` |
| `prev_tick_id`    | `bigint`      |
| `tick_id`         | `bigint`      |
| `lag`             | `interval`    |
| `seq_start`       | `bigint`      |
| `seq_end`         | `bigint`      |

## Dead letter queue

PgQ has a retry queue but no dead letter queue; PgQue adds one. Messages land here when `nack()` is called and `retry_count >= max_retries`.

### `pgque.dead_letter` table

| Column            | Type          | Nullable |
|-------------------|---------------|----------|
| `dl_id`           | `bigserial`   | PK       |
| `dl_queue_id`     | `int4`        | no (FK → `pgque.queue`, `on delete cascade`) |
| `dl_consumer_id`  | `int4`        | no (FK → `pgque.consumer`, `on delete cascade`) |
| `dl_time`         | `timestamptz` | no (default `now()`) |
| `dl_reason`       | `text`        | yes      |
| `ev_id`           | `bigint`      | no       |
| `ev_time`         | `timestamptz` | no       |
| `ev_txid`         | `xid8`        | yes      |
| `ev_retry`        | `int4`        | yes      |
| `ev_type`         | `text`        | yes      |
| `ev_data`         | `text`        | yes      |
| `ev_extra1..4`    | `text`        | yes      |

Grant: `select` to `pgque_reader`, `all` to `pgque_admin`.

#### `pgque.event_dead(batch_id bigint, event_id bigint, reason text, ev_time timestamptz, ev_txid xid8, ev_retry int4, ev_type text, ev_data text, ev_extra1 text default null, ev_extra2 text default null, ev_extra3 text default null, ev_extra4 text default null) → integer`

Inserts one row into `pgque.dead_letter`. Called internally by `pgque.nack()` when retries are exhausted — direct calls are rarely needed.
Grant: `pgque_admin`. Source: `sql/pgque-additions/dlq.sql`.

#### `pgque.dlq_inspect(queue text, limit_count int default 100) → setof pgque.dead_letter`

Returns the most recent dead-letter rows for `queue`, newest first.
Grant: `pgque_reader`. Source: `sql/pgque-additions/dlq.sql`.

#### `pgque.dlq_replay(dead_letter_id bigint) → bigint`

Re-inserts one dead-letter entry into its original queue and deletes it from `pgque.dead_letter`. Returns the new event id.
Grant: `pgque_writer` (replay is a produce action — it calls `insert_event` to put the event back on the queue). Source: `sql/pgque-additions/dlq.sql`.

#### `pgque.dlq_replay_all(queue text) → (replayed bigint, failed bigint, first_error text)`

Replays every dead-letter entry for `queue`. Per-event failures are isolated (one bad row does not abort the rest), surfaced via `raise warning`, and counted in `failed`; `first_error` carries the first failure's `dl_id` and `sqlerrm` for diagnostics.

Read the result with the columns by name:

```sql
select replayed, failed, first_error from pgque.dlq_replay_all('orders');
```

Grant: `pgque_writer` (replay is a produce action — it calls `insert_event` for each replayed entry). Source: `sql/pgque-additions/dlq.sql`.

#### `pgque.dlq_purge(queue text, older_than interval default '30 days') → integer`

Deletes dead-letter rows older than `older_than` for `queue`. Returns the row count deleted. Destructive — audit the entries first.
Grant: `pgque_admin`. Source: `sql/pgque-additions/dlq.sql`.

## PgQ primitives (advanced)

Available but most users should prefer the modern API above. These are the raw PgQ primitives — the modern API wraps them 1:1.

#### `pgque.insert_event(queue_name text, ev_type text, ev_data text) → bigint`

Inserts one event with no extra columns. Returns the event id.
Grant: `pgque_writer`. Source: `sql/pgque.sql`.

#### `pgque.insert_event(queue_name text, ev_type text, ev_data text, ev_extra1 text, ev_extra2 text, ev_extra3 text, ev_extra4 text) → bigint`

Inserts one event with the four `ev_extra*` columns populated.
Grant: `pgque_writer`. Source: `sql/pgque.sql`.

#### `pgque.register_consumer(queue_name text, consumer_id text) → integer`

Registers `consumer_id` on `queue_name`, starting from the most recent tick. Returns `1` for new, `0` if already registered.
Grant: `pgque_reader`. Source: `sql/pgque.sql`.

#### `pgque.register_consumer_at(queue_name text, consumer_name text, tick_pos bigint) → integer`

Registers a consumer at a specific historical tick id. Raises if the tick is not found.
Grant: `pgque_reader`. Source: `sql/pgque.sql`.

#### `pgque.unregister_consumer(queue_name text, consumer_name text) → integer`

Removes the subscription and retry-queue entries owned by this consumer on `queue_name`. Returns the number of subscriptions removed. Cooperative-aware: calling this on a `coop_main` with registered subconsumers raises; unregister subconsumers explicitly.
Grant: `pgque_reader`. Source: `sql/pgque.sql`.

#### `pgque.next_batch(queue_name text, consumer_name text) → bigint`

Activates the next batch for this consumer and returns its id, or `NULL` if no events are ready.
Grant: `pgque_reader`. Source: `sql/pgque.sql`.

#### `pgque.next_batch_info(queue_name text, consumer_name text) → record`

Same as `next_batch` but returns tick bounds alongside `batch_id`. Out columns: `batch_id`, `cur_tick_id`, `prev_tick_id`, `cur_tick_time`, `prev_tick_time`, `cur_tick_event_seq`, `prev_tick_event_seq`.
Grant: `pgque_reader`. Source: `sql/pgque.sql`.

#### `pgque.next_batch_custom(queue_name text, consumer_name text, min_lag interval, min_count int4, min_interval interval) → record`

Activates the next batch with custom size/age constraints. Same out columns as `next_batch_info`. Cooperative-aware: this 5-arg legacy form raises if the named consumer is a `coop_main` with at least one registered member, or if it is a `coop_member` row — in both cases the error message directs the caller to the 7-arg cooperative form. Normal consumers and `coop_main` rows with no members pass through.
Grant: `pgque_reader`. Source: `sql/pgque.sql`.

#### `pgque.get_batch_events(batch_id bigint) → setof record`

Streams the events in a batch. Out columns: `ev_id bigint`, `ev_time timestamptz`, `ev_txid bigint`, `ev_retry int4`, `ev_type text`, `ev_data text`, `ev_extra1..4 text`.
Grant: `pgque_reader`. Source: `sql/pgque.sql`.

#### `pgque.get_batch_cursor(batch_id bigint, cursor_name text, quick_limit int4) → setof record`

Declares a server-side cursor over the batch and returns the first `quick_limit` events. Remaining events can be fetched with `fetch … from <cursor_name>`.
Grant: `pgque_admin` only. Source: `sql/pgque.sql`.

#### `pgque.get_batch_cursor(batch_id bigint, cursor_name text, quick_limit int4, extra_where text) → setof record`

Same as above with an additional `where` filter applied inside the cursor.
Grant: `pgque_admin` only. Source: `sql/pgque.sql`.

> **Security:** `extra_where` is a **trusted SQL fragment**, not a parameter — it is concatenated verbatim into the cursor's `select`. A caller that controls `extra_where` can inject arbitrary predicates (including `UNION ALL`) and forge rows in the returned stream. This behavior is inherited from upstream PgQ and is gated behind `pgque_admin` for that reason. **Never pass user-controlled text as `extra_where`**, even from admin code paths; if you need filtering driven by application input, fetch the batch with `pgque.get_batch_events()` and filter in the application or in a separate parameterized query.

#### `pgque.finish_batch(batch_id bigint) → integer`

Closes the batch and advances the subscription's `last_tick`. Returns `1` on success, `0` with a warning if the batch was not found. Cooperative-aware: `coop_member` batches clear the member cursor; `coop_main` batches are rejected.
Grant: `pgque_reader`. Source: `sql/pgque.sql`.

#### `pgque.event_retry(batch_id bigint, event_id bigint, retry_time timestamptz) → integer`

Puts one event back onto the retry queue with an absolute re-delivery time. Returns `1` on success, `0` if already queued for retry.
Grant: `pgque_reader`. Source: `sql/pgque.sql`.

#### `pgque.event_retry(batch_id bigint, event_id bigint, retry_seconds integer) → integer`

Same as above but takes a relative delay in seconds.
Grant: `pgque_reader`. Source: `sql/pgque.sql`.

#### `pgque.batch_retry(batch_id bigint, retry_seconds integer) → integer`

Re-queues every event in the batch after `retry_seconds`. Returns the number of events enqueued for retry.
Grant: `pgque_admin`. Source: `sql/pgque.sql`.

## Trigger helpers (change-data-capture)

Table triggers that enqueue a PgQue event for every INSERT / UPDATE / DELETE. All three return `trigger`; attach them via `CREATE TRIGGER … EXECUTE PROCEDURE pgque.<name>('queue_name', …)`. Grant: `pgque_admin`. Source: `sql/pgque.sql` (inherited from PgQ).

#### `pgque.jsontriga() → trigger`

Emits row data as JSON (`ev_data` is a JSON blob, `ev_extra1` is the fully-qualified table name). Supports optional trigger args: `SKIP`, `backup`, `ignore=…`, `pkey=…`, `when=…`, `ev_type=…`, `ev_extra1..4=…`.

#### `pgque.logutriga() → trigger`

Emits row data as URL-encoded key/value pairs (`key1=v1&key2=v2`). Useful for legacy pipelines that already consume `logutriga`.

#### `pgque.sqltriga() → trigger`

Emits row data as ready-to-apply SQL (`INSERT … VALUES …`, `UPDATE … SET … WHERE …`, `DELETE FROM … WHERE …`) in `ev_data`.

## Message type

#### `pgque.message` (composite)

Returned by `pgque.receive()` and consumed by `pgque.nack()`.

| Field          | Type          | Notes |
|----------------|---------------|-------|
| `msg_id`       | `bigint`      | event id (`ev_id`) |
| `batch_id`     | `bigint`      | batch containing this message |
| `type`         | `text`        | event type (`ev_type`); may be null |
| `payload`      | `text`        | event data (`ev_data`); cast to `jsonb` if needed |
| `retry_count`  | `int4`        | `NULL` on first delivery, incremented on retry |
| `created_at`   | `timestamptz` | original `ev_time` |
| `extra1`       | `text`        | `ev_extra1`; may be null |
| `extra2`       | `text`        | `ev_extra2`; may be null |
| `extra3`       | `text`        | `ev_extra3`; may be null |
| `extra4`       | `text`        | `ev_extra4`; may be null |

## Roles and grants

Three roles. `pgque_reader` (consume) and `pgque_writer` (produce) are **siblings**, not parent/child — this mirrors upstream PgQ's role model and prevents a producer-only role from acking another consumer's batch. `pgque_admin` is a member of both. Source: `sql/pgque-additions/roles.sql` (plus colocated grants in `sql/pgque-api/*.sql` and `sql/pgque-additions/dlq.sql`).

Apps that produce **and** consume must be granted both `pgque_reader` and `pgque_writer` explicitly.

### Roles are global, not per-queue

PgQue roles are coarse **database-level** roles. They are intended for trusted applications and operators sharing the same database, not as per-queue or per-tenant isolation for mutually untrusted applications.

**What this means in practice:**

- `pgque_reader` gets `select` on **all** tables in the `pgque` schema — it can read events from any queue. It can also call `receive`, `ack`, and `nack` on **any** queue with **any** consumer name. A reader granted for queue A can call `pgque.ack(batch_id)` on a batch opened by a consumer on queue B.
- `pgque_writer` can produce to **any** queue (`pgque.send`, `pgque.send_batch`, `pgque.insert_event`).
- There is **no per-queue ACL** and no per-tenant isolation built into PgQue. Queue names and consumer names are plain strings — any role with the matching grant can interact with them.

This is intentional, by design. The batch-ID-based primitives (`ack`, `nack`, `event_retry`) operate on IDs and do not enforce ownership; the producer/consumer split closes only the producer-vs-consumer boundary, not the consumer-vs-consumer one.

**Recommended isolation patterns** if you need mutually untrusted tenants in one database:

- Run separate PgQue installs in separate schemas per tenant (not yet officially supported — track the roadmap).
- Use separate databases per tenant and connect each tenant's application to its own database.
- Wrap the PgQue API in app-owned stored functions that enforce tenant ownership before delegating to `pgque.*`, and grant only those wrapper functions to tenant roles.

| Role           | Functions granted (direct)                                                                                                                                                                                                                                              |
|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `pgque_reader` | `get_queue_info()`, `get_queue_info(text)`, `get_consumer_info()`, `get_consumer_info(text)`, `get_consumer_info(text, text)`, `get_batch_info(bigint)`, `version()`, `dlq_inspect(text, int)`; `select` on all tables incl. `pgque.dead_letter`; consumer primitives (`register_consumer`, `register_consumer_at`, `unregister_consumer`, `next_batch`, `next_batch_info`, `next_batch_custom`, `get_batch_events`, `finish_batch`, `event_retry` int + timestamptz); modern consume API (`subscribe`, `unsubscribe`, `receive`, `ack`, `nack`); experimental cooperative API (`register_subconsumer`, `unregister_subconsumer`, `subscribe_subconsumer`, `unsubscribe_subconsumer`, cooperative `next_batch`, cooperative `next_batch_custom`, `receive_coop`, `touch_subconsumer`)                        |
| `pgque_writer` | `insert_event` (3, 7), all `send*`, all `send_batch*`, `dlq_replay`, `dlq_replay_all`. **Does not inherit `pgque_reader`** — a producer-only role cannot ack/finish/inspect consumer batches. |
| `pgque_admin`  | Member of both `pgque_reader` and `pgque_writer`, plus `event_dead`, `dlq_purge`, `all` on `pgque` schema, `all` on all tables and sequences, `execute` on all functions — **except** `uninstall()` and internal `insert_event_bulk()` which are explicitly revoked                                                            |

`pgque.uninstall()` is revoked from both `pgque_admin` (explicitly) and PUBLIC (via the schema-wide blanket revoke). Internal `pgque.insert_event_bulk()` is also revoked from `pgque_admin`; callers must use `send_batch()` wrappers. Only the schema/install owner (typically a superuser) can run `uninstall()` or the internal primitive directly. All other functions not listed in the table above retain `execute` only for `pgque_admin` (the schema-wide blanket revoke from PUBLIC applies, and `pgque_admin` is granted `execute on all functions`) — notably the lifecycle helpers `start`, `stop`, `status`, `maint`, `maint_retry_events`, `ticker`, `force_next_tick` (and its alias `force_tick`), and the queue-management helpers `create_queue`, `drop_queue`, `set_queue_config`. Grant these explicitly to additional roles if your policy demands it.

## Experimental (not in default install)

These objects are not part of `sql/pgque.sql`. Load them explicitly with `\i sql/experimental/<file>.sql`. **API and stability are not guaranteed** — signatures, semantics, and grants may change before promotion into the default install.

### `sql/experimental/delayed.sql`

#### `pgque.send_at(queue text, type text, payload jsonb, deliver_at timestamptz) → bigint`

Schedules delayed delivery. If `deliver_at <= now()` behaves like `insert_event` and returns the queue event id. Otherwise inserts into `pgque.delayed_events` and returns the scheduled-entry id (**not** a queue event id).

#### `pgque.maint_deliver_delayed() → integer`

Moves due rows from `pgque.delayed_events` into their target queues. Intended to be called from `pgque.maint()` (the experimental file overrides `maint()` to chain this in).

### `sql/experimental/observability.sql`

All functions below are `security definer` with pinned `search_path` and are installed without explicit grants.

#### `pgque.queue_stats() → table(queue_name text, queue_id int4, depth bigint, oldest_msg_age interval, consumers int4, events_per_sec numeric, cur_table int4, rotation_age interval, rotation_period interval, ticker_paused boolean, last_tick_time timestamptz, last_tick_id bigint, dlq_count bigint)`

#### `pgque.consumer_stats() → table(queue_name text, consumer_name text, lag interval, pending_events bigint, last_batch_start timestamptz, batch_active boolean, batch_id bigint)`

#### `pgque.queue_health() → table(queue_name text, check_name text, status text, …)`

Operational checks (stuck ticker, rotation lag, DLQ growth).

#### `pgque.otel_metrics() → table(metric_name text, metric_type text, metric_value numeric, labels jsonb)`

OTel-compatible metric export rows.

#### `pgque.stuck_consumers(threshold interval default '1 hour') → table(queue_name text, consumer_name text, lag interval, last_active timestamptz)`

#### `pgque.in_flight(queue text) → table(consumer_name text, batch_id bigint, batch_age interval, estimated_events bigint)`

#### `pgque.throughput(queue text, period interval, bucket_size interval) → table(bucket_start timestamptz, events bigint, events_per_sec numeric)`

#### `pgque.error_rate(queue text, period interval, bucket_size interval) → table(bucket_start timestamptz, retries bigint, dead_letters bigint)`

### `sql/experimental/config_api.sql`

#### `pgque.create_queue(queue text, options jsonb) → integer`

Sugar overload: calls `create_queue(queue)` then applies each key in `options` via `set_queue_config`. Recognized keys include `max_retries`, `rotation_period`, `ticker_max_count`, `ticker_max_lag`, `ticker_idle_period`, `ticker_paused`. The 57-byte queue name limit applies here too (see the 1-arg overload above).

#### `pgque.pause_queue(queue text) → void`

Shortcut for `set_queue_config(queue, 'ticker_paused', 'true')`.

#### `pgque.resume_queue(queue text) → void`

Shortcut for `set_queue_config(queue, 'ticker_paused', 'false')`.
