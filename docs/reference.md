# Function reference

Every function shipped in the v0.1 default install (`\i sql/logres.sql`). Each entry lists the signature, return type, the role it is granted to, and the source file. A short code example appears where the signature alone leaves the call ambiguous.

If you are new to logres, start with [tutorial.md](tutorial.md) — it walks the end-to-end `send` / `receive` / `ack` loop. Use this as the lookup table.

Each entry takes this form:

```
#### `logres.<name>(arg text, …) → returntype`

One-line description. Optional second line with a caveat.
Grant: `role_name` or `PUBLIC (default)`. Source: `sql/<path>`.
```

Functions shipped outside the default install are in the [Experimental](#experimental-not-in-default-install) section.

## Publishing

All `send*` functions reduce to `logres.insert_event`. The `text` overloads are the fast path (bytes flow through verbatim); the `jsonb` overloads validate and canonicalize via Postgres before storing. Postgres `text` cannot store NUL (`\x00`), so raw binary must be base64/hex-encoded by the caller. See [SPECx.md §4.1](../blueprints/SPECx.md) for details on overload resolution.

#### `logres.send(queue text, payload jsonb) → bigint`

Inserts `payload` into `queue` with event type `'default'`. Returns the event id.
Grant: `logres_writer`. Source: `sql/logres-api/send.sql`.

```sql
select logres.send('orders', '{"order_id": 42}'::jsonb);
```

#### `logres.send(queue text, payload text) → bigint`

Fast-path send: stores the payload bytes verbatim, no JSON parse. Untyped string literals (`'…'`) resolve to this overload. Returns the event id.
Grant: `logres_writer`. Source: `sql/logres-api/send.sql`.

#### `logres.send(queue text, type text, payload jsonb) → bigint`

Same as the 2-arg `jsonb` overload, but with an explicit event type. Returns the event id.
Grant: `logres_writer`. Source: `sql/logres-api/send.sql`.

```sql
select logres.send('orders', 'order.created', '{"order_id": 42}'::jsonb);
```

#### `logres.send(queue text, type text, payload text) → bigint`

Fast-path send with explicit event type. Returns the event id.
Grant: `logres_writer`. Source: `sql/logres-api/send.sql`.

#### `logres.send_batch(queue text, type text, payloads jsonb[]) → bigint[]`

Inserts each element of `payloads` into `queue` within a single transaction. Returns the array of event ids in the same order.
Grant: `logres_writer`. Source: `sql/logres-api/send.sql`.

```sql
select logres.send_batch('orders', 'order.created',
    array['{"id":1}', '{"id":2}']::jsonb[]);
```

#### `logres.send_batch(queue text, type text, payloads text[]) → bigint[]`

Fast-path batch send. Returns the array of event ids.
Grant: `logres_writer`. Source: `sql/logres-api/send.sql`.

## Consuming

The consume API wraps `logres.next_batch`, `logres.get_batch_events`, `logres.finish_batch`, and `logres.event_retry`. Typical loop: `receive` → process → `ack` (or `nack` on failure).

#### `logres.receive(queue text, consumer text, max_return int default 100) → setof logres.message`

Pulls the next batch for `consumer` on `queue` and streams up to `max_return` messages. Returns an empty set if no batch is available. Each row is a `logres.message` composite (see [§Message type](#message-type)).
Grant: `logres_writer`. Source: `sql/logres-api/receive.sql`.

```sql
select * from logres.receive('orders', 'processor', 100);
```

#### `logres.ack(batch_id bigint) → integer`

Closes the batch and advances the consumer position. Modern alias for `logres.finish_batch`. Returns `1` on success, `0` if the batch was not found.
Grant: `logres_writer`. Source: `sql/logres-api/receive.sql`.

#### `logres.nack(batch_id bigint, msg logres.message, retry_after interval default '60 seconds', reason text default null) → integer`

Negative-acknowledges one message. If `msg.retry_count` is below the queue's `max_retries`, re-queues after `retry_after`; otherwise routes the event to `logres.dead_letter` via `logres.event_dead`. Returns `1`.
Grant: `logres_writer`. Source: `sql/logres-api/receive.sql`.

```sql
perform logres.nack(msg.batch_id, msg, interval '5 minutes', 'validation failed');
```

#### `logres.subscribe(queue text, consumer text) → integer`

Registers `consumer` on `queue`. Modern alias for `logres.register_consumer`. Returns `1` on new registration, `0` if already registered.
Grant: `logres_writer`. Source: `sql/logres-api/send.sql`.

#### `logres.unsubscribe(queue text, consumer text) → integer`

Removes the consumer (and its retry-queue entries) from `queue`. Modern alias for `logres.unregister_consumer`.
Grant: `logres_writer`. Source: `sql/logres-api/send.sql`.

## Queue management

#### `logres.create_queue(queue text) → integer`

Creates a queue with default settings (3 rotation tables, built-in ticker). Returns `1` if created, `0` if a queue with that name already exists.
Grant: PUBLIC (default). Source: `sql/logres.sql`.

#### `logres.drop_queue(queue text) → integer`

Drops `queue`. Fails if consumers are still attached.
Grant: PUBLIC (default). Source: `sql/logres.sql`.

#### `logres.drop_queue(queue text, force bool) → integer`

Drops `queue`. When `force` is true, unregisters all attached consumers first.
Grant: PUBLIC (default). Source: `sql/logres.sql`.

#### `logres.set_queue_config(queue text, param text, value text) → integer`

Sets one queue parameter. Accepted `param` values (without the `queue_` prefix): `ticker_max_count`, `ticker_max_lag`, `ticker_idle_period`, `ticker_paused`, `rotation_period`, `external_ticker`, `max_retries`.
Grant: PUBLIC (default). Source: `sql/logres.sql` (extended in `sql/logres-additions/queue_max_retries.sql`).

```sql
select logres.set_queue_config('orders', 'max_retries', '10');
```

## Lifecycle

Most functions in this section are left on PUBLIC by default — tighten with `revoke execute … from public` if your policy demands it. `uninstall()` is explicitly revoked from `logres_admin`, but PUBLIC execute is not revoked by default (see its entry below).

#### `logres.start() → void`

Schedules three pg_cron jobs in the current database: `logres_ticker` (every 1 s), `logres_maint` (every 30 s), and `logres_rotate_step2` (every 10 s). Requires the `pg_cron` extension — errors if missing. Idempotent: calls `stop()` first.
Grant: PUBLIC (default). Source: `sql/logres-additions/lifecycle.sql`.

#### `logres.stop() → void`

Unschedules the pg_cron jobs set up by `start()` and clears the stored job IDs. Safe to call if `pg_cron` is absent.
Grant: PUBLIC (default). Source: `sql/logres-additions/lifecycle.sql`.

#### `logres.status() → table(component text, status text, detail text)`

Returns a diagnostic report with one row per component: Postgres version, logres version, ticker/maintenance job status, queue count, and consumer count.
Grant: PUBLIC (default). Source: `sql/logres-additions/lifecycle.sql`.

```sql
select * from logres.status();
```

#### `logres.version() → text`

Returns the installed logres version string (set by `build/transform.sh` at assembly time; varies per build).
Grant: `logres_reader`. Source: `sql/logres-additions/lifecycle.sql`.

#### `logres.maint() → integer`

Runs one maintenance cycle: rotation step 1, retry-queue processing, and vacuum of expired tables. Rotation step 2 is intentionally skipped — it must run in its own transaction and is scheduled separately by `start()`. Returns the total number of operations performed.
Grant: PUBLIC (default). Source: `sql/logres-api/maint.sql`.

#### `logres.ticker() → bigint`

Issues ticks for all unpaused, non-external queues. Returns the number of queues ticked. Call this from your scheduler (every 1 s by default) when not using pg_cron.
Grant: PUBLIC (default). Source: `sql/logres.sql`.

#### `logres.ticker(queue text) → bigint`

Checks whether a tick is due for `queue` and inserts one if so. Returns the tick id (or `NULL` if no tick was created).
Grant: PUBLIC (default). Source: `sql/logres.sql`.

> Note: a 4-argument `ticker(queue, tick_id, timestamp, event_seq)` overload exists for queues configured with `external_ticker = true` (pushing ticks from an external clock source). Not covered here.

#### `logres.force_tick(queue text) → bigint`

Bypasses the tick thresholds for one queue and forces a tick immediately. Useful in tests and demos; not for production hot paths. Returns the current last tick id.
Grant: PUBLIC (default). Source: `sql/logres.sql`.

#### `logres.uninstall() → void`

Calls `stop()` (if pg_cron is present) and then `drop schema logres cascade`. Roles (`logres_reader`, `logres_writer`, `logres_admin`) are not dropped and must be removed manually if desired.
Grant: `execute` is revoked from both `logres_admin` and `PUBLIC` — superuser / schema owner only. Source: `sql/logres-additions/lifecycle.sql`.

## Observability

All observability functions here are granted to `logres_reader` and flow up to `logres_writer` and `logres_admin` via role inheritance.

#### `logres.get_queue_info() → setof record`

Returns one row per queue with ticker config and live stats. Grant: `logres_reader`. Source: `sql/logres.sql`.

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

#### `logres.get_queue_info(queue text) → setof record`

Same columns as the 0-arg form, filtered to one queue. Grant: `logres_reader`. Source: `sql/logres.sql`.

#### `logres.get_consumer_info() → setof record`

Returns one row per consumer across all queues. Grant: `logres_reader`. Source: `sql/logres.sql`.

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

#### `logres.get_consumer_info(queue text) → setof record`

Same columns, filtered to one queue. Grant: `logres_reader`. Source: `sql/logres.sql`.

#### `logres.get_consumer_info(queue text, consumer text) → setof record`

Same columns, filtered to one `(queue, consumer)` pair. Either argument may be `NULL` to widen the selection. Grant: `logres_reader`. Source: `sql/logres.sql`.

#### `logres.get_batch_info(batch_id bigint) → record`

Inspects an active batch. Grant: `logres_reader`. Source: `sql/logres.sql`.

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

PgQ has a retry queue but no dead letter queue; logres adds one. Messages land here when `nack()` is called and `retry_count >= max_retries`.

### `logres.dead_letter` table

| Column            | Type          | Nullable |
|-------------------|---------------|----------|
| `dl_id`           | `bigserial`   | PK       |
| `dl_queue_id`     | `int4`        | no (FK → `logres.queue`, `on delete cascade`) |
| `dl_consumer_id`  | `int4`        | no (FK → `logres.consumer`, `on delete cascade`) |
| `dl_time`         | `timestamptz` | no (default `now()`) |
| `dl_reason`       | `text`        | yes      |
| `ev_id`           | `bigint`      | no       |
| `ev_time`         | `timestamptz` | no       |
| `ev_txid`         | `xid8`        | yes      |
| `ev_retry`        | `int4`        | yes      |
| `ev_type`         | `text`        | yes      |
| `ev_data`         | `text`        | yes      |
| `ev_extra1..4`    | `text`        | yes      |

Grant: `select` to `logres_reader`, `all` to `logres_admin`.

#### `logres.event_dead(batch_id bigint, event_id bigint, reason text, ev_time timestamptz, ev_txid xid8, ev_retry int4, ev_type text, ev_data text, ev_extra1 text default null, ev_extra2 text default null, ev_extra3 text default null, ev_extra4 text default null) → integer`

Inserts one row into `logres.dead_letter`. Called internally by `logres.nack()` when retries are exhausted — direct calls are rarely needed.
Grant: `logres_admin`. Source: `sql/logres-additions/dlq.sql`.

#### `logres.dlq_inspect(queue text, limit_count int default 100) → setof logres.dead_letter`

Returns the most recent dead-letter rows for `queue`, newest first.
Grant: `logres_reader`. Source: `sql/logres-additions/dlq.sql`.

#### `logres.dlq_replay(dead_letter_id bigint) → bigint`

Re-inserts one dead-letter entry into its original queue and deletes it from `logres.dead_letter`. Returns the new event id.
Grant: `logres_writer`. Source: `sql/logres-additions/dlq.sql`.

#### `logres.dlq_replay_all(queue text) → integer`

Replays every dead-letter entry for `queue`. Returns the number of events replayed.
Grant: `logres_writer`. Source: `sql/logres-additions/dlq.sql`.

#### `logres.dlq_purge(queue text, older_than interval default '30 days') → integer`

Deletes dead-letter rows older than `older_than` for `queue`. Returns the row count deleted. Destructive — audit the entries first.
Grant: `logres_admin`. Source: `sql/logres-additions/dlq.sql`.

## PgQ primitives (advanced)

Available but most users should prefer the modern API above. These are the raw PgQ primitives — the modern API wraps them 1:1.

#### `logres.insert_event(queue_name text, ev_type text, ev_data text) → bigint`

Inserts one event with no extra columns. Returns the event id.
Grant: `logres_writer`. Source: `sql/logres.sql`.

#### `logres.insert_event(queue_name text, ev_type text, ev_data text, ev_extra1 text, ev_extra2 text, ev_extra3 text, ev_extra4 text) → bigint`

Inserts one event with the four `ev_extra*` columns populated.
Grant: `logres_writer`. Source: `sql/logres.sql`.

#### `logres.register_consumer(queue_name text, consumer_id text) → integer`

Registers `consumer_id` on `queue_name`, starting from the most recent tick. Returns `1` for new, `0` if already registered.
Grant: `logres_writer`. Source: `sql/logres.sql`.

#### `logres.register_consumer_at(queue_name text, consumer_name text, tick_pos bigint) → integer`

Registers a consumer at a specific historical tick id. Raises if the tick is not found.
Grant: `logres_writer`. Source: `sql/logres.sql`.

#### `logres.unregister_consumer(queue_name text, consumer_name text) → integer`

Removes the subscription and retry-queue entries owned by this consumer on `queue_name`. Returns the number of subscriptions removed.
Grant: `logres_writer`. Source: `sql/logres.sql`.

#### `logres.next_batch(queue_name text, consumer_name text) → bigint`

Activates the next batch for this consumer and returns its id, or `NULL` if no events are ready.
Grant: `logres_writer`. Source: `sql/logres.sql`.

#### `logres.next_batch_info(queue_name text, consumer_name text) → record`

Same as `next_batch` but returns tick bounds alongside `batch_id`. Out columns: `batch_id`, `cur_tick_id`, `prev_tick_id`, `cur_tick_time`, `prev_tick_time`, `cur_tick_event_seq`, `prev_tick_event_seq`.
Grant: `logres_writer`. Source: `sql/logres.sql`.

#### `logres.next_batch_custom(queue_name text, consumer_name text, min_lag interval, min_count int4, min_interval interval) → record`

Activates the next batch with custom size/age constraints. Same out columns as `next_batch_info`.
Grant: `logres_writer`. Source: `sql/logres.sql`.

#### `logres.get_batch_events(batch_id bigint) → setof record`

Streams the events in a batch. Out columns: `ev_id bigint`, `ev_time timestamptz`, `ev_txid bigint`, `ev_retry int4`, `ev_type text`, `ev_data text`, `ev_extra1..4 text`.
Grant: `logres_writer`. Source: `sql/logres.sql`.

#### `logres.get_batch_cursor(batch_id bigint, cursor_name text, quick_limit int4) → setof record`

Declares a server-side cursor over the batch and returns the first `quick_limit` events. Remaining events can be fetched with `fetch … from <cursor_name>`.
Grant: PUBLIC (default). Source: `sql/logres.sql`.

#### `logres.get_batch_cursor(batch_id bigint, cursor_name text, quick_limit int4, extra_where text) → setof record`

Same as above with an additional `where` filter applied inside the cursor.
Grant: PUBLIC (default). Source: `sql/logres.sql`.

#### `logres.finish_batch(batch_id bigint) → integer`

Closes the batch and advances the subscription's `last_tick`. Returns `1` on success, `0` with a warning if the batch was not found.
Grant: `logres_writer`. Source: `sql/logres.sql`.

#### `logres.event_retry(batch_id bigint, event_id bigint, retry_time timestamptz) → integer`

Puts one event back onto the retry queue with an absolute re-delivery time. Returns `1` on success, `0` if already queued for retry.
Grant: `logres_writer`. Source: `sql/logres.sql`.

#### `logres.event_retry(batch_id bigint, event_id bigint, retry_seconds integer) → integer`

Same as above but takes a relative delay in seconds.
Grant: `logres_writer`. Source: `sql/logres.sql`.

#### `logres.batch_retry(batch_id bigint, retry_seconds integer) → integer`

Re-queues every event in the batch after `retry_seconds`. Returns the number of events enqueued for retry.
Grant: PUBLIC (default). Source: `sql/logres.sql`.

## Trigger helpers (change-data-capture)

Table triggers that enqueue a logres event for every INSERT / UPDATE / DELETE. All three return `trigger`; attach them via `CREATE TRIGGER … EXECUTE PROCEDURE logres.<name>('queue_name', …)`. Grant: PUBLIC (default). Source: `sql/logres.sql` (inherited from PgQ).

#### `logres.jsontriga() → trigger`

Emits row data as JSON (`ev_data` is a JSON blob, `ev_extra1` is the fully-qualified table name). Supports optional trigger args: `SKIP`, `backup`, `ignore=…`, `pkey=…`, `when=…`, `ev_type=…`, `ev_extra1..4=…`.

#### `logres.logutriga() → trigger`

Emits row data as URL-encoded key/value pairs (`key1=v1&key2=v2`). Useful for legacy pipelines that already consume `logutriga`.

#### `logres.sqltriga() → trigger`

Emits row data as ready-to-apply SQL (`INSERT … VALUES …`, `UPDATE … SET … WHERE …`, `DELETE FROM … WHERE …`) in `ev_data`.

## Message type

#### `logres.message` (composite)

Returned by `logres.receive()` and consumed by `logres.nack()`.

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

Three roles, with inheritance `logres_admin > logres_writer > logres_reader`. Source: `sql/logres-additions/roles.sql` (plus colocated grants in `sql/logres-api/*.sql` and `sql/logres-additions/dlq.sql`).

| Role           | Functions granted (direct)                                                                                                                                                                                                                                              |
|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `logres_reader` | `get_queue_info()`, `get_queue_info(text)`, `get_consumer_info()`, `get_consumer_info(text)`, `get_consumer_info(text, text)`, `get_batch_info(bigint)`, `version()`, `dlq_inspect(text, int)`; `select` on all tables incl. `logres.dead_letter`                        |
| `logres_writer` | everything `logres_reader` has, plus `insert_event` (3, 7), `register_consumer`, `register_consumer_at`, `unregister_consumer`, `next_batch`, `next_batch_info`, `next_batch_custom`, `get_batch_events`, `finish_batch`, `event_retry` (int, timestamptz), all `send*`, `send_batch*`, `subscribe`, `unsubscribe`, `receive`, `ack`, `nack`, `dlq_replay`, `dlq_replay_all` |
| `logres_admin`  | everything `logres_writer` has, plus `event_dead`, `dlq_purge`, `all` on `logres` schema, `all` on all tables and sequences, `execute` on all functions — **except** `uninstall()` which is explicitly revoked                                                            |

`logres.uninstall()` is revoked from `logres_admin`, but not from PUBLIC — any role that can connect can still call it and drop the schema. Tighten with `revoke execute on function logres.uninstall() from public;` if stricter control is required. All other functions not in the table above are on PUBLIC `execute` by default (notably the lifecycle helpers `start`, `stop`, `status`, `maint`, `ticker`, `force_tick`, and the queue-management helpers `create_queue`, `drop_queue`, `set_queue_config`) — revoke and re-grant explicitly if your policy demands it.

## Experimental (not in default install)

These objects are not part of `sql/logres.sql`. Load them explicitly with `\i sql/experimental/<file>.sql`. **API and stability are not guaranteed** — signatures, semantics, and grants may change before promotion into the default install.

### `sql/experimental/delayed.sql`

#### `logres.send_at(queue text, type text, payload jsonb, deliver_at timestamptz) → bigint`

Schedules delayed delivery. If `deliver_at <= now()` behaves like `insert_event` and returns the queue event id. Otherwise inserts into `logres.delayed_events` and returns the scheduled-entry id (**not** a queue event id).

#### `logres.maint_deliver_delayed() → integer`

Moves due rows from `logres.delayed_events` into their target queues. Intended to be called from `logres.maint()` (the experimental file overrides `maint()` to chain this in).

### `sql/experimental/observability.sql`

All functions below are `security definer` with pinned `search_path` and are installed without explicit grants.

#### `logres.queue_stats() → table(queue_name text, queue_id int4, depth bigint, oldest_msg_age interval, consumers int4, events_per_sec numeric, cur_table int4, rotation_age interval, rotation_period interval, ticker_paused boolean, last_tick_time timestamptz, last_tick_id bigint, dlq_count bigint)`

#### `logres.consumer_stats() → table(queue_name text, consumer_name text, lag interval, pending_events bigint, last_batch_start timestamptz, batch_active boolean, batch_id bigint)`

#### `logres.queue_health() → table(queue_name text, check_name text, status text, …)`

Operational checks (stuck ticker, rotation lag, DLQ growth).

#### `logres.otel_metrics() → table(metric_name text, metric_type text, metric_value numeric, labels jsonb)`

OTel-compatible metric export rows.

#### `logres.stuck_consumers(threshold interval default '1 hour') → table(queue_name text, consumer_name text, lag interval, last_active timestamptz)`

#### `logres.in_flight(queue text) → table(consumer_name text, batch_id bigint, batch_age interval, estimated_events bigint)`

#### `logres.throughput(queue text, period interval, bucket_size interval) → table(bucket_start timestamptz, events bigint, events_per_sec numeric)`

#### `logres.error_rate(queue text, period interval, bucket_size interval) → table(bucket_start timestamptz, retries bigint, dead_letters bigint)`

### `sql/experimental/config_api.sql`

#### `logres.create_queue(queue text, options jsonb) → integer`

Sugar overload: calls `create_queue(queue)` then applies each key in `options` via `set_queue_config`. Recognized keys include `max_retries`, `rotation_period`, `ticker_max_count`, `ticker_max_lag`, `ticker_idle_period`, `ticker_paused`.

#### `logres.pause_queue(queue text) → void`

Shortcut for `set_queue_config(queue, 'ticker_paused', 'true')`.

#### `logres.resume_queue(queue text) → void`

Shortcut for `set_queue_config(queue, 'ticker_paused', 'false')`.
