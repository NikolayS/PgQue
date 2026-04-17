# Examples

A few common PgQue patterns. For a guided first-run, see [the tutorial](tutorial.md). For every function signature, see [the reference](reference.md).

## Send with event type

The event type is an arbitrary string tag — consumers can filter on it.

```sql
select pgque.send('orders', 'order.created',
  '{"order_id": 42}'::jsonb);

select pgque.send('orders', 'order.shipped',
  '{"order_id": 42, "tracking": "1Z999AA10123456784"}'::jsonb);
```

## Batch send

Both `jsonb[]` and `text[]` overloads ship — see [the reference](reference.md) for when to pick which.

```sql
select pgque.send_batch('orders', 'order.created', array[
  '{"order_id": 1}'::jsonb,
  '{"order_id": 2}'::jsonb,
  '{"order_id": 3}'::jsonb
]);
```

## Fan-out with multiple consumers

Three subscribers on the same queue, each tracking its own cursor. Unlike `skip locked` queues, every consumer sees every event.

Subscribe **before** producing — a new consumer starts from the latest tick and will not see events that were sent before its `subscribe` call. Produce, tick, and then receive:

```sql
select pgque.subscribe('orders', 'audit_logger');
select pgque.subscribe('orders', 'notification_sender');
select pgque.subscribe('orders', 'analytics_pipeline');

select pgque.send('orders', 'order.created', '{"order_id": 1}'::jsonb);
select pgque.force_tick('orders');
select pgque.ticker();

select * from pgque.receive('orders', 'audit_logger', 100);
select * from pgque.receive('orders', 'notification_sender', 100);
```

Each `receive` returns the same event to its own consumer — no duplication on the producer side, independent cursors on the consumer side.

## Exactly-once processing (transactional pattern)

Wrap the receive, your writes, and the ack in one transaction. If it rolls back, the writes roll back and the ack rolls back together.

```sql
begin;
  create temp table msgs as
    select * from pgque.receive('orders', 'processor', 100);

  insert into processed_orders (order_id, status)
  select (payload::jsonb->>'order_id')::int, 'done'
  from msgs;

  select pgque.ack((select distinct batch_id from msgs limit 1));
commit;
```

Every row in `msgs` shares the same `batch_id`, so `select distinct batch_id from msgs limit 1` is safe. If you prefer extracting it directly, a PL/pgSQL block with `select batch_id into v_batch_id from msgs limit 1` reads the same.

## Recurring jobs with pg_cron

```sql
select cron.schedule('daily_report',
  '0 9 * * *',
  $$select pgque.send('jobs', 'report.generate',
      '{"type": "daily"}'::jsonb)$$);
```

## Dead letter queue inspection

`pgque.dlq_inspect()` lists entries for a queue; from there, replay a single row by its `dl_id` or purge anything older than a given interval.

```sql
select dl_id, dl_reason, ev_type, ev_data
from pgque.dlq_inspect('orders');

-- replay a single entry (returns the new ev_id)
select pgque.dlq_replay(42);

-- or drop entries older than 7 days
select pgque.dlq_purge('orders', interval '7 days');
```

See [the tutorial](tutorial.md) for the full DLQ flow including retry budgets and nack.
