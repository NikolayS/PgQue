# PgQue observability

PgQue includes SQL-level observability helpers such as:

- `pgque.status()`
- `pgque.queue_stats()`
- `pgque.consumer_stats()`
- `pgque.queue_health()`
- `pgque.otel_metrics()`
- `pgque.stuck_consumers()`
- `pgque.in_flight()`

The README should not carry all of that detail. This document exists so the
front page stays small and sharp.

## Goals

- make queue depth visible
- make consumer lag visible
- highlight stalled or unhealthy queues
- support OTel-compatible metric export

## Operational truth

The most important signal is usually consumer lag relative to rotation period.

If slow consumers are allowed to drift too far behind, they can block rotation.
That is expected behavior and should be monitored explicitly.
