# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# PgQue includes code derived from PgQ (ISC license,
# Marko Kreen / Skype Technologies OU).

"""PgqueClient -- thin Python wrapper over the pgque SQL API."""

import json
from typing import Any, Optional, Union

import psycopg

from .errors import (
    PgqueBatchNotFound,
    PgqueConnectionError,
    PgqueConsumerNotFound,
    PgqueError,
    PgqueQueueNotFound,
)
from .types import Event, Message


def connect(dsn: str, *, autocommit: bool = False) -> "PgqueClient":
    """Open a connection to PostgreSQL and return a ``PgqueClient``.

    The returned client owns the connection and must be closed via
    ``client.close()`` or used as a context manager.

    Args:
        dsn: libpq connection string (``postgresql://...``).
        autocommit: If True, the connection runs in autocommit mode.
            Useful for one-off scripts and consumers that prefer
            implicit transactions per statement.

    Raises:
        PgqueConnectionError: Connection could not be established.
    """
    try:
        conn = psycopg.connect(dsn, autocommit=autocommit)
    except psycopg.OperationalError as e:
        raise PgqueConnectionError(str(e)) from e
    return PgqueClient(conn, _owns_conn=True)


def _wrap_sql_error(e: Exception) -> PgqueError:
    """Map a raw psycopg error to a pgque exception subclass."""
    msg = str(e)
    low = msg.lower()
    if "queue not found" in low:
        return PgqueQueueNotFound(msg)
    if (
        "consumer not registered" in low
        or "consumer not found" in low
        or "not subscriber to queue" in low
    ):
        return PgqueConsumerNotFound(msg)
    if "batch not found" in low or "cannot find data for batch" in low:
        return PgqueBatchNotFound(msg)
    return PgqueError(msg)


class PgqueClient:
    """Thin wrapper around pgque SQL functions.

    By default, methods execute SQL against the wrapped connection
    without managing transactions; the caller decides when to
    ``commit()``/``rollback()``. If the connection is in autocommit
    mode, each statement is its own transaction.

    Use ``pgque.connect(dsn)`` to construct a client that owns its
    connection. Pass an existing ``psycopg.Connection`` to share one
    with application code.
    """

    def __init__(
        self,
        conn: psycopg.Connection,
        *,
        _owns_conn: bool = False,
    ):
        self.conn = conn
        self._owns_conn = _owns_conn

    # --- context manager / lifecycle ------------------------------------

    def __enter__(self) -> "PgqueClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying connection if owned by this client.

        If the client was constructed with an externally-managed
        connection, ``close()`` is a no-op.
        """
        if self._owns_conn and not self.conn.closed:
            self.conn.close()

    # --- producer -------------------------------------------------------

    def send(
        self,
        queue: str,
        payload: Any = None,
        *,
        type: str = "default",
    ) -> int:
        """Send a single message to a queue.

        Maps to ``pgque.send(queue, payload)`` or
        ``pgque.send(queue, type, payload)``.

        Args:
            queue: Target queue name.
            payload: Message payload. Accepted forms:

                - ``dict`` / ``list`` — JSON-serialised automatically.
                - ``str`` — must be **valid JSON text** (e.g.
                  ``'"hello"'``, ``'{"k": 1}'``, ``'42'``, ``'null'``).
                  The value is cast to ``jsonb`` by PostgreSQL. The
                  Python literal ``"hello"`` has content ``hello``,
                  which is not valid JSON; pass ``'"hello"'`` or
                  ``json.dumps("hello")`` instead.
                - ``None`` — stored as JSON ``null``.
                - :class:`Event` — ``type`` and ``payload`` are unpacked.

            type: Event type (default ``"default"``). Ignored if
                ``payload`` is an ``Event`` (its own ``type`` wins).

        Returns:
            The event ID assigned by pgque.
        """
        if isinstance(payload, Event):
            type = payload.type
            payload = payload.payload

        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload)
        elif payload is None:
            payload = "null"

        try:
            if type and type != "default":
                row = self.conn.execute(
                    "select pgque.send(%s, %s, %s::jsonb)",
                    (queue, type, payload),
                ).fetchone()
            else:
                row = self.conn.execute(
                    "select pgque.send(%s, %s::jsonb)",
                    (queue, payload),
                ).fetchone()
        except psycopg.Error as e:
            raise _wrap_sql_error(e) from e

        return row[0]

    def send_batch(
        self,
        queue: str,
        type: str,
        payloads: list,
    ) -> list[int]:
        """Send multiple messages in one SQL call.

        Maps to ``pgque.send_batch(queue, type, payloads[])`` and returns event
        IDs in input order. The call is atomic inside the current transaction.

        Payload encoding matches ``send``: ``dict``/``list`` values are JSON
        encoded, ``str`` values must already be valid JSON text, and ``None`` is
        stored as JSON ``null`` rather than SQL NULL.
        """
        json_payloads = [
            json.dumps(p) if isinstance(p, (dict, list))
            else ("null" if p is None else p)
            for p in payloads
        ]
        try:
            row = self.conn.execute(
                "select pgque.send_batch(%s, %s, %s::jsonb[])",
                (queue, type, json_payloads),
            ).fetchone()
        except psycopg.Error as e:
            raise _wrap_sql_error(e) from e
        return list(row[0])

    # --- consumer -------------------------------------------------------

    def subscribe(self, queue: str, consumer: str) -> int:
        """Subscribe ``consumer`` to ``queue``.

        Maps to ``pgque.subscribe(queue, consumer)``. Returns ``1`` when a
        subscription row was created and ``0`` when it already existed.
        """
        try:
            row = self.conn.execute(
                "select pgque.subscribe(%s, %s)", (queue, consumer)
            ).fetchone()
        except psycopg.Error as e:
            raise _wrap_sql_error(e) from e
        return row[0]

    def unsubscribe(self, queue: str, consumer: str) -> int:
        """Unsubscribe ``consumer`` from ``queue``.

        Maps to ``pgque.unsubscribe(queue, consumer)``. Returns ``1`` when a
        subscription row was removed and ``0`` when no row existed.
        """
        try:
            row = self.conn.execute(
                "select pgque.unsubscribe(%s, %s)", (queue, consumer)
            ).fetchone()
        except psycopg.Error as e:
            raise _wrap_sql_error(e) from e
        return row[0]

    def receive(
        self,
        queue: str,
        consumer: str,
        max_messages: int = 100,
    ) -> list[Message]:
        """Receive a batch of messages from a queue.

        Maps to ``pgque.receive(queue, consumer, max_messages)``, which
        opens a batch via ``next_batch`` internally. The caller must
        ``ack()`` the batch (with the ``batch_id`` from any returned
        message) to advance the consumer past it. ``ack()`` finishes the
        whole underlying PgQ batch, including rows beyond ``max_messages``;
        direct callers should pass a value large enough for the queue's
        possible batch size before acknowledging.

        Args:
            queue: Queue name.
            consumer: Consumer name (must be registered on the queue).
            max_messages: Maximum number of messages to return from the
                current batch.

        Returns:
            List of ``Message`` objects, possibly empty if no batch is
            currently available (e.g. the ticker has not run since the
            last enqueue).
        """
        try:
            rows = self.conn.execute(
                "select * from pgque.receive(%s, %s, %s)",
                (queue, consumer, max_messages),
            ).fetchall()
        except psycopg.Error as e:
            raise _wrap_sql_error(e) from e

        return [
            Message(
                msg_id=r[0],
                batch_id=r[1],
                type=r[2],
                payload=r[3],
                retry_count=r[4],
                created_at=r[5],
                extra1=r[6],
                extra2=r[7],
                extra3=r[8],
                extra4=r[9],
            )
            for r in rows
        ]

    def ack(self, batch_id: int) -> int:
        """Acknowledge (finish) a batch. Advances the consumer past it.

        Args:
            batch_id: Batch ID from any ``Message`` in the batch.

        Returns:
            Result returned by ``pgque.ack`` (1 on success).
        """
        try:
            row = self.conn.execute(
                "select pgque.ack(%s)", (batch_id,)
            ).fetchone()
        except psycopg.Error as e:
            raise _wrap_sql_error(e) from e
        return row[0]

    def force_next_tick(self, queue: str) -> Optional[int]:
        """Force the next ``pgque.ticker(queue)`` call to insert a tick.

        Maps to ``pgque.force_next_tick(queue)``. The SQL function bumps the
        queue's event sequence so the next ticker pass skips the normal
        ``ticker_max_count`` / ``ticker_max_lag`` thresholds. It does **not**
        insert the tick itself; call ``pgque.ticker`` afterwards (via raw SQL or
        a scheduler).

        Returns:
            The current last tick ID, or ``None`` for a brand-new / skipped
            queue, matching the SQL function.
        """
        try:
            row = self.conn.execute(
                "select pgque.force_next_tick(%s)", (queue,)
            ).fetchone()
        except psycopg.Error as e:
            raise _wrap_sql_error(e) from e
        return row[0]

    def force_tick(self, queue: str) -> Optional[int]:
        """Deprecated compatibility alias for ``force_next_tick``."""
        return self.force_next_tick(queue)

    def ticker(self, queue: str) -> Optional[int]:
        """Run the per-queue ticker for ``queue``.

        Maps to ``pgque.ticker(queue)``. Returns the new tick ID when a tick
        was inserted, or ``None`` when no tick was needed.
        """
        try:
            row = self.conn.execute(
                "select pgque.ticker(%s)", (queue,)
            ).fetchone()
        except psycopg.Error as e:
            raise _wrap_sql_error(e) from e
        return row[0]

    def ticker_all(self) -> int:
        """Run the global ticker across all eligible queues.

        Maps to zero-argument ``pgque.ticker()`` and returns the number of
        queues that received a tick.
        """
        try:
            row = self.conn.execute("select pgque.ticker()").fetchone()
        except psycopg.Error as e:
            raise _wrap_sql_error(e) from e
        return row[0]

    # --- experimental cooperative consumers -----------------------------
    #
    # Function names, edge-case behavior, and signatures for these methods
    # may change before the cooperative API is marked stable. See the
    # client README ("Experimental: cooperative consumers") and
    # ``docs/reference.md`` for context.

    def subscribe_subconsumer(
        self,
        queue: str,
        consumer: str,
        subconsumer: str,
    ) -> int:
        """Register ``subconsumer`` under logical ``consumer`` for ``queue``.

        Maps to ``pgque.subscribe_subconsumer(queue, consumer, subconsumer)``.
        Returns ``1`` for a new registration and ``0`` if the row already
        existed.
        """
        try:
            row = self.conn.execute(
                "select pgque.subscribe_subconsumer(%s, %s, %s)",
                (queue, consumer, subconsumer),
            ).fetchone()
        except psycopg.Error as e:
            raise _wrap_sql_error(e) from e
        return row[0]

    def unsubscribe_subconsumer(
        self,
        queue: str,
        consumer: str,
        subconsumer: str,
        *,
        batch_handling: int = 0,
    ) -> int:
        """Unregister one subconsumer.

        Maps to ``pgque.unsubscribe_subconsumer(queue, consumer,
        subconsumer, batch_handling)``. The default ``batch_handling=0``
        raises if the subconsumer holds an active batch; pass ``1`` to
        route active messages through the same retry/DLQ policy as
        ``nack`` before the row is removed.
        """
        try:
            row = self.conn.execute(
                "select pgque.unsubscribe_subconsumer(%s, %s, %s, %s)",
                (queue, consumer, subconsumer, batch_handling),
            ).fetchone()
        except psycopg.Error as e:
            raise _wrap_sql_error(e) from e
        return row[0]

    def receive_coop(
        self,
        queue: str,
        consumer: str,
        subconsumer: str,
        *,
        max_messages: int = 100,
        dead_interval: Optional[str] = None,
    ) -> list[Message]:
        """Receive a batch of messages for one cooperative subconsumer.

        Maps to ``pgque.receive_coop(queue, consumer, subconsumer,
        max_return, dead_interval)``. The function auto-registers the
        ``coop_main`` and ``coop_member`` rows on first call, so callers
        do not need to ``subscribe_subconsumer`` ahead of time unless
        they want to convert an existing normal consumer.

        Args:
            queue: Queue name.
            consumer: Logical consumer (the ``coop_main`` row).
            subconsumer: Per-worker member name.
            max_messages: Maximum rows to return from the current batch.
                ``ack(batch_id)`` advances the cooperative cursor past
                the entire underlying batch, so set this >= the queue's
                worst-case batch size or consume the full batch before
                acking.
            dead_interval: Optional PostgreSQL interval syntax (e.g.
                ``"5 minutes"``). When set, allows takeover of a stale
                sibling's batch under a fresh ``batch_id``; the old
                token is invalidated.

        Returns:
            Possibly-empty list of ``Message`` objects.
        """
        try:
            rows = self.conn.execute(
                "select * from pgque.receive_coop(%s, %s, %s, %s, %s::interval)",
                (queue, consumer, subconsumer, max_messages, dead_interval),
            ).fetchall()
        except psycopg.Error as e:
            raise _wrap_sql_error(e) from e

        return [
            Message(
                msg_id=r[0],
                batch_id=r[1],
                type=r[2],
                payload=r[3],
                retry_count=r[4],
                created_at=r[5],
                extra1=r[6],
                extra2=r[7],
                extra3=r[8],
                extra4=r[9],
            )
            for r in rows
        ]

    def touch_subconsumer(
        self,
        queue: str,
        consumer: str,
        subconsumer: str,
    ) -> int:
        """Refresh the heartbeat for a registered subconsumer row.

        Maps to ``pgque.touch_subconsumer(queue, consumer, subconsumer)``.
        Does not create a row if one does not already exist; returns the
        number of rows touched (``1`` when the subconsumer is registered,
        ``0`` otherwise).
        """
        try:
            row = self.conn.execute(
                "select pgque.touch_subconsumer(%s, %s, %s)",
                (queue, consumer, subconsumer),
            ).fetchone()
        except psycopg.Error as e:
            raise _wrap_sql_error(e) from e
        return row[0]

    def nack(
        self,
        batch_id: int,
        msg: Message,
        retry_after: Union[int, float] = 60,
        reason: Optional[str] = None,
    ) -> None:
        """Negatively acknowledge a single message.

        Routes the message to the retry queue with a ``retry_after``
        delay. If the message's ``retry_count`` is at or above the
        queue's ``queue_max_retries``, it is moved to the dead-letter
        queue instead.

        After nacking individual messages, the caller should still
        ``ack()`` the batch to finish it.

        Args:
            batch_id: Batch ID.
            msg: The ``Message`` to retry.
            retry_after: Seconds before the message becomes available
                again (default 60).
            reason: Optional reason text (stored on the DLQ row when
                max retries is exceeded).
        """
        try:
            self.conn.execute(
                "select pgque.nack("
                "  %s,"
                "  ROW(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)::pgque.message,"
                "  %s::interval,"
                "  %s"
                ")",
                (
                    batch_id,
                    msg.msg_id,
                    msg.batch_id,
                    msg.type,
                    json.dumps(msg.payload)
                    if isinstance(msg.payload, (dict, list))
                    else msg.payload,
                    msg.retry_count,
                    msg.created_at,
                    msg.extra1,
                    msg.extra2,
                    msg.extra3,
                    msg.extra4,
                    f"{retry_after} seconds",
                    reason,
                ),
            )
        except psycopg.Error as e:
            raise _wrap_sql_error(e) from e
