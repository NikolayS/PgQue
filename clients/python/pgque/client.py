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
    if "batch not found" in low:
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
                  The value is cast to ``jsonb`` by PostgreSQL; a bare
                  Python string like ``"hello"`` (without surrounding
                  double-quotes) will raise a ``PgqueError``.
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
        """Send multiple messages in a single SQL call (one transaction).

        Maps to ``pgque.send_batch(queue, type, payloads[])``.

        Args:
            queue: Target queue name.
            type: Event type for all messages.
            payloads: Each entry is JSON-encoded automatically if
                ``dict``/``list``; ``str`` entries must be valid JSON
                text (cast to ``jsonb`` by PostgreSQL); ``None`` is
                stored as JSON ``null`` (same as ``send(None)``).

        Returns:
            List of event IDs in input order.
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
        message) to advance the consumer past it.

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
