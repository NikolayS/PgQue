# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# PgQue includes code derived from PgQ (ISC license,
# Marko Kreen / Skype Technologies OU).

"""PgqueClient -- thin Python wrapper over pgque SQL API."""

import json
from typing import Optional

import psycopg

from .types import Message


class PgqueClient:
    """Thin wrapper around pgque SQL functions.

    All methods execute SQL against the provided psycopg connection.
    Transaction management (commit/rollback) is the caller's
    responsibility unless autocommit is enabled on the connection.
    """

    def __init__(self, conn: psycopg.Connection):
        self.conn = conn

    def send(
        self,
        queue: str,
        payload=None,
        *,
        type: str = "default",
    ) -> int:
        """Send a single message to a queue.

        Maps to ``pgque.send(queue, payload)`` or
        ``pgque.send(queue, type, payload)``.

        Args:
            queue: Target queue name.
            payload: Message payload (dict serialized to JSON, or str).
            type: Event type (default ``"default"``).

        Returns:
            The event ID assigned by pgque.
        """
        if isinstance(payload, dict):
            payload = json.dumps(payload)

        if type != "default":
            row = self.conn.execute(
                "select pgque.send(%s, %s, %s::jsonb)",
                (queue, type, payload),
            ).fetchone()
        else:
            row = self.conn.execute(
                "select pgque.send(%s, %s::jsonb)",
                (queue, payload),
            ).fetchone()

        return row[0]

    def send_batch(
        self,
        queue: str,
        type: str,
        payloads: list,
    ) -> list[int]:
        """Send multiple messages in a single transaction.

        Maps to ``pgque.send_batch(queue, type, payloads[])``.

        Args:
            queue: Target queue name.
            type: Event type for all messages.
            payloads: List of payloads (dicts or strings).

        Returns:
            List of event IDs.
        """
        json_payloads = [
            json.dumps(p) if isinstance(p, dict) else p for p in payloads
        ]
        row = self.conn.execute(
            "select pgque.send_batch(%s, %s, %s::jsonb[])",
            (queue, type, json_payloads),
        ).fetchone()
        return list(row[0])

    def receive(
        self,
        queue: str,
        consumer: str,
        max_messages: int = 100,
    ) -> list[Message]:
        """Receive messages from a queue.

        Maps to ``pgque.receive(queue, consumer, max_messages)``.
        Opens a batch via ``next_batch()`` internally. The caller must
        ``ack()`` the batch after processing to advance the consumer.

        Args:
            queue: Queue name.
            consumer: Consumer name.
            max_messages: Maximum number of messages to return from the
                current batch.

        Returns:
            List of ``Message`` objects (may be empty if no batch is
            available).
        """
        rows = self.conn.execute(
            "select * from pgque.receive(%s, %s, %s)",
            (queue, consumer, max_messages),
        ).fetchall()

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
        """Acknowledge (finish) a batch.

        Maps to ``pgque.ack(batch_id)`` which calls
        ``pgque.finish_batch()``.  This advances the consumer past
        the entire batch.

        Args:
            batch_id: Batch ID returned in received messages.

        Returns:
            Result from ``pgque.ack()``.
        """
        row = self.conn.execute(
            "select pgque.ack(%s)", (batch_id,)
        ).fetchone()
        return row[0]

    def nack(
        self,
        batch_id: int,
        msg: Message,
        retry_after: int = 60,
        reason: Optional[str] = None,
    ) -> None:
        """Negatively acknowledge a single message for retry.

        Maps to ``pgque.nack(batch_id, msg, retry_after, reason)``.
        The message is copied to the retry queue. If the retry count
        exceeds ``queue_max_retries``, the message is moved to the
        dead-letter table instead.

        After nacking individual messages, the caller should still
        call ``ack()`` to finish the batch.

        Args:
            batch_id: Batch ID.
            msg: The ``Message`` to retry.
            retry_after: Seconds before retry (default 60).
            reason: Optional reason string (stored in DLQ if max
                retries exceeded).
        """
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
                msg.payload,
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
