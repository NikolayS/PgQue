# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# PgQue includes code derived from PgQ (ISC license,
# Marko Kreen / Skype Technologies OU).

"""Consumer -- event-driven message consumer with LISTEN/NOTIFY support."""

import logging
import select
import signal
import threading
import time
from typing import Callable, Literal, Optional

import psycopg
from psycopg import sql

from .client import PgqueClient
from .types import Message

logger = logging.getLogger("pgque")

# Maximum time the LISTEN wait blocks before re-checking the stop flag.
# Bounds shutdown latency to roughly this many seconds.
_WAIT_SLICE_SECONDS = 0.5
# PostgreSQL int4 max; request the whole batch by default.
_DEFAULT_MAX_MESSAGES = 2_147_483_647


class Consumer:
    """Synchronous polling consumer with LISTEN/NOTIFY wakeup.

    Usage::

        consumer = Consumer(
            dsn="postgresql://localhost/mydb",
            queue="orders",
            name="order_processor",
        )

        @consumer.on("order.created")
        def handle_order(msg: Message):
            process_order(msg.payload)

        consumer.start()  # blocks until SIGTERM/SIGINT

    Handler return semantics:
        - If the handler returns without exception, the message is
          considered processed.
        - If the handler raises an exception, the message is nacked
          with the default retry_after.
        - If no handler is registered for a message type (and no
          default ``"*"`` handler exists), the message is nacked
          (sent to retry_queue, or to the dead-letter queue once
          ``queue_max_retries`` is exhausted). To ack unknown types
          instead, pass ``unknown_handler_policy="ack"``.

    After all messages in a batch have been dispatched, the batch is
    acked automatically.
    """

    def __init__(
        self,
        dsn: str,
        *,
        queue: str,
        name: str,
        poll_interval: int = 30,
        max_messages: int = _DEFAULT_MAX_MESSAGES,
        retry_after: int = 60,
        unknown_handler_policy: Literal["nack", "ack"] = "nack",
        subconsumer: Optional[str] = None,
        dead_interval: Optional[str] = None,
    ):
        self.dsn = dsn
        self.queue = queue
        self.name = name
        self.poll_interval = poll_interval
        self.max_messages = max_messages
        self.retry_after = retry_after
        if unknown_handler_policy not in ("nack", "ack"):
            raise ValueError(
                "unknown_handler_policy must be 'nack' or 'ack', "
                f"got {unknown_handler_policy!r}"
            )
        self._unknown_handler_policy = unknown_handler_policy

        # Experimental cooperative-consumers mode. When ``subconsumer`` is
        # set, the poll loop calls ``client.receive_coop(...)`` instead of
        # the normal ``receive(...)``. ``dead_interval`` is meaningless
        # outside coop mode and signals a programming error if provided.
        if dead_interval is not None and subconsumer is None:
            raise ValueError(
                "dead_interval is only valid in cooperative mode "
                "(set subconsumer=...)"
            )
        self.subconsumer = subconsumer
        self.dead_interval = dead_interval

        self._handlers: dict[str, Callable] = {}
        self._default_handler: Optional[Callable] = None
        self._running = False
        self._log = logging.getLogger(f"pgque.consumer.{name}")

    def on(self, event_type: str):
        """Decorator to register a handler for a given event type.

        Args:
            event_type: The ``pgque.message.type`` value to match.
                Use ``"*"`` to register a default/catch-all handler.
        """

        def decorator(func: Callable):
            if event_type == "*":
                self._default_handler = func
            else:
                self._handlers[event_type] = func
            return func

        return decorator

    def start(self) -> None:
        """Run the consume loop (blocks until SIGTERM/SIGINT).

        Opens its own connection, subscribes to LISTEN, and polls for
        batches. Each batch is processed and acked in a single
        transaction.
        """
        self._running = True

        # Graceful shutdown on signals; only main-thread invocations can
        # install signal handlers. When the consumer is run from a worker
        # thread (tests, embedded use), skip registration -- callers stop
        # via Consumer.stop().
        in_main_thread = threading.current_thread() is threading.main_thread()
        original_sigterm = None
        original_sigint = None

        def _stop(signum, frame):
            logger.info("received signal %s, shutting down", signum)
            self._running = False

        if in_main_thread:
            original_sigterm = signal.getsignal(signal.SIGTERM)
            original_sigint = signal.getsignal(signal.SIGINT)
            signal.signal(signal.SIGTERM, _stop)
            signal.signal(signal.SIGINT, _stop)

        try:
            with psycopg.connect(self.dsn, autocommit=True) as conn:
                # Subscribe for wakeup notifications
                channel = f"pgque_{self.queue}"
                conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(channel)))
                logger.info(
                    "consumer %s listening on %s (poll=%ds)",
                    self.name,
                    self.queue,
                    self.poll_interval,
                )

                while self._running:
                    processed = self._poll_once(conn)

                    if not self._running:
                        break

                    if processed:
                        # Non-empty batch: more batches may already be
                        # ticked (backlog). Their notifies fired while
                        # we were not listening, so waiting for a new
                        # NOTIFY would drain the backlog at one batch
                        # per poll_interval. Re-poll immediately until
                        # the queue comes back empty.
                        continue

                    # Wait for NOTIFY or poll_interval timeout in short
                    # bounded slices. psycopg's conn.notifies() can
                    # block uninterruptibly for the full timeout, which
                    # makes stop() slow and can miss prompt wakeups.
                    # Polling the underlying socket with
                    # select() lets us re-check _running every SLICE
                    # seconds and drain any pending NOTIFY immediately.
                    self._wait_for_notify_or_stop(conn)

        finally:
            if in_main_thread:
                signal.signal(signal.SIGTERM, original_sigterm)
                signal.signal(signal.SIGINT, original_sigint)

        logger.info("consumer %s stopped", self.name)

    def stop(self) -> None:
        """Request graceful shutdown (safe to call from another thread)."""
        self._running = False

    def _wait_for_notify_or_stop(self, conn: psycopg.Connection) -> None:
        """Wait up to ``poll_interval`` for a NOTIFY, in short slices.

        Returns early on any of:
          * a NOTIFY arrives (drained from the connection),
          * ``stop()`` flips ``_running`` to False,
          * ``poll_interval`` elapses cumulatively.

        Each slice is at most ``_WAIT_SLICE_SECONDS`` so ``stop()`` is
        observed within ~SLICE seconds of the call.
        """
        # Drain any NOTIFY already buffered in libpq from the prior
        # _poll_once (e.g. delivered alongside query results). Without
        # this, a buffered notify sits in libpq until the socket
        # becomes readable for some other reason -- select() won't see
        # it, and wakeup latency stretches out. Restores the implicit
        # entry-drain semantics of the old conn.notifies(timeout=...).
        drained = False
        for _notify in conn.notifies(timeout=0):
            drained = True
        if drained:
            return

        deadline = time.monotonic() + self.poll_interval
        fd = conn.fileno()
        while self._running:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            slice_timeout = min(_WAIT_SLICE_SECONDS, remaining)
            # select() returns when the socket is readable (notify
            # delivered by the server) or when slice_timeout expires.
            # It is a thin wrapper around the OS poll, so it is cheap
            # and interruptible.
            r, _w, _x = select.select([fd], [], [], slice_timeout)
            if not self._running:
                return
            if r:
                # Drain pending notifications without blocking. A
                # zero timeout makes notifies() return immediately
                # after consuming whatever is buffered.
                for _notify in conn.notifies(timeout=0):
                    pass
                return

    def _poll_once(self, conn: psycopg.Connection) -> bool:
        """Receive one batch and dispatch messages.

        Returns True when a non-empty batch was processed and acked
        (the caller should re-poll immediately to drain any backlog),
        False when the queue was empty or the batch was left unacked
        for redelivery (the caller should wait for NOTIFY/timeout).

        If any per-message ``nack()`` raises, all remaining messages in
        the batch are still dispatched (their handlers run), but the
        batch is NOT acked at the end -- the receive transaction commits
        without finishing the batch, so PgQ redelivers the whole batch
        on the next poll. Without this guard, swallowing a nack failure
        and then acking would advance past the batch and silently drop
        the failed message.
        """
        # Use a transaction block for receive + ack
        with conn.transaction():
            client = PgqueClient(conn)
            if self.subconsumer is not None:
                msgs = client.receive_coop(
                    self.queue,
                    self.name,
                    self.subconsumer,
                    max_messages=self.max_messages,
                    dead_interval=self.dead_interval,
                )
            else:
                msgs = client.receive(
                    self.queue, self.name, self.max_messages
                )

            if not msgs:
                return False

            batch_id = msgs[0].batch_id
            logger.debug(
                "batch %d: %d message(s)", batch_id, len(msgs)
            )

            nack_failed = False

            for msg in msgs:
                handler = self._handlers.get(msg.type, self._default_handler)
                if handler is None:
                    if self._unknown_handler_policy == "ack":
                        self._log.warning(
                            "no handler for event type=%s ev_id=%s; acking",
                            msg.type,
                            msg.msg_id,
                        )
                        continue
                    self._log.warning(
                        "no handler for event type=%s ev_id=%s; nacking",
                        msg.type,
                        msg.msg_id,
                    )
                    try:
                        client.nack(
                            batch_id,
                            msg,
                            retry_after=self.retry_after,
                            reason=f"no handler for type={msg.type}",
                        )
                    except Exception:
                        nack_failed = True
                        self._log.exception(
                            "nack failed for unhandled msg_id=%d; "
                            "skipping batch ack so PgQ redelivers",
                            msg.msg_id,
                        )
                        continue
                    continue

                try:
                    handler(msg)
                except Exception:
                    self._log.exception(
                        "handler failed for msg_id=%d, nacking",
                        msg.msg_id,
                    )
                    try:
                        client.nack(
                            batch_id, msg, retry_after=self.retry_after
                        )
                    except Exception:
                        nack_failed = True
                        self._log.exception(
                            "nack failed for msg_id=%d; "
                            "skipping batch ack so PgQ redelivers",
                            msg.msg_id,
                        )
                        continue

            if nack_failed:
                # Do NOT ack -- redeliver on next poll. Report False so
                # the caller waits before re-polling instead of
                # redelivering the same batch in a hot loop.
                return False

            # pgque.ack returns 1 on success, 0 if the batch was already
            # finished or not found (stale/double ack, cross-consumer
            # race). Mirror the TS+Go consumers and log warn on 0; do
            # not treat it as an error.
            if client.ack(batch_id) == 0:
                logger.warning(
                    "pgque: ack batch %d returned 0 -- stale or double ack "
                    "(batch already finished or not found)",
                    batch_id,
                )

            return True
