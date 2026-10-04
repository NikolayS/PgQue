# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Typed receive-overflow classification and consumer fail-fast behavior."""

from types import SimpleNamespace
from unittest import mock
import signal

import pgque
import pytest

from pgque.client import _wrap_sql_error


class FakeSqlError(Exception):
    def __init__(self, message: str, *, sqlstate: str, hint: str = "server hint"):
        super().__init__(message)
        self.sqlstate = sqlstate
        self.diag = SimpleNamespace(message_primary=message, message_hint=hint)


@pytest.mark.parametrize("operation", ["receive", "receive_coop"])
def test_wraps_only_pgque_receive_overflow(operation):
    raw = FakeSqlError(
        f"pgque.{operation}: batch exceeds max_return of 17",
        sqlstate="54000",
    )

    error = _wrap_sql_error(raw, operation=operation, configured_limit=17)

    assert isinstance(error, pgque.ReceiveOverflowError)
    assert isinstance(error, pgque.PgqueError)
    assert error.sqlstate == "54000"
    assert error.hint == "server hint"
    assert error.configured_limit == 17
    assert error.operation == operation


@pytest.mark.parametrize(
    ("sqlstate", "message"),
    [
        ("54000", "program limit exceeded"),
        ("54000", "pgque.send: batch exceeds max_return of 17"),
        ("54000", "pgque.receive: batch exceeds max_return of seventeen"),
        ("22023", "pgque.receive: batch exceeds max_return of 17"),
    ],
)
def test_does_not_misclassify_other_errors(sqlstate, message):
    error = _wrap_sql_error(
        FakeSqlError(message, sqlstate=sqlstate),
        operation="receive",
        configured_limit=17,
    )
    assert type(error) is pgque.PgqueError


@pytest.mark.parametrize(
    ("operation", "configured_limit"),
    [(None, None), ("receive_coop", 17), ("receive", 18)],
)
def test_matching_server_text_requires_matching_receive_call_context(
    operation, configured_limit
):
    raw = FakeSqlError(
        "pgque.receive: batch exceeds max_return of 17", sqlstate="54000"
    )
    error = _wrap_sql_error(
        raw, operation=operation, configured_limit=configured_limit
    )
    assert type(error) is pgque.PgqueError


@pytest.mark.parametrize(
    ("operation", "subconsumer"),
    [("receive", None), ("receive_coop", "worker-1")],
)
def test_consumer_overflow_fails_fast_without_retry_or_dispatch(
    operation, subconsumer
):
    overflow = pgque.ReceiveOverflowError(
        f"pgque.{operation}: batch exceeds max_return of 3",
        sqlstate="54000",
        hint="increase safely",
        configured_limit=3,
        operation=operation,
    )
    connection = mock.MagicMock()
    connection.__enter__.return_value = connection
    connection.__exit__.return_value = False
    connection.closed = False
    handler = mock.Mock()
    consumer = pgque.Consumer(
        "dsn", queue="q", name="c", max_messages=3,
        subconsumer=subconsumer,
    )
    consumer.on("event")(handler)
    original_sigterm = signal.getsignal(signal.SIGTERM)
    original_sigint = signal.getsignal(signal.SIGINT)

    # If the old resilience loop catches the overflow, stop its loop on the
    # first retry wait so this regression fails promptly instead of hanging.
    def stop_on_retry():
        consumer.stop()

    with mock.patch("pgque.consumer.psycopg.connect", return_value=connection), \
            mock.patch.object(
                pgque.PgqueClient, operation, side_effect=overflow
            ) as receive, \
            mock.patch.object(pgque.PgqueClient, "ack") as ack, \
            mock.patch.object(pgque.PgqueClient, "nack") as nack, \
            mock.patch.object(
                consumer, "_sleep_before_reconnect", side_effect=stop_on_retry
            ) as retry:
        with pytest.raises(pgque.ReceiveOverflowError) as exc_info:
            consumer.start()

    assert exc_info.value is overflow
    assert receive.call_count == 1
    retry.assert_not_called()
    handler.assert_not_called()
    ack.assert_not_called()
    nack.assert_not_called()
    assert consumer._running is False
    connection.__exit__.assert_called_once()
    assert signal.getsignal(signal.SIGTERM) is original_sigterm
    assert signal.getsignal(signal.SIGINT) is original_sigint
