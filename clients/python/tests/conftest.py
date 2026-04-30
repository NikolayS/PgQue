# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Shared pytest fixtures.

Tests are env-gated: set ``PGQUE_TEST_DSN`` to a Postgres instance with
the PgQue schema installed. Without it, every test that depends on a
real database is skipped.
"""

import os
import secrets
from typing import Iterator

import pytest

DSN = os.environ.get("PGQUE_TEST_DSN")


def _require_dsn() -> str:
    if not DSN:
        pytest.skip("PGQUE_TEST_DSN not set")
    return DSN


@pytest.fixture
def dsn() -> str:
    return _require_dsn()


@pytest.fixture
def conn(dsn):
    """Raw psycopg connection (autocommit off)."""
    import psycopg

    with psycopg.connect(dsn) as c:
        yield c


@pytest.fixture
def queue_name(request) -> str:
    """A unique queue name for each test, scoped by test name + random suffix."""
    base = request.node.name.replace("[", "_").replace("]", "_")
    return f"pyt_{base[:40]}_{secrets.token_hex(4)}"


@pytest.fixture
def consumer_name(request) -> str:
    base = request.node.name.replace("[", "_").replace("]", "_")
    return f"pyt_c_{base[:38]}_{secrets.token_hex(4)}"


@pytest.fixture
def setup_queue(conn, queue_name, consumer_name) -> Iterator[tuple[str, str]]:
    """Create queue + register consumer; tear down after test."""
    conn.execute("select pgque.create_queue(%s)", (queue_name,))
    conn.execute(
        "select pgque.register_consumer(%s, %s)", (queue_name, consumer_name)
    )
    conn.commit()
    try:
        yield (queue_name, consumer_name)
    finally:
        try:
            conn.rollback()
            conn.execute(
                "select pgque.unregister_consumer(%s, %s)",
                (queue_name, consumer_name),
            )
            conn.execute("select pgque.drop_queue(%s, true)", (queue_name,))
            conn.commit()
        except Exception:
            conn.rollback()
