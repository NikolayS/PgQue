# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""``pgque.connect`` factory + Client lifecycle."""

import pytest

import pgque


def test_connect_returns_client(dsn):
    client = pgque.connect(dsn)
    assert isinstance(client, pgque.PgqueClient)
    assert not client.conn.closed
    client.close()
    assert client.conn.closed


def test_connect_context_manager(dsn):
    with pgque.connect(dsn) as client:
        assert not client.conn.closed
    assert client.conn.closed


def test_connect_bad_dsn_raises_pgque_connection_error():
    with pytest.raises(pgque.PgqueConnectionError):
        pgque.connect(
            "postgresql://nobody:wrong@localhost:1/nonexistent_db_xyz"
        )


def test_external_conn_is_not_closed_by_close(dsn):
    import psycopg

    raw = psycopg.connect(dsn)
    try:
        client = pgque.PgqueClient(raw)
        client.close()  # external conn -> no-op
        assert not raw.closed
    finally:
        raw.close()


def test_autocommit_flag(dsn):
    with pgque.connect(dsn, autocommit=True) as client:
        assert client.conn.autocommit is True


def test_close_is_idempotent(dsn):
    """Calling close() twice must not raise."""
    client = pgque.connect(dsn)
    client.close()
    client.close()  # second call must be a no-op
