import os
import pytest
import psycopg

DSN = os.environ.get("PGQUE_TEST_DSN", "postgresql://postgres:pg_current_test@localhost/pg_current_test")

@pytest.fixture
def conn():
    with psycopg.connect(DSN) as c:
        yield c

@pytest.fixture
def setup_queue(conn):
    """Create a test queue and clean up after."""
    conn.execute("SELECT pg_current.create_queue('pytest_queue')")
    conn.execute("SELECT pg_current.register_consumer('pytest_queue', 'pytest_consumer')")
    conn.commit()
    yield
    try:
        conn.execute("SELECT pg_current.unregister_consumer('pytest_queue', 'pytest_consumer')")
        conn.execute("SELECT pg_current.drop_queue('pytest_queue')")
        conn.commit()
    except Exception:
        conn.rollback()
