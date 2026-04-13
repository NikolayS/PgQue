import os
import pytest
import psycopg

DSN = os.environ.get("PGQUE_TEST_DSN", "postgresql://postgres:pgque_test@localhost/pgque_test")

@pytest.fixture
def conn():
    with psycopg.connect(DSN) as c:
        yield c

@pytest.fixture
def setup_queue(conn):
    """Create a test queue and clean up after."""
    conn.execute("SELECT pgque.create_queue('pytest_queue')")
    conn.execute("SELECT pgque.register_consumer('pytest_queue', 'pytest_consumer')")
    conn.commit()
    yield
    try:
        conn.execute("SELECT pgque.unregister_consumer('pytest_queue', 'pytest_consumer')")
        conn.execute("SELECT pgque.drop_queue('pytest_queue')")
        conn.commit()
    except Exception:
        conn.rollback()
