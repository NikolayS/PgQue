# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Transaction-visibility contract tests.

PgQ uses snapshot isolation: events inserted in transaction T are only
visible to a batch whose tick was taken *after* T committed.  Collapsing
send + force_next_tick + receive into one transaction (no intervening commit)
violates this contract -- receive returns 0 rows.

These tests document and enforce that contract so that future authors
cannot accidentally "fix" the two-commit ordering and silently break
PgQ visibility semantics.
"""

from unittest import mock

import pgque


# ---------------------------------------------------------------------------
# 1. Visibility contract: collapsed transaction returns nothing
# ---------------------------------------------------------------------------

def test_collapsed_transaction_returns_no_messages(conn, setup_queue):
    """send → force_next_tick → receive in ONE transaction must return 0 rows.

    PgQ snapshot isolation: the tick's snapshot is taken at force_next_tick time.
    Events in the *same* transaction are not yet committed at that point, so
    they are invisible to the batch.  A missing ``conn.commit()`` between
    send and force_next_tick is the canonical mistake this test guards against.

    If this test starts passing with len > 0, something has changed in the
    PgQ snapshot mechanism and the two-commit ordering assumption is broken.
    """
    queue, consumer = setup_queue
    client = pgque.PgqueClient(conn)

    # All three operations in the same transaction -- NO commit in between.
    client.send(queue, {"x": 1}, type="collapsed.test")
    conn.execute("select pgque.force_next_tick(%s)", (queue,))
    conn.execute("select pgque.ticker(%s)", (queue,))
    # Deliberately NO conn.commit() here.
    msgs = client.receive(queue, consumer, max_messages=10)

    assert len(msgs) == 0, (
        "PgQ visibility violation: send/tick/receive in one transaction "
        f"returned {len(msgs)} message(s); expected 0. "
        "Add conn.commit() between send and force_next_tick."
    )

    # Rollback so setup_queue teardown sees a clean state.
    conn.rollback()


# ---------------------------------------------------------------------------
# 2. Red/green: strengthened unknown-type assertion catches a broken consumer
# ---------------------------------------------------------------------------

def test_unhandled_event_nack_assertion_catches_stale_cursor(
    dsn, conn, setup_queue
):
    """Regression guard: the 'follow-up receive returns 0' assertion must
    FAIL when the consumer hasn't actually advanced its cursor.

    Red/green intent
    ----------------
    *Red* (what the old, weak assertion missed): if ``_poll_once`` never
    calls ``ack()`` -- e.g. because nack raised and the code silently
    swallowed it -- the batch cursor stays at the old position.  A naive
    ``assert retry_queue == 0`` would pass vacuously (nothing is in
    retry_queue because nack never succeeded), hiding the bug.

    *Green*: the stronger assertion -- re-receive returns 0 rows for the
    consumer after the batch was correctly processed -- catches this case.

    This test simulates the broken path by patching ``_poll_once`` to be a
    no-op (it receives messages but neither acks nor nacks them), then
    checks that the follow-up receive still returns the message (cursor did
    not advance).  The test itself asserts this invariant holds, which
    documents the contract: a consumer that does not ack must NOT advance
    the cursor.
    """
    queue, consumer_name = setup_queue
    client = pgque.PgqueClient(conn)
    msg_id = client.send(queue, {"x": 1}, type="totally.unregistered.type")
    conn.commit()
    conn.execute("select pgque.force_next_tick(%s)", (queue,))
    conn.execute("select pgque.ticker(%s)", (queue,))
    conn.commit()

    cons = pgque.Consumer(
        dsn=dsn, queue=queue, name=consumer_name, poll_interval=1
    )

    # Patch _poll_once to do nothing -- simulates a consumer that receives
    # but never acks, leaving the batch cursor in place.
    with mock.patch.object(type(cons), "_poll_once", return_value=None):
        import threading
        import time

        t = threading.Thread(target=cons.start, daemon=True)
        t.start()
        time.sleep(2.0)
        cons.stop()
        t.join(timeout=4.0)

    # Because _poll_once was a no-op, the cursor did NOT advance.
    # A fresh receive must still return the original msg_id.
    conn.execute("select pgque.force_next_tick(%s)", (queue,))
    conn.execute("select pgque.ticker(%s)", (queue,))
    conn.commit()
    follow_up = client.receive(queue, consumer_name, max_messages=10)

    # The cursor did not advance, so the message must still be visible.
    assert any(m.msg_id == msg_id for m in follow_up), (
        "Expected the unprocessed message to still be visible (cursor "
        "did not advance because _poll_once was a no-op), but re-receive "
        "returned no rows. This indicates the batch cursor advanced without "
        "an explicit ack, which would be a PgQ visibility violation."
    )

    # Cleanup: ack the batch so queue teardown is clean.
    if follow_up:
        client.ack(follow_up[0].batch_id)
        conn.commit()
