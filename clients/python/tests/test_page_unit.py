# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

from pgque import PgqueClient


class Cursor:
    def __init__(self, rows): self.rows = rows
    def fetchall(self): return self.rows
    def fetchone(self): return self.rows[0]


class Conn:
    closed = False
    def __init__(self, replies): self.replies, self.calls = list(replies), []
    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        reply = self.replies.pop(0)
        if isinstance(reply, Exception): raise reply
        return Cursor(reply)


def page_row():
    return ("page", 2**63 - 1, "token", 1, True, None, None,
            2**63 - 2, 2**63 - 1, "x", {}, None, None,
            None, None, None, None, 1)


def test_receive_page_uses_typed_expansion_and_preserves_int8():
    conn = Conn([[page_row()]])
    page = PgqueClient(conn).receive_page("q", "c", "w")
    assert "left join lateral unnest(p.messages)" in conn.calls[0][0]
    assert page.batch_id == 2**63 - 1
    assert page.messages[0].msg_id == 2**63 - 2


def test_receive_page_decodes_metadata_only_row():
    row = ("idle", None, None, None, None, None, None,
           None, None, None, None, None, None, None, None, None, None, None)
    page = PgqueClient(Conn([[row]])).receive_page("q", "c", "w")
    assert page.status == "idle"
    assert page.messages == []
    assert page.page_token is None


def test_process_page_handler_error_leaves_page_outstanding():
    conn = Conn([[page_row()]])
    def fail(_): raise RuntimeError("boom")
    try:
        PgqueClient(conn).process_page("q", "c", "w", fail)
    except RuntimeError:
        pass
    else:
        raise AssertionError("handler exception not propagated")
    assert len(conn.calls) == 1


def test_process_page_rejects_async_and_generator_handlers_before_receive():
    async def async_handler(_): pass
    def generator_handler(_): yield None
    for handler in (async_handler, generator_handler):
        conn = Conn([])
        try:
            PgqueClient(conn).process_page("q", "c", "w", handler)
        except TypeError:
            pass
        else:
            raise AssertionError("lazy handler accepted")
        assert conn.calls == []


def test_process_page_rejects_lazy_return_without_ack():
    conn = Conn([[page_row()]])
    def handler(_): return (x for x in ())
    try:
        PgqueClient(conn).process_page("q", "c", "w", handler)
    except TypeError:
        pass
    else:
        raise AssertionError("lazy return accepted")
    assert len(conn.calls) == 1


def test_process_page_accepts_completed_return_value_without_closing_it():
    class Completed:
        closed = False
        def close(self): self.closed = True

    completed = Completed()
    conn = Conn([[page_row()], [["acked", True]]])
    result = PgqueClient(conn).process_page("q", "c", "w", lambda _: completed)
    assert result.processed_count == 1
    assert result.batch_finished
    assert not completed.closed
    assert len(conn.calls) == 2


def test_process_page_accepts_completed_iterator_without_closing_it():
    class ExecutedCursor:
        closed = False
        def __iter__(self): return self
        def __next__(self): raise StopIteration
        def close(self): self.closed = True

    cursor = ExecutedCursor()
    conn = Conn([[page_row()], [["acked", True]]])
    result = PgqueClient(conn).process_page("q", "c", "w", lambda _: cursor)
    assert result.processed_count == 1
    assert result.batch_finished
    assert not cursor.closed
    assert len(conn.calls) == 2


def test_process_page_does_not_close_unknown_awaitable():
    class Awaitable:
        closed = False
        def __await__(self): return iter(())
        def close(self): self.closed = True

    awaitable = Awaitable()
    conn = Conn([[page_row()]])
    try:
        PgqueClient(conn).process_page("q", "c", "w", lambda _: awaitable)
    except TypeError:
        pass
    else:
        raise AssertionError("awaitable return accepted")
    assert not awaitable.closed
    assert len(conn.calls) == 1
