# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# PgQue includes code derived from PgQ (ISC license,
# Marko Kreen / Skype Technologies OU).

"""Message and Event types for pgque."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass
class Message:
    """A message received from a pgque queue.

    Maps to the ``pgque.message`` composite type:
        msg_id      -- ev_id
        batch_id    -- batch containing this message
        type        -- ev_type (``None`` if enqueued via raw
                       ``pgque.insert_event(queue, null, null)``)
        payload     -- ev_data (jsonb auto-decoded by psycopg, otherwise
                       text; ``None`` if raw insert_event passed null)
        retry_count -- ev_retry (None for first delivery)
        created_at  -- ev_time
        extra1..4   -- ev_extra1..ev_extra4

    ``type`` and ``payload`` are ``Optional`` because the low-level PgQ
    primitive ``pgque.insert_event(queue, null, null)`` can produce rows
    whose ``ev_type`` and ``ev_data`` are SQL-NULL. ``Client.send`` and
    ``Client.send_batch`` always emit non-NULL values, so most consumers
    will never observe ``None`` here -- but consumers that read from
    queues fed by raw ``insert_event`` calls must handle it.
    """

    msg_id: int
    batch_id: int
    type: Optional[str]
    payload: Optional[Any]
    retry_count: Optional[int]
    created_at: datetime
    extra1: Optional[str] = None
    extra2: Optional[str] = None
    extra3: Optional[str] = None
    extra4: Optional[str] = None


@dataclass
class Event:
    """An event being published to a queue. Convenience type for ``Client.send``.

    For most code, passing ``payload`` and ``type`` directly to ``send`` is
    simpler. ``Event`` is useful when constructing events programmatically
    or when the payload + metadata travel together.
    """

    payload: Any
    type: str = "default"
    extra: dict[str, str] = field(default_factory=dict)
