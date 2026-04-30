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
        type        -- ev_type
        payload     -- ev_data (jsonb auto-decoded by psycopg, otherwise text)
        retry_count -- ev_retry (None for first delivery)
        created_at  -- ev_time
        extra1..4   -- ev_extra1..ev_extra4
    """

    msg_id: int
    batch_id: int
    type: str
    payload: Any
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
