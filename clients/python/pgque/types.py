# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# PgQue includes code derived from PgQ (ISC license,
# Marko Kreen / Skype Technologies OU).

"""Message and type definitions for pgque."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Message:
    """A message received from a pgque queue.

    Maps to the pgque.message composite type:
        msg_id      -- ev_id
        batch_id    -- batch containing this message
        type        -- ev_type
        payload     -- ev_data (caller casts to jsonb if needed)
        retry_count -- ev_retry (None for first delivery)
        created_at  -- ev_time
        extra1..4   -- ev_extra1..ev_extra4
    """

    msg_id: int
    batch_id: int
    type: str
    payload: str
    retry_count: Optional[int]
    created_at: datetime
    extra1: Optional[str] = None
    extra2: Optional[str] = None
    extra3: Optional[str] = None
    extra4: Optional[str] = None
