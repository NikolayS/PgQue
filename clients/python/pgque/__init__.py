# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# PgQue includes code derived from PgQ (ISC license,
# Marko Kreen / Skype Technologies OU).

"""pgque -- Python client for PgQue (PgQ Universal Edition)."""

from .client import PgqueClient
from .consumer import Consumer
from .types import Message

__all__ = ["PgqueClient", "Consumer", "Message"]
