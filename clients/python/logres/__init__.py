# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# logres includes code derived from PgQ (ISC license,
# Marko Kreen / Skype Technologies OU).

"""logres -- Python client for logres (PgQ Universal Edition)."""

from .client import LogresClient
from .consumer import Consumer
from .types import Message

__all__ = ["LogresClient", "Consumer", "Message"]
