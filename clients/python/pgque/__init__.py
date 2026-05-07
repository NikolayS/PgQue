# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# PgQue includes code derived from PgQ (ISC license,
# Marko Kreen / Skype Technologies OU).

"""pgque -- Python client for PgQue (PgQ Universal Edition).

Quickstart::

    import pgque

    with pgque.connect("postgresql://localhost/mydb") as client:
        client.send("orders", {"order_id": 42}, type="order.created")
        client.conn.commit()

See https://github.com/NikolayS/pgque for the SQL schema install and
full documentation.
"""

from .client import PgqueClient, connect
from .consumer import Consumer
from .errors import (
    PgqueBatchNotFound,
    PgqueConnectionError,
    PgqueConsumerNotFound,
    PgqueError,
    PgqueQueueNotFound,
)
from .types import Event, Message

__version__ = "0.2.0.dev1"

__all__ = [
    "PgqueClient",
    "Consumer",
    "Message",
    "Event",
    "PgqueError",
    "PgqueConnectionError",
    "PgqueQueueNotFound",
    "PgqueBatchNotFound",
    "PgqueConsumerNotFound",
    "connect",
    "__version__",
]
