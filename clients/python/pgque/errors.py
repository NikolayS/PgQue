# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# PgQue includes code derived from PgQ (ISC license,
# Marko Kreen / Skype Technologies OU).

"""Exception hierarchy for pgque."""


class PgqueError(Exception):
    """Base class for all pgque-raised errors."""


class ReceiveOverflowError(PgqueError):
    """A complete receive batch exceeded the configured safety ceiling."""

    def __init__(
        self,
        message: str,
        *,
        sqlstate: str,
        hint: str | None,
        configured_limit: int,
        operation: str,
    ):
        super().__init__(message)
        self.sqlstate = sqlstate
        self.hint = hint
        self.configured_limit = configured_limit
        self.operation = operation


class PgqueConnectionError(PgqueError):
    """Failed to connect to PostgreSQL or the connection was lost."""


class PgqueQueueNotFound(PgqueError):
    """Queue does not exist (raised by pgque SQL with a recognizable message)."""


class PgqueBatchNotFound(PgqueError):
    """Batch ID does not exist or was already finished."""


class PgqueConsumerNotFound(PgqueError):
    """Consumer is not registered on the queue."""
