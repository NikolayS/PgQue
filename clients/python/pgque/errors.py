# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# PgQue includes code derived from PgQ (ISC license,
# Marko Kreen / Skype Technologies OU).

"""Exception hierarchy for pgque."""


class PgqueError(Exception):
    """Base class for all pgque-raised errors."""


class PgqueReceiveOverflowError(PgqueError):
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

    def __reduce__(self):
        return (
            _restore_receive_overflow_error,
            (
                str(self),
                self.sqlstate,
                self.hint,
                self.configured_limit,
                self.operation,
            ),
        )


def _restore_receive_overflow_error(
    message: str,
    sqlstate: str,
    hint: str | None,
    configured_limit: int,
    operation: str,
) -> PgqueReceiveOverflowError:
    return PgqueReceiveOverflowError(
        message,
        sqlstate=sqlstate,
        hint=hint,
        configured_limit=configured_limit,
        operation=operation,
    )


class PgqueConnectionError(PgqueError):
    """Failed to connect to PostgreSQL or the connection was lost."""


class PgqueQueueNotFound(PgqueError):
    """Queue does not exist (raised by pgque SQL with a recognizable message)."""


class PgqueBatchNotFound(PgqueError):
    """Batch ID does not exist or was already finished."""


class PgqueConsumerNotFound(PgqueError):
    """Consumer is not registered on the queue."""
