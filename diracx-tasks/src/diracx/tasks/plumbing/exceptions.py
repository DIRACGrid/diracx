from __future__ import annotations

__all__ = [
    "UnableToAcquireLockError",
    "StopRetryingError",
    "TooManyRetriesError",
    "UnretryableError",
    "TaskRetryRequestedError",
    "SendTaskError",
    "ResultIsMissingError",
]


class UnableToAcquireLockError(Exception):
    """Lock acquisition failed."""


class StopRetryingError(Exception):
    """Base exception to indicate that retries should stop."""


class TooManyRetriesError(StopRetryingError):
    """Task has exceeded its retry limit."""


class UnretryableError(StopRetryingError):
    """Task should not be retried."""


class TaskRetryRequestedError(Exception):
    """Raised by tasks that want to explicitly request a retry."""


class SendTaskError(Exception):
    """Raised when a task cannot be sent to the broker."""


class ResultIsMissingError(Exception):
    """Raised when trying to get a result that doesn't exist."""
