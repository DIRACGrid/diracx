"""Retry policies deciding when a failed task is attempted again."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, datetime, timedelta

from pydantic import BaseModel


class RetryPolicyBase(BaseModel, ABC):
    """Abstract base class for retry policies."""

    @abstractmethod
    def schedule_retry(self, attempt: int, exception: Exception) -> datetime | None:
        """Return the datetime for the next retry, or None to stop retrying.

        Args:
            attempt: The current retry attempt number (starting from 1).
            exception: The exception that caused the retry.

        Returns:
            The time of the next retry, or None if the task should not be
            retried.
        """
        ...


class NoRetry(RetryPolicyBase):
    """A retry policy that never retries."""

    def schedule_retry(self, attempt: int, exception: Exception) -> None:
        """Never schedule a retry.

        Args:
            attempt: The current retry attempt number (unused).
            exception: The exception that caused the retry (unused).

        Returns:
            Always None.
        """
        return None


class ExponentialBackoff(RetryPolicyBase):
    """Exponential backoff retry policy."""

    base_delay_seconds: int = 10
    """Delay in seconds, doubled with each attempt."""

    max_retries: int = 5
    """Attempt number from which the task is no longer retried."""

    def schedule_retry(self, attempt: int, exception: Exception) -> datetime | None:
        """Schedule a retry after ``base_delay_seconds * 2**attempt`` seconds.

        Args:
            attempt: The current retry attempt number (starting from 1).
            exception: The exception that caused the retry (unused).

        Returns:
            The time of the next retry, or None once ``max_retries`` is reached.
        """
        if attempt >= self.max_retries:
            return None
        delay = self.base_delay_seconds * (2**attempt)
        return datetime.now(tz=UTC) + timedelta(seconds=delay)
