from __future__ import annotations

__all__ = [
    "ExponentialBackoff",
    "NoRetry",
    "RetryPolicyBase",
]

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

        """
        ...


class NoRetry(RetryPolicyBase):
    """A retry policy that never retries."""

    def schedule_retry(self, attempt: int, exception: Exception) -> None:
        return None


class ExponentialBackoff(RetryPolicyBase):
    """Exponential backoff retry policy."""

    base_delay_seconds: int = 10
    max_retries: int = 5

    def schedule_retry(self, attempt: int, exception: Exception) -> datetime | None:
        if attempt >= self.max_retries:
            return None
        delay = self.base_delay_seconds * (2**attempt)
        return datetime.now(tz=UTC) + timedelta(seconds=delay)
