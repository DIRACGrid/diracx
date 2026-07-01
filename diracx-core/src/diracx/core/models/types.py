"""Custom types for DiracX pydantic models."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pydantic import AfterValidator, AwareDatetime
from typing_extensions import Annotated


def _validate_utc(v: datetime) -> datetime:
    """Validate that a datetime is timezone-aware and normalized to UTC.

    Pydantic's ``AwareDatetime`` already rejects naive datetimes before this
    validator runs, so this function only ensures the value is explicitly in
    UTC.

    Args:
        v (datetime): The input datetime value.

    Returns:
        datetime: The normalized datetime with UTC timezone information.
    """
    if v.utcoffset() != timedelta(0):
        raise ValueError(f"Datetime must be in UTC, got offset {v.utcoffset()}")
    return v.replace(tzinfo=UTC)


UTCDatetime = Annotated[AwareDatetime, AfterValidator(_validate_utc)]
"""A timezone-aware datetime that must be normalized to UTC."""
