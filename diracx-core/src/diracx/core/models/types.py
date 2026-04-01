"""Custom types for DiracX pydantic models."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pydantic import AfterValidator, AwareDatetime
from typing_extensions import Annotated


def _validate_utc(v: datetime) -> datetime:
    """Reject aware datetimes that are not in UTC.

    AwareDatetime already rejects naive datetimes before this runs.
    """
    if v.utcoffset() != timedelta(0):
        raise ValueError(f"Datetime must be in UTC, got offset {v.utcoffset()}")
    return v.replace(tzinfo=UTC)


UTCDatetime = Annotated[AwareDatetime, AfterValidator(_validate_utc)]
