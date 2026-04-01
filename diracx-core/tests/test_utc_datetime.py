"""Tests for UTCDatetime pydantic type validation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import BaseModel, ValidationError

from diracx.core.models._types import UTCDatetime


class SampleModel(BaseModel):
    ts: UTCDatetime
    optional_ts: UTCDatetime | None = None


class TestUTCDatetimeAcceptsUTC:
    def test_utc_timezone(self):
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        m = SampleModel(ts=dt)
        assert m.ts == dt
        assert m.ts.tzinfo is UTC

    def test_timezone_utc(self):
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        m = SampleModel(ts=dt)
        assert m.ts.utcoffset() == timedelta(0)

    def test_iso_string_utc(self):
        m = SampleModel(ts="2024-01-01T12:00:00Z")
        assert m.ts.tzinfo is UTC

    def test_iso_string_plus_zero(self):
        m = SampleModel(ts="2024-01-01T12:00:00+00:00")
        assert m.ts.utcoffset() == timedelta(0)

    def test_optional_none(self):
        m = SampleModel(ts="2024-01-01T12:00:00Z", optional_ts=None)
        assert m.optional_ts is None


class TestUTCDatetimeRejectsNonUTC:
    def test_naive_datetime(self):
        dt = datetime(2024, 1, 1, 12, 0, 0)  # noqa: DTZ001
        with pytest.raises(ValidationError, match="timezone"):
            SampleModel(ts=dt)

    def test_non_utc_timezone(self):
        cet = timezone(timedelta(hours=1))
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=cet)
        with pytest.raises(ValidationError, match="must be in UTC"):
            SampleModel(ts=dt)

    def test_iso_string_non_utc(self):
        with pytest.raises(ValidationError, match="must be in UTC"):
            SampleModel(ts="2024-01-01T12:00:00+05:30")

    def test_naive_iso_string(self):
        with pytest.raises(ValidationError):
            SampleModel(ts="2024-01-01T12:00:00")
