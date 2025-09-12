from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID as StdUUID  # noqa: N811

import freezegun
import pytest
from uuid_utils import UUID, uuid7

from diracx.db.sql.utils import uuid7_from_datetime, uuid7_to_datetime


def frozen_uuid7() -> UUID:
    """Create a UUID7 in a way which respects the freezegun context."""
    timestamp = datetime.now(tz=timezone.utc).timestamp()
    return uuid7(int(timestamp), int((timestamp % 1) * 1e9))


class TestDatetimeToUuid7:
    """Test cases for the datetime_to_uuid7 function."""

    def test_datetime_to_uuid7_random(self):
        """Test that the datetime_to_uuid7 function returns a UUID7 with the current timestamp."""
        dt = datetime.fromisoformat("2024-01-15T12:30:45.123456+00:00")
        result = uuid7_from_datetime(dt, randomize=True)
        assert str(result)[:15] == "018d0d1a-5183-7"
        assert len(set(str(result)[15:])) > 4

    def test_datetime_to_uuid7_deterministic(self):
        """Test that the datetime_to_uuid7 function returns a UUID7 with the current timestamp."""
        dt = datetime.fromisoformat("2024-01-15T12:30:45.123456+00:00")
        result = uuid7_from_datetime(dt, randomize=False)
        assert str(result) == "018d0d1a-5183-7000-8000-000000000000"


class TestUuid7ToDatetime:
    """Test cases for the uuid7_to_datetime function."""

    def test_uuid7_now(self):
        """Test that the uuid7 function returns a UUID7 with the current timestamp."""
        test_uuid = frozen_uuid7()
        result = uuid7_to_datetime(test_uuid)
        assert result.tzinfo == timezone.utc
        assert result - datetime.now(tz=timezone.utc) < timedelta(milliseconds=2)

    @freezegun.freeze_time("2024-01-15 12:30:45.123456")
    def test_uuid7_to_datetime_with_uuid_utils_uuid(self):
        """Test conversion with uuid_utils.UUID object."""
        test_uuid = frozen_uuid7()
        result = uuid7_to_datetime(test_uuid)

        assert isinstance(result, datetime)
        assert result.tzinfo == timezone.utc
        assert result.isoformat() == "2024-01-15T12:30:45.123000+00:00"

    @freezegun.freeze_time("2024-01-15 12:30:45.123456")
    def test_uuid7_to_datetime_with_stdlib_uuid(self):
        """Test conversion with standard library UUID object."""
        test_uuid = frozen_uuid7()
        stdlib_uuid = StdUUID(str(test_uuid))

        result = uuid7_to_datetime(stdlib_uuid)
        assert isinstance(result, datetime)
        assert result.tzinfo == timezone.utc

    @freezegun.freeze_time("2024-01-15 12:30:45.123456")
    def test_uuid7_to_datetime_with_string_uuid(self):
        """Test conversion with string UUID that gets converted to uuid_utils.UUID."""
        test_uuid = frozen_uuid7()
        uuid_string = str(test_uuid)

        result = uuid7_to_datetime(uuid_string)
        assert isinstance(result, datetime)
        assert result.tzinfo == timezone.utc

    @freezegun.freeze_time("2024-01-15 12:30:45.123456")
    def test_uuid7_to_datetime_precision(self):
        """Test that the conversion maintains reasonable precision."""
        test_uuid = frozen_uuid7()
        result = uuid7_to_datetime(test_uuid)

        assert isinstance(result, datetime)
        assert result.tzinfo == timezone.utc
        assert result.isoformat() == "2024-01-15T12:30:45.123000+00:00"

    @freezegun.freeze_time("1970-01-01 00:00:00.000000")
    def test_uuid7_to_datetime_epoch_boundary(self):
        """Test conversion with UUID7 at epoch boundary."""
        test_uuid = frozen_uuid7()
        result = uuid7_to_datetime(test_uuid)

        assert isinstance(result, datetime)
        assert result.tzinfo == timezone.utc
        assert result.isoformat() == "1970-01-01T00:00:00+00:00"

    @freezegun.freeze_time("2100-01-01 00:00:00.000000")
    def test_uuid7_to_datetime_future_timestamp(self):
        """Test conversion with future timestamp."""
        test_uuid = frozen_uuid7()
        result = uuid7_to_datetime(test_uuid)

        assert isinstance(result, datetime)
        assert result.tzinfo == timezone.utc
        assert result.isoformat() == "2100-01-01T00:00:00+00:00"

    @freezegun.freeze_time("2024-01-15 12:30:45.123456")
    def test_uuid7_to_datetime_consistency(self):
        """Test that multiple calls with the same UUID return the same result."""
        test_uuid = frozen_uuid7()

        result1 = uuid7_to_datetime(test_uuid)
        result2 = uuid7_to_datetime(test_uuid)

        assert result1.isoformat() == result2.isoformat()
        assert result1 == result2

    @freezegun.freeze_time("2024-01-15 12:30:45.123456")
    def test_uuid7_to_datetime_different_input_types_same_result(self):
        """Test that different input types representing the same UUID return the same result."""
        test_uuid = frozen_uuid7()
        uuid_string = str(test_uuid)
        stdlib_uuid = StdUUID(uuid_string)

        result_from_uuid_utils = uuid7_to_datetime(test_uuid)
        result_from_string = uuid7_to_datetime(uuid_string)
        result_from_stdlib = uuid7_to_datetime(stdlib_uuid)

        # All results should be identical
        assert result_from_uuid_utils == result_from_string == result_from_stdlib

    def test_uuid7_to_datetime_invalid_input_type(self):
        """Test that invalid input types raise appropriate errors."""
        with pytest.raises(TypeError):
            uuid7_to_datetime(123)  # type: ignore

        with pytest.raises(TypeError):
            uuid7_to_datetime(123.45)  # type: ignore

        with pytest.raises(TypeError):
            uuid7_to_datetime([])  # type: ignore

        with pytest.raises(TypeError):
            uuid7_to_datetime({})  # type: ignore

    def test_uuid7_to_datetime_invalid_uuid_string(self):
        """Test that invalid UUID strings raise appropriate errors."""
        with pytest.raises(ValueError):
            uuid7_to_datetime("not-a-uuid")

        with pytest.raises(ValueError):
            uuid7_to_datetime("12345")

        with pytest.raises(ValueError):
            uuid7_to_datetime("")

    def test_uuid7_to_datetime_non_uuid7_uuid(self):
        """Test behavior with non-UUID7 UUIDs."""
        uuid4 = StdUUID("550e8400-e29b-41d4-a716-446655440000")

        with pytest.raises(ValueError, match="is not a UUIDv7"):
            uuid7_to_datetime(uuid4)

    @freezegun.freeze_time("2024-01-15 12:30:45.123456")
    def test_uuid7_to_datetime_timezone_awareness(self):
        """Test that the returned datetime is always timezone-aware and in UTC."""
        test_uuid = frozen_uuid7()
        result = uuid7_to_datetime(test_uuid)

        assert result.tzinfo is not None

        assert result.tzinfo == timezone.utc

        assert result.tzinfo.utcoffset(result) is not None

    @freezegun.freeze_time("2024-01-15 12:30:45.123456")
    def test_uuid7_to_datetime_millisecond_precision(self):
        """Test that the conversion handles millisecond precision correctly."""
        uuids = [frozen_uuid7() for _ in range(5)]
        results = [uuid7_to_datetime(uuid) for uuid in uuids]

        for result in results:
            assert isinstance(result, datetime)
            assert result.tzinfo == timezone.utc

        timestamps = [r.timestamp() for r in results]
        assert timestamps == sorted(timestamps)

    @freezegun.freeze_time("2024-01-15 12:30:45.123456")
    def test_uuid7_to_datetime_timestamp_property(self):
        """Test that the function correctly uses the timestamp property."""
        test_uuid = frozen_uuid7()
        result = uuid7_to_datetime(test_uuid)

        expected_timestamp = test_uuid.timestamp / 1000.0
        actual_timestamp = result.timestamp()

        assert abs(actual_timestamp - expected_timestamp) < 0.001

    @freezegun.freeze_time("2024-01-15 12:30:45.123456")
    def test_uuid7_to_datetime_deterministic_with_freeze(self):
        """Test that the function works deterministically with freezegun."""
        with freezegun.freeze_time("2024-01-15 12:30:45.123000"):
            uuid1 = frozen_uuid7()
            result1 = uuid7_to_datetime(uuid1)

        with freezegun.freeze_time("2024-01-15 12:30:45.123000"):
            uuid2 = frozen_uuid7()
            result2 = uuid7_to_datetime(uuid2)

        time_diff = abs((result1 - result2).total_seconds())
        assert time_diff < 1.0

    @freezegun.freeze_time("1970-01-01 00:00:00")
    def test_uuid7_to_datetime_edge_case_zero_timestamp(self):
        """Test edge case with very small timestamp values."""
        test_uuid = frozen_uuid7()
        result = uuid7_to_datetime(test_uuid)

        assert isinstance(result, datetime)
        assert result.tzinfo == timezone.utc
        assert result.isoformat() == "1970-01-01T00:00:00+00:00"
