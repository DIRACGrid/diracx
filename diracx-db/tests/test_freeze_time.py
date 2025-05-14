from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta, timezone

import freezegun
import pytest
import sqlalchemy
from sqlalchemy.ext.asyncio import create_async_engine

from diracx.testing.time import julian_date, mock_sqlite_time

RE_SQLITE_TIME = re.compile(r"(\d{4})-(\d{2})-(\d{2})(?: (\d{2}):(\d{2}):(\d{2}))?")


@pytest.mark.parametrize(
    "dt, expected",
    [
        (datetime(2000, 1, 1, 18, 0, 0, tzinfo=UTC), 2451545.25),
        (datetime(2000, 1, 1, 6, 0, 0, tzinfo=UTC), 2451544.75),
    ],
)
def test_julian_date(dt, expected):
    """Test the julian_date function with known values."""
    assert julian_date(dt) == expected


def test_julian_date_non_utc():
    """Test the julian_date function with a non-UTC timezone."""
    non_utc = timezone(timedelta(hours=-5))
    dt = datetime(2000, 1, 1, 18, 0, 0, tzinfo=non_utc)
    jd = julian_date(dt)
    # dt in UTC is 2000-01-01 23:00:00, so fractional day is (23-12)/24 = 11/24
    expected = 2451545 + 11 / 24
    assert abs(jd - expected) < 1e-6


@pytest.mark.parametrize("with_mock", [True, False])
async def test_freeze_sqlite_datetime(with_mock):
    """Test the SQLite DATETIME() function with freezegun."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True, echo=True)
    if with_mock:
        sqlalchemy.event.listen(engine.sync_engine, "connect", mock_sqlite_time)

    async with engine.begin() as conn:
        # DATETIME()
        result = await conn.execute(sqlalchemy.text("SELECT DATETIME()"))
        value = datetime.strptime(result.scalar_one(), "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=UTC
        )
        actual = datetime.now(UTC)
        assert abs(value - actual) < timedelta(seconds=1)

        if with_mock:
            with freezegun.freeze_time("2000-10-01 12:00:00"):
                result = await conn.execute(sqlalchemy.text("SELECT DATETIME()"))
                value = datetime.strptime(
                    result.scalar_one(), "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=UTC)
                actual = datetime(2000, 10, 1, 12, 0, 0, tzinfo=UTC)
                assert abs(value - actual) < timedelta(seconds=1)

        # DATETIME('now')
        result = await conn.execute(sqlalchemy.text("SELECT DATETIME('now')"))
        value = datetime.strptime(result.scalar_one(), "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=UTC
        )
        actual = datetime.now(UTC)
        assert abs(value - actual) < timedelta(seconds=1)

        if with_mock:
            with freezegun.freeze_time("2000-10-01 12:00:00"):
                result = await conn.execute(sqlalchemy.text("SELECT DATETIME('now')"))
                value = datetime.strptime(
                    result.scalar_one(), "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=UTC)
                actual = datetime(2000, 10, 1, 12, 0, 0, tzinfo=UTC)
                assert abs(value - actual) < timedelta(seconds=1)


@pytest.mark.parametrize("with_mock", [True, False])
async def test_freeze_sqlite_julianday(with_mock):
    """Test the SQLite JULIANDAY() function with freezegun."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True, echo=True)
    if with_mock:
        sqlalchemy.event.listen(engine.sync_engine, "connect", mock_sqlite_time)

    async with engine.begin() as conn:
        # JULIANDAY()
        result = await conn.execute(sqlalchemy.text("SELECT JULIANDAY()"))
        actual = julian_date(datetime.now(UTC))
        assert abs(result.scalar_one() - actual) < 1e-3

        if with_mock:
            with freezegun.freeze_time("2000-10-01 12:00:00"):
                result = await conn.execute(sqlalchemy.text("SELECT JULIANDAY()"))
                actual = julian_date(datetime(2000, 10, 1, 12, 0, 0, tzinfo=UTC))
                assert abs(result.scalar_one() - actual) < 1e-3

        # JULIANDAY('now')
        result = await conn.execute(sqlalchemy.text("SELECT JULIANDAY('now')"))
        actual = julian_date(datetime.now(UTC))
        assert abs(result.scalar_one() - actual) < 1e-3

        if with_mock:
            with freezegun.freeze_time("2000-10-01 12:00:00"):
                result = await conn.execute(sqlalchemy.text("SELECT JULIANDAY('now')"))
                actual = julian_date(datetime(2000, 10, 1, 12, 0, 0, tzinfo=UTC))
                assert abs(result.scalar_one() - actual) < 1e-3

        # JULIANDAY('YYYY-MM-DD')
        result = await conn.execute(sqlalchemy.text("SELECT JULIANDAY('1996-05-30')"))
        assert result.scalar_one() == 2450233.5

        if with_mock:
            with freezegun.freeze_time("2020-12-13 12:34:56"):
                result = await conn.execute(
                    sqlalchemy.text("SELECT JULIANDAY('1996-05-30')")
                )
                assert result.scalar_one() == 2450233.5

        # JULIANDAY('YYYY-MM-DD HH:MM:SS')
        result = await conn.execute(
            sqlalchemy.text("SELECT JULIANDAY('2000-10-01 12:00:00')")
        )
        assert result.scalar_one() == 2451819.0

        if with_mock:
            with freezegun.freeze_time("2020-12-13 12:34:56"):
                result = await conn.execute(
                    sqlalchemy.text("SELECT JULIANDAY('2000-10-01 12:00:00')")
                )
                assert result.scalar_one() == 2451819.0

        # JULIANDAY('1356')
        result = await conn.execute(sqlalchemy.text("SELECT JULIANDAY('1356')"))
        assert result.scalar_one() == 1356.0

        if with_mock:
            with freezegun.freeze_time("2020-12-13 12:34:56"):
                result = await conn.execute(sqlalchemy.text("SELECT JULIANDAY('1356')"))
                assert result.scalar_one() == 1356.0

        # JULIANDAY('1356.12')
        result = await conn.execute(sqlalchemy.text("SELECT JULIANDAY('1356.12')"))
        assert result.scalar_one() == 1356.12

        if with_mock:
            with freezegun.freeze_time("2020-12-13 12:34:56"):
                result = await conn.execute(
                    sqlalchemy.text("SELECT JULIANDAY('1356.12')")
                )
                assert result.scalar_one() == 1356.12
