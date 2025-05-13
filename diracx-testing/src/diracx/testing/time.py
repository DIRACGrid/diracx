"""Helper functions for mocking time functions.

The main functionality in this module is to mock SQLite's date and time
functions. This is useful for testing purposes, especially when using
freezegun to freeze time.

Example usage to mock SQLite time functions in SQLAlchemy for connections in
a given engine:

    import sqlalchemy

    from diracx.testing.time import mock_sqlite_time

    sqlalchemy.event.listen(engine.sync_engine, "connect", mock_sqlite_time)

This functionality is tested in the `diracx-db` tests, specifically in the
`test_freeze_time.py` file.
"""

from __future__ import annotations

__all__ = [
    "mock_sqlite_time",
    "julian_date",
]

import re
from datetime import UTC, datetime

RE_SQLITE_TIME = re.compile(r"(\d{4})-(\d{2})-(\d{2})(?: (\d{2}):(\d{2}):(\d{2}))?")


def mock_sqlite_time(dbapi_connection, connection_record):
    """Override SQLite’s date and time functions.

    See: https://sqlite.org/lang_datefunc.html#tmval
    """

    # 1. date(time-value, modifier, modifier, ...)
    def date_mock(*args):
        """Mock for DATE() function."""
        raise NotImplementedError(args)

    # 2. time(time-value, modifier, modifier, ...)
    def time_mock(*args):
        """Mock for TIME() function."""
        raise NotImplementedError(args)

    # 3. datetime(time-value, modifier, modifier, ...)
    def datetime_mock(*args):
        """Mock for DATETIME() function."""
        if len(args) == 0:
            args = ("now",)
        if len(args) > 1:
            raise NotImplementedError(args)

        time_value = args[0]
        if time_value == "now":
            return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

        raise NotImplementedError(args)

    # 7. julianday(time-value, modifier, modifier, ...)
    def julianday_mock(*args):
        """Mock for JULIANDAY() function."""
        if len(args) == 0:
            args = ("now",)
        if len(args) > 1:
            raise NotImplementedError(args)

        if args[0] == "now":
            return julian_date(datetime.now(UTC))
        if match := RE_SQLITE_TIME.fullmatch(args[0]):
            parts = [0 if x is None else int(x) for x in match.groups()]
            return julian_date_from_parts(*parts)
        if match := re.fullmatch(r"(\d{4})(?:\.(\d{2}))?", args[0]):
            return float(args[0])

        raise NotImplementedError(args[0])

    # 5. unixepoch(time-value, modifier, modifier, ...)
    def unixepoch_mock(*args):
        """Mock for UNIXEPOCH() function."""
        raise NotImplementedError(args)

    # 6. strftime(format, time-value, modifier, modifier, ...)
    def strftime_mock(*args):
        """Mock for STRFTIME() function."""
        raise NotImplementedError(args)

    # 7. timediff(time-value, time-value)
    def timediff_mock(*args):
        """Mock for TIMEDIFF() function."""
        raise NotImplementedError(args)

    conn = dbapi_connection
    conn.create_function("DATE", -1, date_mock)
    conn.create_function("TIME", -1, time_mock)
    conn.create_function("DATETIME", -1, datetime_mock)
    conn.create_function("JULIANDAY", -1, julianday_mock)
    conn.create_function("UNIXEPOCH", -1, unixepoch_mock)
    conn.create_function("STRFTIME", -1, strftime_mock)
    conn.create_function("TIMEDIFF", -1, timediff_mock)


def julian_date(dt: datetime) -> float:
    """Convert a datetime to Julian Date."""
    if dt.tzinfo is None:
        raise ValueError("Datetime must be timezone-aware")
    if dt.tzinfo.utcoffset(dt) is None:
        raise ValueError("Datetime must be timezone-aware")
    dt = dt.astimezone(UTC)

    return julian_date_from_parts(
        dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second
    )


def julian_date_from_parts(
    year: int, month: int, day: int, hour: int, minute: int, second: int
) -> float:
    """Convert year, month, day, hour, minute, second to Julian Date.

    The time MUST be in UTC for the conversion to be correct.
    """
    # Step 1: break out the "month shift"
    a = (14 - month) // 12
    y = year + 4800 - a
    m = month + 12 * a - 3

    # Step 2: compute the (integer) Julian Day Number
    jdn = day + (153 * m + 2) // 5 + 365 * y + y // 4 - y // 100 + y // 400 - 32045

    # Step 3: add fractional day
    return jdn + (hour - 12) / 24 + minute / 1440 + second / 86400
