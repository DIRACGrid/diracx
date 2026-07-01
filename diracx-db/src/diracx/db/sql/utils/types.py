"""SQL-compatible custom column types and helpers used by DiracX.

This module defines convenience type aliases (for fixed-length strings), a
UTC-aware server default for timestamps and a set of SQLAlchemy
TypeDecorator implementations used across DiracX to normalise datetime
handling and enum-backed boolean columns.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import sqlalchemy.types as types
from sqlalchemy import DateTime, Enum
from sqlalchemy.orm import mapped_column
from typing_extensions import Annotated

from .functions import utcnow

# Module-level constants for default timezone values
_DEFAULT_UTC = ZoneInfo("UTC")

datetime_now = Annotated[
    datetime, mapped_column(DateTime(timezone=True), server_default=utcnow())
]

str32 = Annotated[str, 32]
str64 = Annotated[str, 64]
str128 = Annotated[str, 128]
str255 = Annotated[str, 255]
str512 = Annotated[str, 512]
str1024 = Annotated[str, 1024]


def enum_column(name, enum_type, **kwargs):
    """Create a mapped enum column with a stable representation.

    Args:
        name (str): Column name.
        enum_type (Enum): Python Enum type used for values.
        **kwargs: Additional keyword arguments passed to ``mapped_column``.

    Returns:
        sqlalchemy.orm.mapped_column: Configured mapped column using ``Enum``
            with ``native_enum=False`` so the DB stores strings.
    """
    return mapped_column(name, Enum(enum_type, native_enum=False, length=16), **kwargs)


class EnumBackedBool(types.TypeDecorator):
    """A TypeDecorator mapping an enum with values ``"True"``/``"False"`` to bool.

    This stores boolean values as a short string enum in the database while
    presenting them as Python ``bool`` values in application code.
    """

    impl = types.Enum("True", "False", name="enum_backed_bool")
    cache_ok = True

    def process_bind_param(self, value, dialect) -> str:
        """Convert a Python bool to the database representation.

        Args:
            value (bool | None): Value being bound to the column.
            dialect: SQLAlchemy dialect instance.

        Returns:
            str | None: The string representation stored in the DB.
        """
        if value is True:
            return "True"
        elif value is False:
            return "False"
        else:
            raise NotImplementedError(value, dialect)

    def process_result_value(self, value, dialect) -> bool:
        """Convert the database representation back to Python bool.

        Args:
            value (str | None): The raw value loaded from the DB.
            dialect: SQLAlchemy dialect instance.

        Returns:
            bool | None: Converted boolean value.
        """
        if value == "True":
            return True
        elif value == "False":
            return False
        else:
            raise NotImplementedError(f"Unknown {value=}")


class SmarterDateTime(types.TypeDecorator):
    """A DateTime TypeDecorator that accepts ISO8601 strings and normalises timezones.

    This type handles differences between database engines regarding whether
    datetimes are stored as timezone-aware or naive values. When binding and
    retrieving values it will convert between the application's desired
    timezone and the storage timezone used by the database.

    Args:
        stored_tz (ZoneInfo | None): Timezone to assume when storing values.
            ``None`` indicates the system local timezone. Defaults to UTC.
        returned_tz (ZoneInfo | None): Timezone to convert returned values to.
            Defaults to UTC.
        stored_naive_sqlite (bool): Whether SQLite stores naive datetimes.
        stored_naive_mysql (bool): Whether MySQL stores naive datetimes.
        stored_naive_postgres (bool): Whether Postgres stores naive datetimes.
            Defaults to False (Postgres is timezone-aware by default).
    """

    impl = DateTime()
    cache_ok = True

    def __init__(
        self,
        stored_tz: ZoneInfo | None = None,
        returned_tz: ZoneInfo | None = None,
        stored_naive_sqlite=True,
        stored_naive_mysql=True,
        stored_naive_postgres=False,  # Forces timezone-awareness
    ):
        if stored_tz is None:
            stored_tz = _DEFAULT_UTC
        if returned_tz is None:
            returned_tz = _DEFAULT_UTC
        self._stored_naive_dialect = {
            "sqlite": stored_naive_sqlite,
            "mysql": stored_naive_mysql,
            "postgres": stored_naive_postgres,
        }
        self._stored_tz: ZoneInfo | None = stored_tz  # None = Local timezone
        self._returned_tz: ZoneInfo = returned_tz

    def _stored_naive(self, dialect):
        """Return whether the given dialect stores naive datetimes.

        Raises:
            NotImplementedError: If the dialect is not recognised.
        """
        if dialect.name not in self._stored_naive_dialect:
            raise NotImplementedError(dialect.name)
        return self._stored_naive_dialect.get(dialect.name)

    def process_bind_param(self, value, dialect):
        """Prepare a Python datetime (or ISO string) for storage in the DB.

        Accepts ISO8601 strings and converts them to ``datetime``. Ensures the
        value is timezone-aware and converts it to the configured storage form
        (potentially making it naive when the backend stores naive timestamps).

        Args:
            value (str | datetime | None): Value to bind.
            dialect: SQLAlchemy dialect instance.

        Returns:
            datetime | None: Value ready to be stored in the DB.

        Raises:
            ValueError: If parsing fails or the provided value is not timezone-aware.
        """
        if value is None:
            return None

        if isinstance(value, str):
            try:
                value: datetime = datetime.fromisoformat(value)
            except ValueError as err:
                raise ValueError(f"Unable to parse datetime string: {value}") from err

        if not isinstance(value, datetime):
            raise ValueError(f"Expected datetime or ISO8601 string, but got {value!r}")

        if not value.tzinfo:
            raise ValueError(
                f"Provided timestamp {value=} has no tzinfo -"
                " this is problematic and may cause inconsistencies in stored timestamps.\n"
                " Please always work with tz-aware datetimes / attach tzinfo to your datetime objects:"
                " e.g. datetime.now(tz=timezone.utc) or use datetime_obj.astimezone() with no arguments if you need to "
                "attach the local timezone to a local naive timestamp."
            )

        # Check that we need to convert the timezone to match self._stored_tz timezone:
        if self._stored_naive(dialect):
            # if self._stored_tz is None, we use our local/system timezone.
            stored_tz = self._stored_tz

            # astimezone converts to the stored timezone (local timezone if None)
            # replace strips the TZ info --> naive datetime object
            value = value.astimezone(tz=stored_tz).replace(tzinfo=None)

        return value

    def process_result_value(self, value, dialect):
        """Convert a stored DB datetime value back to the application's timezone.

        Args:
            value (datetime | None): Value loaded from the DB.
            dialect: SQLAlchemy dialect instance.

        Returns:
            datetime | None: Timezone-aware datetime converted to ``returned_tz``.

        Raises:
            NotImplementedError: If the loaded value is not a ``datetime``.
            ValueError: If the DB returned a tz-aware datetime while storage was
                expected to be naive for the dialect.
        """
        if value is None:
            return None
        if not isinstance(value, datetime):
            raise NotImplementedError(f"{value=} not a datetime object")

        if self._stored_naive(dialect):
            # Here we add back the tzinfo to the naive timestamp
            # from the DB to make it aware again.
            if value.tzinfo is None:
                # we are definitely given a naive timestamp, so handle it.
                # add back the timezone info if stored_tz is set
                if self._stored_tz:
                    value = value.replace(tzinfo=self._stored_tz)
                else:
                    # if stored as a local time, add back the system timezone info...
                    value = value.astimezone()
            else:
                raise ValueError(
                    f"stored_naive is True for {dialect.name=}, but the database engine returned "
                    "a tz-aware datetime. You need to check the SQLAlchemy model is consistent with the DB schema."
                )

        # finally, convert the datetime according to the "returned_tz"
        value = value.astimezone(self._returned_tz)

        # phew...
        return value
