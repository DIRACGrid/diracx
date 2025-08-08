from __future__ import annotations

from datetime import datetime
from functools import partial
from zoneinfo import ZoneInfo

import sqlalchemy.types as types
from sqlalchemy import Column as RawColumn
from sqlalchemy import DateTime, Enum

from .functions import utcnow

Column: partial[RawColumn] = partial(RawColumn, nullable=False)
NullColumn: partial[RawColumn] = partial(RawColumn, nullable=True)
DateNowColumn = partial(Column, type_=DateTime(timezone=True), server_default=utcnow())


def EnumColumn(name, enum_type, **kwargs):  # noqa: N802
    return Column(name, Enum(enum_type, native_enum=False, length=16), **kwargs)


class EnumBackedBool(types.TypeDecorator):
    """Maps a ``EnumBackedBool()`` column to True/False in Python."""

    impl = types.Enum("True", "False", name="enum_backed_bool")
    cache_ok = True

    def process_bind_param(self, value, dialect) -> str:
        if value is True:
            return "True"
        elif value is False:
            return "False"
        else:
            raise NotImplementedError(value, dialect)

    def process_result_value(self, value, dialect) -> bool:
        if value == "True":
            return True
        elif value == "False":
            return False
        else:
            raise NotImplementedError(f"Unknown {value=}")


class SmarterDateTime(types.TypeDecorator):
    """A DateTime type that also accepts ISO8601 strings.

    Takes into account converting timezone aware datetime objects into
    naive form and back when needed.

    """

    impl = DateTime()
    cache_ok = True

    def __init__(
        self,
        stored_tz: ZoneInfo | None = ZoneInfo("UTC"),
        returned_tz: ZoneInfo = ZoneInfo("UTC"),
        stored_naive_sqlite=True,
        stored_naive_mysql=True,
        stored_naive_postgres=False,  # Forces timezone-awareness
    ):
        self._stored_naive_dialect = {
            "sqlite": stored_naive_sqlite,
            "mysql": stored_naive_mysql,
            "postgres": stored_naive_postgres,
        }
        self._stored_tz: ZoneInfo | None = stored_tz  # None = Local timezone
        self._returned_tz: ZoneInfo = returned_tz

    def _stored_naive(self, dialect):
        if dialect.name not in self._stored_naive_dialect:
            raise NotImplementedError(dialect.name)
        return self._stored_naive_dialect.get(dialect.name)

    def process_bind_param(self, value, dialect):
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
