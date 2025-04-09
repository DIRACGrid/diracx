from __future__ import annotations

from datetime import datetime
from functools import partial
from zoneinfo import ZoneInfo

import sqlalchemy.types as types
import tzlocal
from sqlalchemy import Column as RawColumn
from sqlalchemy import DateTime, Enum

from .functions import utcnow

Column: partial[RawColumn] = partial(RawColumn, nullable=False)
NullColumn: partial[RawColumn] = partial(RawColumn, nullable=True)
DateNowColumn = partial(Column, type_=DateTime(timezone=True), server_default=utcnow())


def EnumColumn(name, enum_type, **kwargs):  # noqa: N802
    return Column(name, Enum(enum_type, native_enum=False, length=16), **kwargs)


def get_local_timezone():
    return tzlocal.get_localzone()


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


class StringParsingDateTime(types.TypeDecorator):
    """A DateTime type that also accepts ISO8601 strings.

    Takes into account converting timezone aware datetime objects into
    naive form if required.

    """

    impl = DateTime()
    cache_ok = True

    def __init__(self, tz: ZoneInfo | None=None, stored_naive=True):
        self._stored_naive = stored_naive
        self._tz: ZoneInfo | None = tz  # None = Local timezone

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
                " e.g. datetime.now(tz=timezone.utc) or use tzlocal.get_localzone() if you need to "
                "attach tzinfo to a local naive timestamp."
            )

        # Check that we need to convert the timezone to match self._tz timezone:
            # e.g. _stored_naive is True
            #   --> We need to ensure the datetime is in the correct timezone
            #   if tzinfo in the provided timestamp is None, we assume already correct
        if self._stored_naive and value.tzinfo is not None:
            # if self._tz is None, we use our system timezone.
            stored_tz = self._tz or get_local_timezone()

            # astimezone converts to the stored timezone
            # replace strips the TZ info --> naive datetime object
            value = value.astimezone(tz=stored_tz).replace(tzinfo=None)

        return value

    def process_result_value(self, value, dialect):
        if self._stored_naive:
            if isinstance(value, datetime) and not value.tzinfo:
                # add back the timezone info
                value = value.replace(tzinfo=self._tz)

                if value.tzinfo is None:
                    # if stored as a local time, add back the timezone info...
                    value = value.replace(tzinfo=get_local_timezone())

        return value
