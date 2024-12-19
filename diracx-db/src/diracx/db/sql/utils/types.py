from __future__ import annotations

from functools import partial

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

    impl = types.Enum
    cache_ok: bool = True

    def __init__(self) -> None:
        super().__init__("True", "False")

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
