from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, func
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression

if TYPE_CHECKING:
    from sqlalchemy.types import TypeEngine


class utcnow(expression.FunctionElement):  # noqa: N801
    type: TypeEngine = DateTime()
    inherit_cache: bool = True


@compiles(utcnow, "postgresql")
def pg_utcnow(element, compiler, **kw) -> str:
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


@compiles(utcnow, "mssql")
def ms_utcnow(element, compiler, **kw) -> str:
    return "GETUTCDATE()"


@compiles(utcnow, "mysql")
def mysql_utcnow(element, compiler, **kw) -> str:
    return "(UTC_TIMESTAMP)"


@compiles(utcnow, "sqlite")
def sqlite_utcnow(element, compiler, **kw) -> str:
    return "DATETIME('now')"


class date_trunc(expression.FunctionElement):  # noqa: N801
    """Sqlalchemy function to truncate a date to a given resolution.

    Primarily used to be able to query for a specific resolution of a date e.g.

        select * from table where date_trunc('day', date_column) = '2021-01-01'
        select * from table where date_trunc('year', date_column) = '2021'
        select * from table where date_trunc('minute', date_column) = '2021-01-01 12:00'
    """

    type = DateTime()
    # Cache does not work as intended with time resolution values, so we disable it
    inherit_cache = False

    def __init__(self, *args, time_resolution, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._time_resolution = time_resolution


@compiles(date_trunc, "postgresql")
def pg_date_trunc(element, compiler, **kw):
    res = {
        "SECOND": "second",
        "MINUTE": "minute",
        "HOUR": "hour",
        "DAY": "day",
        "MONTH": "month",
        "YEAR": "year",
    }[element._time_resolution]
    return f"date_trunc('{res}', {compiler.process(element.clauses)})"


@compiles(date_trunc, "mysql")
def mysql_date_trunc(element, compiler, **kw):
    pattern = {
        "SECOND": "%Y-%m-%d %H:%i:%S",
        "MINUTE": "%Y-%m-%d %H:%i",
        "HOUR": "%Y-%m-%d %H",
        "DAY": "%Y-%m-%d",
        "MONTH": "%Y-%m",
        "YEAR": "%Y",
    }[element._time_resolution]

    (dt_col,) = list(element.clauses)
    return compiler.process(func.date_format(dt_col, pattern))


@compiles(date_trunc, "sqlite")
def sqlite_date_trunc(element, compiler, **kw):
    pattern = {
        "SECOND": "%Y-%m-%d %H:%M:%S",
        "MINUTE": "%Y-%m-%d %H:%M",
        "HOUR": "%Y-%m-%d %H",
        "DAY": "%Y-%m-%d",
        "MONTH": "%Y-%m",
        "YEAR": "%Y",
    }[element._time_resolution]
    (dt_col,) = list(element.clauses)
    return compiler.process(
        func.strftime(
            pattern,
            dt_col,
        )
    )


def substract_date(**kwargs: float) -> datetime:
    return datetime.now(tz=timezone.utc) - timedelta(**kwargs)


def hash(code: str):
    return hashlib.sha256(code.encode()).hexdigest()
