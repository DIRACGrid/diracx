from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlalchemy import DateTime
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


class days_since(expression.FunctionElement):  # noqa: N801
    """Sqlalchemy function to get the number of days since a given date.

    Primarily used to be able to query for a specific resolution of a date e.g.

        select * from table where days_since(date_column) = 0
        select * from table where days_since(date_column) = 1
    """

    type = DateTime()
    inherit_cache = False

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


@compiles(days_since, "postgresql")
def pg_days_since(element, compiler, **kw):
    return f"EXTRACT(DAY FROM (now() - {compiler.process(element.clauses)}))"


@compiles(days_since, "mysql")
def mysql_days_since(element, compiler, **kw):
    return f"DATEDIFF(NOW(), {compiler.process(element.clauses)})"


@compiles(days_since, "sqlite")
def sqlite_days_since(element, compiler, **kw):
    return f"julianday('now') - julianday({compiler.process(element.clauses)})"


def substract_date(**kwargs: float) -> datetime:
    return datetime.now(tz=timezone.utc) - timedelta(**kwargs)


def hash(code: str):
    return hashlib.sha256(code.encode()).hexdigest()
