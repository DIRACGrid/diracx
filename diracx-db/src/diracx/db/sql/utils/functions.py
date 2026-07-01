"""SQLAlchemy helper SQL functions used by DiracX database models.

This module exposes database-agnostic UTC timestamp and date helper
functions that are compiled to the correct SQL expression for each supported
backend.
"""

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
    """Compile the UTC now expression for PostgreSQL."""
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


@compiles(utcnow, "mssql")
def ms_utcnow(element, compiler, **kw) -> str:
    """Compile the UTC now expression for MSSQL."""
    return "GETUTCDATE()"


@compiles(utcnow, "mysql")
def mysql_utcnow(element, compiler, **kw) -> str:
    """Compile the UTC now expression for MySQL."""
    return "(UTC_TIMESTAMP)"


@compiles(utcnow, "sqlite")
def sqlite_utcnow(element, compiler, **kw) -> str:
    """Compile the UTC now expression for SQLite."""
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
    """Compile the days_since expression for SQLite."""
    return f"julianday('now') - julianday({compiler.process(element.clauses)})"


def substract_date(**kwargs: float) -> datetime:
    """Return a UTC datetime offset by the provided timedelta arguments.

    Args:
        **kwargs (float): Arguments accepted by ``datetime.timedelta``.

    Returns:
        datetime: Current UTC datetime minus the provided timedelta.
    """
    return datetime.now(tz=timezone.utc) - timedelta(**kwargs)


def hash(code: str):
    """Return the SHA-256 hash of the provided text.

    Args:
        code (str): Text to hash.

    Returns:
        str: Hexadecimal SHA-256 digest.
    """
    return hashlib.sha256(code.encode()).hexdigest()
