"""Custom SQLAlchemy functions and helpers for SQL database expressions."""

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
    """SQL expression for the current UTC timestamp.

    Attributes:
        type: SQLAlchemy datetime type returned by the expression.
        inherit_cache: Whether compiled expressions may use SQLAlchemy caching.
    """

    type: TypeEngine = DateTime()
    inherit_cache: bool = True


@compiles(utcnow, "postgresql")
def pg_utcnow(element, compiler, **kw) -> str:
    """Compile ``utcnow`` for PostgreSQL.

    Args:
        element: SQLAlchemy expression being compiled.
        compiler: SQLAlchemy SQL compiler.
        **kw: Additional compiler options.

    Returns:
        PostgreSQL SQL for the current UTC timestamp.
    """
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


@compiles(utcnow, "mssql")
def ms_utcnow(element, compiler, **kw) -> str:
    """Compile ``utcnow`` for Microsoft SQL Server.

    Args:
        element: SQLAlchemy expression being compiled.
        compiler: SQLAlchemy SQL compiler.
        **kw: Additional compiler options.

    Returns:
        SQL Server SQL for the current UTC timestamp.
    """
    return "GETUTCDATE()"


@compiles(utcnow, "mysql")
def mysql_utcnow(element, compiler, **kw) -> str:
    """Compile ``utcnow`` for MySQL.

    Args:
        element: SQLAlchemy expression being compiled.
        compiler: SQLAlchemy SQL compiler.
        **kw: Additional compiler options.

    Returns:
        MySQL SQL for the current UTC timestamp.
    """
    return "(UTC_TIMESTAMP)"


@compiles(utcnow, "sqlite")
def sqlite_utcnow(element, compiler, **kw) -> str:
    """Compile ``utcnow`` for SQLite.

    Args:
        element: SQLAlchemy expression being compiled.
        compiler: SQLAlchemy SQL compiler.
        **kw: Additional compiler options.

    Returns:
        SQLite SQL for the current UTC timestamp.
    """
    return "DATETIME('now')"


class days_since(expression.FunctionElement):  # noqa: N801
    """Sqlalchemy function to get the number of days since a given date.

    Primarily used to be able to query for a specific resolution of a date e.g.

        select * from table where days_since(date_column) = 0
        select * from table where days_since(date_column) = 1

    Attributes:
        type: SQLAlchemy datetime type associated with the expression.
        inherit_cache: Whether compiled expressions may use SQLAlchemy caching.
    """

    type = DateTime()
    inherit_cache = False

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


@compiles(days_since, "postgresql")
def pg_days_since(element, compiler, **kw):
    """Compile ``days_since`` for PostgreSQL.

    Args:
        element: SQLAlchemy expression containing the date column.
        compiler: SQLAlchemy SQL compiler.
        **kw: Additional compiler options.

    Returns:
        PostgreSQL SQL expression calculating elapsed days.
    """
    return f"EXTRACT(DAY FROM (now() - {compiler.process(element.clauses)}))"


@compiles(days_since, "mysql")
def mysql_days_since(element, compiler, **kw):
    """Compile ``days_since`` for MySQL.

    Args:
        element: SQLAlchemy expression containing the date column.
        compiler: SQLAlchemy SQL compiler.
        **kw: Additional compiler options.

    Returns:
        MySQL SQL expression calculating elapsed days.
    """
    return f"DATEDIFF(NOW(), {compiler.process(element.clauses)})"


@compiles(days_since, "sqlite")
def sqlite_days_since(element, compiler, **kw):
    """Compile ``days_since`` for SQLite.

    Args:
        element: SQLAlchemy expression containing the date column.
        compiler: SQLAlchemy SQL compiler.
        **kw: Additional compiler options.

    Returns:
        SQLite SQL expression calculating elapsed days.
    """
    return f"julianday('now') - julianday({compiler.process(element.clauses)})"


def substract_date(**kwargs: float) -> datetime:
    """Subtract a duration from the current UTC datetime.

    Args:
        **kwargs: Keyword duration components accepted by ``datetime.timedelta``.

    Returns:
        Current UTC datetime minus the specified duration.
    """
    return datetime.now(tz=timezone.utc) - timedelta(**kwargs)


def hash(code: str):
    """Return the SHA-256 hexadecimal digest of a string.

    Args:
        code: Text to hash.

    Returns:
        Lowercase hexadecimal SHA-256 digest.
    """
    return hashlib.sha256(code.encode()).hexdigest()
