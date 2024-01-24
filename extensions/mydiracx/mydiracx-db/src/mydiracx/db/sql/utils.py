from __future__ import annotations

__all__ = ("utcnow", "Column", "NullColumn", "DateNowColumn", "BaseSQLDB")

import contextlib
import logging
import os
from abc import ABCMeta
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import TYPE_CHECKING, AsyncIterator, Self, cast

from pydantic import parse_obj_as
from sqlalchemy import Column as RawColumn
from sqlalchemy import DateTime, Enum, MetaData, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression

from diracx.core.exceptions import InvalidQueryError
from diracx.core.extensions import select_from_extension
from diracx.core.settings import SqlalchemyDsn
from diracx.db.exceptions import DBUnavailable

if TYPE_CHECKING:
    from sqlalchemy.types import TypeEngine

logger = logging.getLogger(__name__)


class utcnow(expression.FunctionElement):
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


def substract_date(**kwargs: float) -> datetime:
    return datetime.now(tz=timezone.utc) - timedelta(**kwargs)


Column: partial[RawColumn] = partial(RawColumn, nullable=False)
NullColumn: partial[RawColumn] = partial(RawColumn, nullable=True)
DateNowColumn = partial(Column, DateTime(timezone=True), server_default=utcnow())


def EnumColumn(enum_type, **kwargs):
    return Column(Enum(enum_type, native_enum=False, length=16), **kwargs)


class SQLDBError(Exception):
    pass


class SQLDBUnavailable(DBUnavailable, SQLDBError):
    """Used whenever we encounter a problem with the B connection"""


class BaseSQLDB(metaclass=ABCMeta):
    """This should be the base class of all the DiracX DBs"""

    # engine: AsyncEngine
    # TODO: Make metadata an abstract property
    metadata: MetaData

    def __init__(self, db_url: str) -> None:
        # We use a ContextVar to make sure that self._conn
        # is specific to each context, and avoid parallel
        # route executions to overlap
        self._conn: ContextVar[AsyncConnection | None] = ContextVar(
            "_conn", default=None
        )
        self._db_url = db_url
        self._engine: AsyncEngine | None = None

    @classmethod
    def available_implementations(cls, db_name: str) -> list[type[BaseSQLDB]]:
        """Return the available implementations of the DB in reverse priority order."""
        db_classes: list[type[BaseSQLDB]] = [
            entry_point.load()
            for entry_point in select_from_extension(
                group="diracx.db.sql", name=db_name
            )
        ]
        if not db_classes:
            raise NotImplementedError(f"Could not find any matches for {db_name=}")
        return db_classes

    @classmethod
    def available_urls(cls) -> dict[str, str]:
        """Return a dict of available database urls.

        The list of available URLs is determined by environment variables
        prefixed with ``DIRACX_DB_URL_{DB_NAME}``.
        """
        db_urls: dict[str, str] = {}
        for entry_point in select_from_extension(group="diracx.db.sql"):
            db_name = entry_point.name
            var_name = f"DIRACX_DB_URL_{entry_point.name.upper()}"
            if var_name in os.environ:
                try:
                    db_url = os.environ[var_name]
                    if db_url == "sqlite+aiosqlite:///:memory:":
                        db_urls[db_name] = db_url
                    else:
                        db_urls[db_name] = parse_obj_as(SqlalchemyDsn, db_url)
                except Exception:
                    logger.error("Error loading URL for %s", db_name)
                    raise
        return db_urls

    @classmethod
    def transaction(cls) -> Self:
        raise NotImplementedError("This should never be called")

    @property
    def engine(self) -> AsyncEngine:
        """The engine to use for database operations.

        It is normally not necessary to use the engine directly,
        unless you are doing something special, like writing a
        test fixture that gives you a db.


        Requires that the engine_context has been entered.

        """
        assert self._engine is not None, "engine_context must be entered"
        return self._engine

    @contextlib.asynccontextmanager
    async def engine_context(self) -> AsyncIterator[None]:
        """Context manage to manage the engine lifecycle.
        This is called once at the application startup
        (see ``lifetime_functions``)
        """
        assert self._engine is None, "engine_context cannot be nested"

        # Set the pool_recycle to 30mn
        # That should prevent the problem of MySQL expiring connection
        # after 60mn by default
        engine = create_async_engine(self._db_url, pool_recycle=60 * 30)
        self._engine = engine

        yield

        self._engine = None
        await engine.dispose()

    @property
    def conn(self) -> AsyncConnection:
        if self._conn.get() is None:
            raise RuntimeError(f"{self.__class__} was used before entering")
        return cast(AsyncConnection, self._conn.get())

    async def __aenter__(self) -> Self:
        """
        Create a connection.
        This is called by the Dependency mechanism (see ``db_transaction``),
        It will create a new connection/transaction for each route call.
        """
        assert self._conn.get() is None, "BaseSQLDB context cannot be nested"
        try:
            self._conn.set(await self.engine.connect().__aenter__())
        except Exception as e:
            raise SQLDBUnavailable("Cannot connect to DB") from e

        return self

    async def __aexit__(self, exc_type, exc, tb):
        """
        This is called when exciting a route.
        If there was no exception, the changes in the DB are committed.
        Otherwise, they are rollbacked.
        """
        if exc_type is None:
            await self._conn.get().commit()
        await self._conn.get().__aexit__(exc_type, exc, tb)
        self._conn.set(None)

    async def ping(self):
        """
        Check whether the connection to the DB is still working.
        We could enable the ``pre_ping`` in the engine, but this would
        be ran at every query.
        """
        try:
            await self.conn.scalar(select(1))
        except OperationalError as e:
            raise SQLDBUnavailable("Cannot ping the DB") from e


def apply_search_filters(table, stmt, search):
    # Apply any filters
    for query in search:
        column = table.columns[query["parameter"]]
        if query["operator"] == "eq":
            expr = column == query["value"]
        elif query["operator"] == "neq":
            expr = column != query["value"]
        elif query["operator"] == "gt":
            expr = column > query["value"]
        elif query["operator"] == "lt":
            expr = column < query["value"]
        elif query["operator"] == "in":
            expr = column.in_(query["values"])
        elif query["operator"] in "like":
            expr = column.like(query["value"])
        elif query["operator"] in "ilike":
            expr = column.ilike(query["value"])
        else:
            raise InvalidQueryError(f"Unknown filter {query=}")
        stmt = stmt.where(expr)
    return stmt
