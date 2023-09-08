from __future__ import annotations

__all__ = ("utcnow", "Column", "NullColumn", "DateNowColumn", "BaseDB")

import contextlib
import logging
import os
from abc import ABCMeta
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import TYPE_CHECKING, Self
from collections.abc import AsyncIterator

from pydantic import parse_obj_as
from sqlalchemy import Column as RawColumn
from sqlalchemy import DateTime, Enum, MetaData
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression

from diracx.core.exceptions import InvalidQueryError
from diracx.core.extensions import select_from_extension
from diracx.core.settings import SqlalchemyDsn

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


Column = partial(RawColumn, nullable=False)
NullColumn = partial(RawColumn, nullable=True)
DateNowColumn = partial(Column, DateTime(timezone=True), server_default=utcnow())


def EnumColumn(enum_type, **kwargs):
    return Column(Enum(enum_type, native_enum=False, length=16), **kwargs)


class BaseDB(metaclass=ABCMeta):
    """This should be the base class of all the DiracX DBs"""

    # engine: AsyncEngine
    # TODO: Make metadata an abstract property
    metadata: MetaData

    def __init__(self, db_url: str) -> None:
        self._conn = None
        self._db_url = db_url
        self._engine: AsyncEngine | None = None

    @classmethod
    def available_urls(cls) -> dict[str, str]:
        """Return a dict of available database urls.

        The list of available URLs is determined by environment variables
        prefixed with ``DIRACX_DB_URL_{DB_NAME}``.
        """
        db_urls: dict[str, str] = {}
        for entry_point in select_from_extension(group="diracx.dbs"):
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
                    logger.error("Error loading URL %s for %s", db_name, db_url)
                    raise
        return db_urls

    @classmethod
    def transaction(cls) -> Self:
        raise NotImplementedError("This should never be called")

    @property
    def engine(self) -> AsyncEngine:
        """The engine to use for database operations.

        Requires that the engine_context has been entered.
        """
        assert self._engine is not None, "engine_context must be entered"
        return self._engine

    @contextlib.asynccontextmanager
    async def engine_context(self) -> AsyncIterator[None]:
        """Context manage to manage the engine lifecycle.

        Tables are automatically created upon entering
        """
        assert self._engine is None, "engine_context cannot be nested"

        engine = create_async_engine(
            self._db_url,
            echo=True,
        )
        async with engine.begin() as conn:
            await conn.run_sync(self.metadata.create_all)
        self._engine = engine

        yield

        self._engine = None
        await engine.dispose()

    @property
    def conn(self) -> AsyncConnection:
        if self._conn is None:
            raise RuntimeError(f"{self.__class__} was used before entering")
        return self._conn

    async def __aenter__(self):
        self._conn = await self.engine.connect().__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            await self._conn.commit()
        await self._conn.__aexit__(exc_type, exc, tb)
        self._conn = None


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
            expr = column.like(query["values"])
        else:
            raise InvalidQueryError(f"Unknown filter {query=}")
        stmt = stmt.where(expr)
    return stmt
