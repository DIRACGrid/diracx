from __future__ import annotations

import contextlib
import logging
import os
import re
from abc import ABCMeta
from collections.abc import AsyncIterator
from contextvars import ContextVar
from datetime import datetime
from typing import Self, cast

from pydantic import TypeAdapter
from sqlalchemy import DateTime, MetaData, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from diracx.core.exceptions import InvalidQueryError
from diracx.core.extensions import select_from_extension
from diracx.core.models import SortDirection
from diracx.core.settings import SqlalchemyDsn
from diracx.db.exceptions import DBUnavailableError

from .functions import date_trunc

logger = logging.getLogger(__name__)


class SQLDBError(Exception):
    pass


class SQLDBUnavailableError(DBUnavailableError, SQLDBError):
    """Used whenever we encounter a problem with the B connection."""


class BaseSQLDB(metaclass=ABCMeta):
    """This should be the base class of all the SQL DiracX DBs.

    The details covered here should be handled automatically by the service and
    task machinery of DiracX and this documentation exists for informational
    purposes.

    The available databases are discovered by calling `BaseSQLDB.available_urls`.
    This method returns a mapping of database names to connection URLs. The
    available databases are determined by the `diracx.dbs.sql` entrypoint in the
    `pyproject.toml` file and the connection URLs are taken from the environment
    variables of the form `DIRACX_DB_URL_<db-name>`.

    If extensions to DiracX are being used, there can be multiple implementations
    of the same database. To list the available implementations use
    `BaseSQLDB.available_implementations(db_name)`. The first entry in this list
    will be the preferred implementation and it can be initialized by calling
    it's `__init__` function with a URL perviously obtained from
    `BaseSQLDB.available_urls`.

    To control the lifetime of the SQLAlchemy engine used for connecting to the
    database, which includes the connection pool, the `BaseSQLDB.engine_context`
    asynchronous context manager should be entered. When inside this context
    manager, the engine can be accessed with `BaseSQLDB.engine`.

    Upon entering, the DB class can then be used as an asynchronous context
    manager to enter transactions. If an exception is raised the transaction is
    rolled back automatically. If the inner context exits peacefully, the
    transaction is committed automatically. When inside this context manager,
    the DB connection can be accessed with `BaseSQLDB.conn`.

    For example:

    ```python
    db_name = ...
    url = BaseSQLDB.available_urls()[db_name]
    MyDBClass = BaseSQLDB.available_implementations(db_name)[0]

    db = MyDBClass(url)
    async with db.engine_context():
        async with db:
            # Do something in the first transaction
            # Commit will be called automatically

        async with db:
            # This transaction will be rolled back due to the exception
            raise Exception(...)
    ```
    """

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
    def available_implementations(cls, db_name: str) -> list[type["BaseSQLDB"]]:
        """Return the available implementations of the DB in reverse priority order."""
        db_classes: list[type[BaseSQLDB]] = [
            entry_point.load()
            for entry_point in select_from_extension(
                group="diracx.dbs.sql", name=db_name
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
        for entry_point in select_from_extension(group="diracx.dbs.sql"):
            db_name = entry_point.name
            var_name = f"DIRACX_DB_URL_{entry_point.name.upper()}"
            if var_name in os.environ:
                try:
                    db_url = os.environ[var_name]
                    if db_url == "sqlite+aiosqlite:///:memory:":
                        db_urls[db_name] = db_url
                    # pydantic does not allow for underscore in scheme
                    # so we do a special case
                    elif "_" in db_url.split(":")[0]:
                        # Validate the URL with a fake schema, and then store
                        # the original one
                        scheme_id = db_url.find(":")
                        fake_url = (
                            db_url[:scheme_id].replace("_", "-") + db_url[scheme_id:]
                        )
                        TypeAdapter(SqlalchemyDsn).validate_python(fake_url)
                        db_urls[db_name] = db_url

                    else:
                        db_urls[db_name] = str(
                            TypeAdapter(SqlalchemyDsn).validate_python(db_url)
                        )
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

        It is normally not necessary to use the engine directly, unless you are
        doing something special, like writing a test fixture that gives you a db.

        Requires that the engine_context has been entered.
        """
        assert self._engine is not None, "engine_context must be entered"
        return self._engine

    @contextlib.asynccontextmanager
    async def engine_context(self) -> AsyncIterator[None]:
        """Context manage to manage the engine lifecycle.

        This is called once at the application startup (see ``lifetime_functions``).
        """
        assert self._engine is None, "engine_context cannot be nested"

        # Set the pool_recycle to 30mn
        # That should prevent the problem of MySQL expiring connection
        # after 60mn by default
        engine = create_async_engine(self._db_url, pool_recycle=60 * 30)
        self._engine = engine
        try:
            yield
        finally:
            self._engine = None
            await engine.dispose()

    @property
    def conn(self) -> AsyncConnection:
        if self._conn.get() is None:
            raise RuntimeError(f"{self.__class__} was used before entering")
        return cast(AsyncConnection, self._conn.get())

    async def __aenter__(self) -> Self:
        """Create a connection.

        This is called by the Dependency mechanism (see ``db_transaction``),
        It will create a new connection/transaction for each route call.
        """
        assert self._conn.get() is None, "BaseSQLDB context cannot be nested"
        try:
            self._conn.set(await self.engine.connect().__aenter__())
        except Exception as e:
            raise SQLDBUnavailableError(
                f"Cannot connect to {self.__class__.__name__}"
            ) from e

        return self

    async def __aexit__(self, exc_type, exc, tb):
        """This is called when exiting a route.

        If there was no exception, the changes in the DB are committed.
        Otherwise, they are rolled back.
        """
        if exc_type is None:
            await self._conn.get().commit()
        await self._conn.get().__aexit__(exc_type, exc, tb)
        self._conn.set(None)

    async def ping(self):
        """Check whether the connection to the DB is still working.

        We could enable the ``pre_ping`` in the engine, but this would be ran at
        every query.
        """
        try:
            await self.conn.scalar(select(1))
        except OperationalError as e:
            raise SQLDBUnavailableError("Cannot ping the DB") from e


def find_time_resolution(value):
    if isinstance(value, datetime):
        return None, value
    if match := re.fullmatch(
        r"\d{4}(-\d{2}(-\d{2}(([ T])\d{2}(:\d{2}(:\d{2}(\.\d{1,6}Z?)?)?)?)?)?)?", value
    ):
        if match.group(6):
            precision, pattern = "SECOND", r"\1-\2-\3 \4:\5:\6"
        elif match.group(5):
            precision, pattern = "MINUTE", r"\1-\2-\3 \4:\5"
        elif match.group(3):
            precision, pattern = "HOUR", r"\1-\2-\3 \4"
        elif match.group(2):
            precision, pattern = "DAY", r"\1-\2-\3"
        elif match.group(1):
            precision, pattern = "MONTH", r"\1-\2"
        else:
            precision, pattern = "YEAR", r"\1"
        return (
            precision,
            re.sub(
                r"^(\d{4})-?(\d{2})?-?(\d{2})?[ T]?(\d{2})?:?(\d{2})?:?(\d{2})?\.?(\d{1,6})?Z?$",
                pattern,
                value,
            ),
        )

    raise InvalidQueryError(f"Cannot parse {value=}")


def apply_search_filters(column_mapping, stmt, search):
    for query in search:
        try:
            column = column_mapping(query["parameter"])
        except KeyError as e:
            raise InvalidQueryError(f"Unknown column {query['parameter']}") from e

        if isinstance(column.type, DateTime):
            if "value" in query and isinstance(query["value"], str):
                resolution, value = find_time_resolution(query["value"])
                if resolution:
                    column = date_trunc(column, time_resolution=resolution)
                query["value"] = value

            if query.get("values"):
                resolutions, values = zip(
                    *map(find_time_resolution, query.get("values"))
                )
                if len(set(resolutions)) != 1:
                    raise InvalidQueryError(
                        f"Cannot mix different time resolutions in {query=}"
                    )
                if resolution := resolutions[0]:
                    column = date_trunc(column, time_resolution=resolution)
                query["values"] = values

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
        elif query["operator"] == "not in":
            expr = column.notin_(query["values"])
        elif query["operator"] in "like":
            expr = column.like(query["value"])
        elif query["operator"] in "ilike":
            expr = column.ilike(query["value"])
        else:
            raise InvalidQueryError(f"Unknown filter {query=}")
        stmt = stmt.where(expr)
    return stmt


def apply_sort_constraints(column_mapping, stmt, sorts):
    sort_columns = []
    for sort in sorts or []:
        try:
            column = column_mapping(sort["parameter"])
        except KeyError as e:
            raise InvalidQueryError(
                f"Cannot sort by {sort['parameter']}: unknown column"
            ) from e
        sorted_column = None
        if sort["direction"] == SortDirection.ASC:
            sorted_column = column.asc()
        elif sort["direction"] == SortDirection.DESC:
            sorted_column = column.desc()
        else:
            raise InvalidQueryError(f"Unknown sort {sort['direction']=}")
        sort_columns.append(sorted_column)
    if sort_columns:
        stmt = stmt.order_by(*sort_columns)
    return stmt
