from __future__ import annotations

__all__ = (
    "MockOSDBMixin",
    "fake_available_osdb_implementations",
)

import contextlib
from datetime import datetime, timezone
from functools import partial
from typing import Any, AsyncIterator

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from diracx.core.models import SearchSpec, SortSpec
from diracx.db.sql import utils as sql_utils


class MockOSDBMixin:
    """A subclass of DummyOSDB that hacks it to use sqlite as a backed.

    This is only intended for testing and development purposes to avoid the
    need to run a full OpenSearch instance. This class is used by defining a
    new class that inherits from this mixin as well the real DB class, i.e.

    .. code-block:: python

        class JobParametersDB(MockOSDBMixin, JobParametersDB):
            pass

    or

    .. code-block:: python

        JobParametersDB = type("JobParametersDB", (MockOSDBMixin, JobParametersDB), {})
    """

    def __init__(self, connection_kwargs: dict[str, Any]) -> None:
        from sqlalchemy import JSON, Column, Integer, MetaData, String, Table

        from diracx.db.sql.utils import DateNowColumn

        # Dynamically create a subclass of BaseSQLDB so we get clearer errors
        mocked_db = type(f"Mocked{self.__class__.__name__}", (sql_utils.BaseSQLDB,), {})
        self._sql_db = mocked_db(connection_kwargs["sqlalchemy_dsn"])

        # Dynamically create the table definition based on the fields
        columns = [
            Column("doc_id", Integer, primary_key=True),
            Column("extra", JSON, default={}, nullable=False),
        ]
        for field, field_type in self.fields.items():
            match field_type["type"]:
                case "date":
                    column_type = DateNowColumn
                case "long":
                    column_type = partial(Column, type_=Integer)
                case "keyword":
                    column_type = partial(Column, type_=String(255))
                case "text":
                    column_type = partial(Column, type_=String(64 * 1024))
                case _:
                    raise NotImplementedError(f"Unknown field type: {field_type=}")
            columns.append(column_type(field, default=None))
        self._sql_db.metadata = MetaData()
        self._table = Table("dummy", self._sql_db.metadata, *columns)

    @contextlib.asynccontextmanager
    async def client_context(self) -> AsyncIterator[None]:
        async with self._sql_db.engine_context():
            yield

    async def __aenter__(self):
        """Enter the request context.

        This is a no-op as the real OpenSearch class doesn't use transactions.
        Instead we enter a transaction in each method that needs it.
        """
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        pass

    async def create_index_template(self) -> None:
        async with self._sql_db.engine.begin() as conn:
            await conn.run_sync(self._sql_db.metadata.create_all)

    async def upsert(self, vo, doc_id, document) -> None:
        async with self._sql_db:
            values = {}
            for key, value in document.items():
                if key in self.fields:
                    values[key] = value
                else:
                    values.setdefault("extra", {})[key] = value

            stmt = sqlite_insert(self._table).values(doc_id=doc_id, **values)
            # TODO: Upsert the JSON blob properly
            stmt = stmt.on_conflict_do_update(index_elements=["doc_id"], set_=values)
            await self._sql_db.conn.execute(stmt)

    async def search(
        self,
        parameters: list[str] | None,
        search: list[SearchSpec],
        sorts: list[SortSpec],
        *,
        distinct: bool = False,
        per_page: int = 100,
        page: int | None = None,
    ) -> tuple[int, list[dict[Any, Any]]]:
        async with self._sql_db:
            # Apply selection
            if parameters:
                columns = []
                for p in parameters:
                    if p in self.fields:
                        columns.append(self._table.columns[p])
                    else:
                        columns.append(self._table.columns["extra"][p].label(p))
            else:
                columns = self._table.columns
            stmt = select(*columns)
            if distinct:
                stmt = stmt.distinct()

            # Apply filtering
            stmt = sql_utils.apply_search_filters(
                self._table.columns.__getitem__, stmt, search
            )

            # Apply sorting
            stmt = sql_utils.apply_sort_constraints(
                self._table.columns.__getitem__, stmt, sorts
            )

            # Apply pagination
            if page is not None:
                stmt = stmt.offset((page - 1) * per_page).limit(per_page)

            results = []
            async for row in await self._sql_db.conn.stream(stmt):
                result = dict(row._mapping)
                result.pop("doc_id", None)
                if "extra" in result:
                    result.update(result.pop("extra"))
                for k, v in list(result.items()):
                    if isinstance(v, datetime) and v.tzinfo is None:
                        result[k] = v.replace(tzinfo=timezone.utc)
                    if v is None:
                        result.pop(k)
                results.append(result)
        return results

    async def ping(self):
        async with self._sql_db:
            return await self._sql_db.ping()


def fake_available_osdb_implementations(name, *, real_available_implementations):
    implementations = real_available_implementations(name)

    # Dynamically generate a class that inherits from the first implementation
    # but that also has the MockOSDBMixin
    mock_parameter_db = type(name, (MockOSDBMixin, implementations[0]), {})

    return [mock_parameter_db] + implementations
