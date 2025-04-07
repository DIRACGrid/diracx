from __future__ import annotations

__all__ = ("BaseOSDB",)

import contextlib
import json
import logging
import os
from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterator
from contextvars import ContextVar
from datetime import datetime
from typing import Any, Self

from opensearchpy import AsyncOpenSearch

from diracx.core.exceptions import InvalidQueryError
from diracx.core.extensions import select_from_extension
from diracx.db.exceptions import DBUnavailableError

logger = logging.getLogger(__name__)


class OpenSearchDBError(Exception):
    pass


class OpenSearchDBUnavailableError(DBUnavailableError, OpenSearchDBError):
    pass


class BaseOSDB(metaclass=ABCMeta):
    """This should be the base class of all the OpenSearch DiracX DBs.

    The details covered here should be handled automatically by the service and
    task machinery of DiracX and this documentation exists for informational
    purposes.

    The available OpenSearch databases are discovered by calling `BaseOSDB.available_urls`.
    This method returns a dictionary of database names to connection parameters.
    The available databases are determined by the `diracx.dbs.os` entrypoint in
    the `pyproject.toml` file and the connection parameters are taken from the
    environment variables prefixed with `DIRACX_OS_DB_{DB_NAME}`.

    If extensions to DiracX are being used, there can be multiple implementations
    of the same database. To list the available implementations use
    `BaseOSDB.available_implementations(db_name)`. The first entry in this list
    will be the preferred implementation and it can be initialized by calling
    its `__init__` function with the connection parameters previously obtained
    from `BaseOSDB.available_urls`.

    To control the lifetime of the OpenSearch client, the `BaseOSDB.client_context`
    asynchronous context manager should be entered. When inside this context
    manager, the client can be accessed with `BaseOSDB.client`.

    Upon entering, the DB class can then be used as an asynchronous context
    manager to perform operations. Currently this context manager has no effect
    however it must be used as it may be used in future. When inside this
    context manager, the DB connection can be accessed with `BaseOSDB.client`.

    For example:

    ```python
    db_name = ...
    conn_params = BaseOSDB.available_urls()[db_name]
    MyDBClass = BaseOSDB.available_implementations(db_name)[0]

    db = MyDBClass(conn_params)
    async with db.client_context:
        async with db:
            # Do something with the OpenSearch client
    ```
    """

    # TODO: Make metadata an abstract property
    fields: dict
    index_prefix: str

    @abstractmethod
    def index_name(self, vo: str, doc_id: int) -> str: ...

    def __init__(self, connection_kwargs: dict[str, Any]) -> None:
        self._client: AsyncOpenSearch | None = None
        self._connection_kwargs = connection_kwargs
        # We use a ContextVar to make sure that self._conn
        # is specific to each context, and avoid parallel
        # route executions to overlap
        self._conn: ContextVar[bool] = ContextVar("_conn", default=False)

    @classmethod
    def available_implementations(cls, db_name: str) -> list[type[BaseOSDB]]:
        """Return the available implementations of the DB in reverse priority order."""
        db_classes: list[type[BaseOSDB]] = [
            entry_point.load()
            for entry_point in select_from_extension(
                group="diracx.dbs.os", name=db_name
            )
        ]
        if not db_classes:
            raise NotImplementedError(f"Could not find any matches for {db_name=}")
        return db_classes

    @classmethod
    def available_urls(cls) -> dict[str, dict[str, Any]]:
        """Return a dict of available OpenSearch database urls.

        The list of available URLs is determined by environment variables
        prefixed with ``DIRACX_OS_DB_{DB_NAME}``.
        """
        conn_kwargs: dict[str, dict[str, Any]] = {}
        for entry_point in select_from_extension(group="diracx.dbs.os"):
            db_name = entry_point.name
            var_name = f"DIRACX_OS_DB_{entry_point.name.upper()}"
            if var_name in os.environ:
                try:
                    conn_kwargs[db_name] = json.loads(os.environ[var_name])
                except Exception:
                    logger.error("Error loading connection parameters for %s", db_name)
                    raise
        return conn_kwargs

    @classmethod
    def session(cls) -> Self:
        """This is just a fake method such that the Dependency overwrite has
        a hash to use.
        """
        raise NotImplementedError("This should never be called")

    @property
    def client(self) -> AsyncOpenSearch:
        """Just a getter for _client, making sure we entered
        the context manager.
        """
        if self._client is None:
            raise RuntimeError(f"{self.__class__} was used before entering")
        return self._client

    @contextlib.asynccontextmanager
    async def client_context(self) -> AsyncIterator[None]:
        """Context manage to manage the client lifecycle.
        This is called when starting fastapi.

        """
        assert self._client is None, "client_context cannot be nested"
        async with AsyncOpenSearch(**self._connection_kwargs) as self._client:
            try:
                yield
            finally:
                self._client = None

    async def ping(self):
        """Check whether the connection to the DB is still working.
        We could enable the ``pre_ping`` in the engine, but this would
        be ran at every query.
        """
        if not await self.client.ping():
            raise OpenSearchDBUnavailableError(
                f"Failed to connect to {self.__class__.__qualname__}"
            )

    async def __aenter__(self):
        """This is entered on every request.
        At the moment it does nothing, however, we keep it here
        in case we ever want to use OpenSearch equivalent of a transaction.
        """
        assert not self._conn.get(), "BaseOSDB context cannot be nested"
        assert self._client is not None, "client_context hasn't been entered"
        self._conn.set(True)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        assert self._conn.get()
        self._conn.set(False)

    async def create_index_template(self) -> None:
        template_body = {
            "template": {"mappings": {"properties": self.fields}},
            "index_patterns": [f"{self.index_prefix}*"],
        }
        result = await self.client.indices.put_index_template(
            name=self.index_prefix, body=template_body
        )
        assert result["acknowledged"]

    async def upsert(self, vo: str, doc_id: int, document: Any) -> None:
        index_name = self.index_name(vo, doc_id)
        response = await self.client.update(
            index=index_name,
            id=doc_id,
            body={"doc": document, "doc_as_upsert": True},
            params=dict(retry_on_conflict=10),
        )
        logger.debug(
            "Upserted document %s in index %s with response: %s",
            doc_id,
            index_name,
            response,
        )

    async def search(
        self, parameters, search, sorts, *, per_page: int = 100, page: int | None = None
    ) -> list[dict[str, Any]]:
        """Search the database for matching results.

        See the DiracX search API documentation for details.
        """
        body = {}
        if parameters:
            body["_source"] = parameters
        if search:
            body["query"] = apply_search_filters(self.fields, search)
        body["sort"] = []
        for sort in sorts:
            field_name = sort["parameter"]
            field_type = self.fields.get(field_name, {}).get("type")
            require_type("sort", field_name, field_type, {"keyword", "long", "date"})
            body["sort"].append({field_name: {"order": sort["direction"]}})

        params = {}
        if page is not None:
            params["from"] = (page - 1) * per_page
            params["size"] = per_page

        response = await self.client.search(
            body=body, params=params, index=f"{self.index_prefix}*"
        )
        hits = [hit["_source"] for hit in response["hits"]["hits"]]

        # Dates are returned as strings, convert them to Python datetimes
        for hit in hits:
            for field_name in hit:
                if field_name not in self.fields:
                    continue
                if self.fields[field_name]["type"] == "date":
                    hit[field_name] = datetime.strptime(
                        hit[field_name], "%Y-%m-%dT%H:%M:%S.%f%z"
                    )

        return hits


def require_type(operator, field_name, field_type, allowed_types):
    if field_type not in allowed_types:
        raise InvalidQueryError(
            f"Cannot apply {operator} to {field_name} ({field_type=}, {allowed_types=})"
        )


def apply_search_filters(db_fields, search):
    """Build an OpenSearch query from the given DiracX search parameters.

    If the searched parameters cannot be efficiently translated to a query for
    OpenSearch an InvalidQueryError exception is raised.
    """
    result = {
        "must": [],
        "must_not": [],
    }
    for query in search:
        field_name = query["parameter"]
        field_type = db_fields.get(field_name, {}).get("type")
        if field_type is None:
            raise InvalidQueryError(
                f"Field {field_name} is not included in the index mapping"
            )

        match operator := query["operator"]:
            case "eq":
                require_type(
                    operator, field_name, field_type, {"keyword", "long", "date"}
                )
                result["must"].append({"term": {field_name: {"value": query["value"]}}})
            case "neq":
                require_type(
                    operator, field_name, field_type, {"keyword", "long", "date"}
                )
                result["must_not"].append(
                    {"term": {field_name: {"value": query["value"]}}}
                )
            case "gt":
                require_type(operator, field_name, field_type, {"long", "date"})
                result["must"].append({"range": {field_name: {"gt": query["value"]}}})
            case "lt":
                require_type(operator, field_name, field_type, {"long", "date"})
                result["must"].append({"range": {field_name: {"lt": query["value"]}}})
            case "in":
                require_type(
                    operator, field_name, field_type, {"keyword", "long", "date"}
                )
                result["must"].append({"terms": {field_name: query["values"]}})
            case "not in":
                require_type(
                    operator, field_name, field_type, {"keyword", "long", "date"}
                )
                result["must_not"].append({"terms": {field_name: query["values"]}})
            # TODO: Implement like and ilike
            # If the pattern is a simple "col like 'abc%'", we can use a prefix query
            # Else we need to use a wildcard query where we replace % with * and _ with ?
            # This should also need to handle escaping of %/_/*/?
            case _:
                raise InvalidQueryError(f"Unknown filter {query=}")

    return {"bool": result}
