from __future__ import annotations

__all__ = ("BaseOSDB",)

import contextlib
import json
import logging
import os
from abc import ABCMeta
from typing import Any, AsyncIterator, Self

from opensearchpy import AsyncOpenSearch

from diracx.core.extensions import select_from_extension

logger = logging.getLogger(__name__)


class BaseOSDB(metaclass=ABCMeta):
    # TODO: Make metadata an abstract property
    mapping: dict

    def __init__(self, connection_kwargs: dict[str, Any]) -> None:
        self._client: AsyncOpenSearch | None = None
        self._connection_kwargs = connection_kwargs

    @classmethod
    def available_urls(cls) -> dict[str, dict[str, Any]]:
        """Return a dict of available OpenSearch database urls.

        The list of available URLs is determined by environment variables
        prefixed with ``DIRACX_OS_DB_{DB_NAME}``.
        """
        conn_kwargs: dict[str, dict[str, Any]] = {}
        for entry_point in select_from_extension(group="diracx.os_dbs"):
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
        a hash to use"""
        raise NotImplementedError("This should never be called")

    @property
    def client(self) -> AsyncOpenSearch:
        """Just a getter for _client, making sure we entered
        the context manager"""
        if self._client is None:
            raise RuntimeError(f"{self.__class__} was used before entering")
        return self._client

    @contextlib.asynccontextmanager
    async def client_context(self) -> AsyncIterator[None]:
        """Context manage to manage the client lifecycle.
        This is called when starting fastapi

        """
        assert self._client is None, "client_context cannot be nested"
        async with AsyncOpenSearch(**self._connection_kwargs) as self._client:
            yield
        self._client = None

    async def __aenter__(self):
        """This is entered on every request. It does nothing"""
        assert self._client is None, "client_context hasn't been entered"
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._client = None
        return

    async def search(
        self, parameters, search, sorts, *, per_page: int = 100, page: int | None = None
    ) -> list[dict[str, Any]]:
        return self.client.search(
            body={"query": {"bool": {"must": [{"term": {"JobID": 123}}]}}},
            params=dict(size=per_page),
            index="lhcb-production_elasticjobparameters_index*",
        )
