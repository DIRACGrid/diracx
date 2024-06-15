from __future__ import annotations

from contextlib import asynccontextmanager
from functools import partial


def fake_available_implementations(name, *, real_available_implementations):
    from diracx.testing.mock_osdb import MockOSDBMixin

    implementations = real_available_implementations(name)

    # Dynamically generate a class that inherits from the first implementation
    # but that also has the MockOSDBMixin
    MockParameterDB = type(name, (MockOSDBMixin, implementations[0]), {})

    return [MockParameterDB] + implementations


@asynccontextmanager
async def ensure_dbs_exist():
    from diracx.db.__main__ import init_os, init_sql

    await init_sql()
    await init_os()
    yield


def create_app():
    """Create a FastAPI application for testing purposes.

    This is a wrapper around diracx.routers.create_app that:
     * adds a lifetime function to ensure the DB schemas are initialized
     * replaces the parameter DBs with sqlite-backed versions
    """
    from diracx.db.os.utils import BaseOSDB
    from diracx.routers import create_app

    BaseOSDB.available_implementations = partial(
        fake_available_implementations,
        real_available_implementations=BaseOSDB.available_implementations,
    )

    app = create_app()
    app.lifetime_functions.append(ensure_dbs_exist)

    return app
