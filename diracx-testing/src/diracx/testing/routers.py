from __future__ import annotations

from contextlib import asynccontextmanager
from functools import partial

from diracx.testing.mock_osdb import fake_available_osdb_implementations


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
        fake_available_osdb_implementations,
        real_available_implementations=BaseOSDB.available_implementations,
    )

    app = create_app()
    app.lifetime_functions.append(ensure_dbs_exist)

    return app
