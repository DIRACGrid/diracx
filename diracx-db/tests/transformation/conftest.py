"""Transformation databases the tests run against.

Every test using ``transformation_db`` runs on SQLite, and also on each real
server given as the SQLAlchemy URL of an empty database the tests may create
tables in, since MySQL and MariaDB differ from SQLite where it matters here
(``JSON`` normalisation, row locks), e.g.::

    DIRACX_TEST_MYSQL_URL=mysql+aiomysql://root:pw@127.0.0.1:3306/roundtrip
    DIRACX_TEST_MARIADB_URL=mysql+aiomysql://root:pw@127.0.0.1:3307/roundtrip
"""

from __future__ import annotations

import os

import pytest

from diracx.db.sql import TransformationDB

SERVERS = {
    name: os.environ.get(variable)
    for name, variable in [
        ("mysql", "DIRACX_TEST_MYSQL_URL"),
        ("mariadb", "DIRACX_TEST_MARIADB_URL"),
    ]
}


@pytest.fixture(params=["sqlite", *(name for name, url in SERVERS.items() if url)])
async def transformation_db(request):
    db = TransformationDB(SERVERS.get(request.param) or "sqlite+aiosqlite:///:memory:")
    async with db.engine_context():
        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.drop_all)
            await conn.run_sync(db.metadata.create_all)
        try:
            yield db
        finally:
            async with db.engine.begin() as conn:
                await conn.run_sync(db.metadata.drop_all)
