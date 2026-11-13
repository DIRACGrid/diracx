from __future__ import annotations

import pytest

from diracx.db.sql import TransformationDB
from diracx.testing.sqldb import sql_db_with_schema


@pytest.fixture
async def transformation_db(sql_db_url: str):
    async with sql_db_with_schema(TransformationDB(sql_db_url)) as db:
        yield db
