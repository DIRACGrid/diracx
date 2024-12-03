from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from diracx.core.exceptions import InvalidQueryError
from diracx.db.sql.utils import SQLDBUnavailableError

from gubbins.db.sql.lollygag.db import LollygagDB

if TYPE_CHECKING:
    from typing import AsyncGenerator

# Each DB test class must defined a fixture looking like this one
# It allows to get an instance of an in memory DB,


@pytest.fixture
async def lollygag_db(tmp_path) -> AsyncGenerator[LollygagDB, None]:
    lollygag_db = LollygagDB("sqlite+aiosqlite:///:memory:")
    async with lollygag_db.engine_context():
        async with lollygag_db.engine.begin() as conn:
            await conn.run_sync(lollygag_db.metadata.create_all)
        yield lollygag_db


async def test_insert_and_summary(lollygag_db: LollygagDB):
    # Each context manager creates a transaction
    # So it is important to write test this way
    async with lollygag_db as lollygag_db:
        # First we check that the DB is empty
        result = await lollygag_db.summary(["Model"], [])
        assert not result

    # Now we add some data in the DB
    async with lollygag_db as lollygag_db:
        # Add a car owner
        owner_id = await lollygag_db.insert_owner(name="Magnum")
        assert owner_id

        # Add cars, belonging to the same guy
        result = await asyncio.gather(
            *(
                lollygag_db.insert_car(uuid4(), f"model_{i}", owner_id)
                for i in range(10)
            )
        )
        assert result

    # Check that there are now 10 cars assigned to a single driver
    async with lollygag_db as lollygag_db:
        result = await lollygag_db.summary(["OwnerID"], [])

        assert result[0]["count"] == 10

    # Test the selection
    async with lollygag_db as lollygag_db:
        result = await lollygag_db.summary(
            ["OwnerID"], [{"parameter": "Model", "operator": "eq", "value": "model_1"}]
        )

        assert result[0]["count"] == 1

    async with lollygag_db as lollygag_db:
        with pytest.raises(InvalidQueryError):
            result = await lollygag_db.summary(
                ["OwnerID"],
                [
                    {
                        "parameter": "Model",
                        "operator": "BADSELECTION",
                        "value": "model_1",
                    }
                ],
            )


async def test_bad_connection():
    lollygag_db = LollygagDB("mysql+aiomysql://tata:yoyo@db.invalid:3306/name")
    async with lollygag_db.engine_context():
        with pytest.raises(SQLDBUnavailableError):
            async with lollygag_db:
                lollygag_db.ping()
