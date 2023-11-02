from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from diracx.core.exceptions import InvalidQueryError
from diracx.db.sql.dummy.db import DummyDB
from diracx.db.sql.utils import SQLDBUnavailable

# Each DB test class must defined a fixture looking like this one
# It allows to get an instance of an in memory DB,


@pytest.fixture
async def dummy_db(tmp_path) -> DummyDB:
    dummy_db = DummyDB("sqlite+aiosqlite:///:memory:")
    async with dummy_db.engine_context():
        async with dummy_db.engine.begin() as conn:
            await conn.run_sync(dummy_db.metadata.create_all)
        yield dummy_db


async def test_insert_and_summary(dummy_db: DummyDB):
    # Each context manager creates a transaction
    # So it is important to write test this way
    async with dummy_db as dummy_db:
        # First we check that the DB is empty
        result = await dummy_db.summary(["model"], [])
        assert not result

    # Now we add some data in the DB
    async with dummy_db as dummy_db:
        # Add a car owner
        owner_id = await dummy_db.insert_owner(name="Magnum")
        assert owner_id

        # Add cars, belonging to the same guy
        result = await asyncio.gather(
            *(dummy_db.insert_car(uuid4(), f"model_{i}", owner_id) for i in range(10))
        )
        assert result

    # Check that there are now 10 cars assigned to a single driver
    async with dummy_db as dummy_db:
        result = await dummy_db.summary(["ownerID"], [])

        assert result[0]["count"] == 10

    # Test the selection
    async with dummy_db as dummy_db:
        result = await dummy_db.summary(
            ["ownerID"], [{"parameter": "model", "operator": "eq", "value": "model_1"}]
        )

        assert result[0]["count"] == 1

    async with dummy_db as dummy_db:
        with pytest.raises(InvalidQueryError):
            result = await dummy_db.summary(
                ["ownerID"],
                [
                    {
                        "parameter": "model",
                        "operator": "BADSELECTION",
                        "value": "model_1",
                    }
                ],
            )


async def test_bad_connection():
    dummy_db = DummyDB("mysql+aiomysql://tata:yoyo@db.invalid:3306/name")
    async with dummy_db.engine_context():
        with pytest.raises(SQLDBUnavailable):
            async with dummy_db:
                dummy_db.ping()
