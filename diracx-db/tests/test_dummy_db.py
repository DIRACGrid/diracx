from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from diracx.core.exceptions import InvalidQueryError
from diracx.db.sql.dummy.db import DummyDB
from diracx.db.sql.utils import SQLDBUnavailableError

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
        result = await dummy_db.summary(["Model"], [])
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
        result = await dummy_db.summary(["OwnerID"], [])

        assert result[0]["count"] == 10

    # Test the selection
    async with dummy_db as dummy_db:
        result = await dummy_db.summary(
            ["OwnerID"], [{"parameter": "Model", "operator": "eq", "value": "model_1"}]
        )

        assert result[0]["count"] == 1

    async with dummy_db as dummy_db:
        with pytest.raises(InvalidQueryError):
            result = await dummy_db.summary(
                ["OwnerID"],
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
        with pytest.raises(SQLDBUnavailableError):
            async with dummy_db:
                dummy_db.ping()


async def test_successful_transaction(dummy_db):
    """Test SQL transaction model: successful case.

    Successful transactions (no exception raised) should be committed at the end of the context manager.
    """
    # The connection is not created until the context manager is entered
    with pytest.raises(RuntimeError):
        assert dummy_db.conn

    # The connection is created when the context manager is entered
    # This is our transaction
    async with dummy_db as dummy_db:
        assert dummy_db.conn

        # First we check that the DB is empty
        result = await dummy_db.summary(["OwnerID"], [])
        assert not result

        # Add data
        owner_id = await dummy_db.insert_owner(name="Magnum")
        assert owner_id
        result = await asyncio.gather(
            *(dummy_db.insert_car(uuid4(), f"model_{i}", owner_id) for i in range(10))
        )
        assert result

        result = await dummy_db.summary(["OwnerID"], [])
        assert result[0]["count"] == 10

    # The connection is closed when the context manager is exited
    with pytest.raises(RuntimeError):
        assert dummy_db.conn

    # Start a new transaction
    # The previous data should still be there because the transaction was committed (successful)
    async with dummy_db as dummy_db:
        result = await dummy_db.summary(["OwnerID"], [])
        assert result[0]["count"] == 10


async def test_failed_transaction(dummy_db):
    """Test SQL transaction model: failed case.

    Failed transactions (exception raised) should be rolled back at the end of the context manager.
    """
    # The connection is not created until the context manager is entered
    with pytest.raises(RuntimeError):
        assert dummy_db.conn

    # The connection is created when the context manager is entered
    # This is our transaction
    with pytest.raises(KeyError):
        async with dummy_db as dummy_db:
            assert dummy_db.conn

            # First we check that the DB is empty
            result = await dummy_db.summary(["OwnerID"], [])
            assert not result

            # Add data
            owner_id = await dummy_db.insert_owner(name="Magnum")
            assert owner_id
            result = await asyncio.gather(
                *(
                    dummy_db.insert_car(uuid4(), f"model_{i}", owner_id)
                    for i in range(10)
                )
            )
            assert result

            # This will raise an exception and the transaction will be rolled back
            result = await dummy_db.summary(["unexistingfieldraisinganerror"], [])
            assert result[0]["count"] == 10

    # The connection is closed when the context manager is exited
    with pytest.raises(RuntimeError):
        assert dummy_db.conn

    # Start a new transaction
    # The previous data should not be there because the transaction was rolled back (failed)
    async with dummy_db as dummy_db:
        result = await dummy_db.summary(["OwnerID"], [])
        assert not result


async def test_nested_transaction(dummy_db):
    """Test SQL transaction model: nested case.

    Nested transactions are not allowed and raise exceptions.
    """
    # The connection is not created until the context manager is entered
    with pytest.raises(RuntimeError):
        assert dummy_db.conn

    # The connection is created when the context manager is entered
    # This is our transaction
    async with dummy_db as dummy_db:
        assert dummy_db.conn

        with pytest.raises(AssertionError):
            # Start a nested transaction (not allowed)
            async with dummy_db as dummy_db:
                pass

    # The connection is closed when the context manager is exited
    with pytest.raises(RuntimeError):
        assert dummy_db.conn


async def test_successful_with_exception_transaction(dummy_db):
    """Test SQL transaction model: successful case but raising an exception on purpose.

    Successful transactions raising an exception on purpose should be rolled back unless manually committed.
    """
    # The connection is not created until the context manager is entered
    with pytest.raises(RuntimeError):
        assert dummy_db.conn

    # The connection is created when the context manager is entered
    # This is our transaction
    with pytest.raises(RuntimeError):
        async with dummy_db as dummy_db:
            assert dummy_db.conn

            # First we check that the DB is empty
            result = await dummy_db.summary(["OwnerID"], [])
            assert not result

            # Add data
            owner_id = await dummy_db.insert_owner(name="Magnum")
            assert owner_id
            result = await asyncio.gather(
                *(
                    dummy_db.insert_car(uuid4(), f"model_{i}", owner_id)
                    for i in range(10)
                )
            )
            assert result

            result = await dummy_db.summary(["OwnerID"], [])
            assert result[0]["count"] == 10

            # This will raise an exception but the transaction will be rolled back
            if result[0]["count"] == 10:
                raise RuntimeError("This transaction will fail on purpose")

    # The connection is closed when the context manager is exited
    with pytest.raises(RuntimeError):
        assert dummy_db.conn

    # Start a new transaction
    # The previous data should not be there because the transaction was rolled back (failed)
    async with dummy_db as dummy_db:
        result = await dummy_db.summary(["OwnerID"], [])
        assert not result

    # Start a new transaction, this time we commit it manually
    with pytest.raises(RuntimeError):
        async with dummy_db as dummy_db:
            assert dummy_db.conn

            # First we check that the DB is empty
            result = await dummy_db.summary(["OwnerID"], [])
            assert not result

            # Add data
            owner_id = await dummy_db.insert_owner(name="Magnum")
            assert owner_id
            result = await asyncio.gather(
                *(
                    dummy_db.insert_car(uuid4(), f"model_{i}", owner_id)
                    for i in range(10)
                )
            )
            assert result

            result = await dummy_db.summary(["OwnerID"], [])
            assert result[0]["count"] == 10

            # Manually commit the transaction, and then raise an exception
            await dummy_db.conn.commit()

            # This will raise an exception but the transaction will not be rolled back this time
            if result[0]["count"] == 10:
                raise RuntimeError("This transaction will fail on purpose")

    # The connection is closed when the context manager is exited
    with pytest.raises(RuntimeError):
        assert dummy_db.conn

    # Start a new transaction
    # The previous data should be there because the transaction was committed before the exception
    async with dummy_db as dummy_db:
        result = await dummy_db.summary(["OwnerID"], [])
        assert result[0]["count"] == 10
