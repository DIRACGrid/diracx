from __future__ import annotations

import asyncio

import pytest

from diracx.core.exceptions import InvalidQueryError, JobNotFound
from diracx.core.models import (
    ScalarSearchOperator,
    ScalarSearchSpec,
    SortDirection,
    SortSpec,
    VectorSearchOperator,
    VectorSearchSpec,
)
from diracx.db.sql.jobs.db import JobDB


@pytest.fixture
async def job_db(tmp_path):
    job_db = JobDB("sqlite+aiosqlite:///:memory:")
    async with job_db.engine_context():
        async with job_db.engine.begin() as conn:
            # set PRAGMA foreign_keys=ON if sqlite
            if job_db._db_url.startswith("sqlite"):
                await conn.exec_driver_sql("PRAGMA foreign_keys=ON")
            await conn.run_sync(job_db.metadata.create_all)
        yield job_db


async def test_search_parameters(job_db):
    """Test that we can search specific parameters for jobs in the database."""
    async with job_db as job_db:
        result = await job_db.search(["JobID"], [], [])
        assert not result

        result = await asyncio.gather(
            *(
                job_db.insert(
                    f"JDL{i}",
                    "owner",
                    "owner_group",
                    "New",
                    "dfdfds",
                    "lhcb",
                )
                for i in range(100)
            )
        )

    async with job_db as job_db:
        # Search a specific parameter: JobID
        result = await job_db.search(["JobID"], [], [])
        assert result
        for r in result:
            assert r.keys() == {"JobID"}

        # Search a specific parameter: Status
        result = await job_db.search(["Status"], [], [])
        assert result
        for r in result:
            assert r.keys() == {"Status"}

        # Search for multiple parameters: JobID, Status
        result = await job_db.search(["JobID", "Status"], [], [])
        assert result
        for r in result:
            assert r.keys() == {"JobID", "Status"}

        # Search for a non-existent parameter: Dummy
        with pytest.raises(InvalidQueryError):
            result = await job_db.search(["Dummy"], [], [])


async def test_search_conditions(job_db):
    """Test that we can search for specific jobs in the database."""
    async with job_db as job_db:
        result = await asyncio.gather(
            *(
                job_db.insert(
                    f"JDL{i}",
                    f"owner{i}",
                    "owner_group",
                    "New",
                    "dfdfds",
                    "lhcb",
                )
                for i in range(100)
            )
        )

    async with job_db as job_db:
        # Search a specific scalar condition: JobID eq 3
        condition = ScalarSearchSpec(
            parameter="JobID", operator=ScalarSearchOperator.EQUAL, value=3
        )
        result = await job_db.search([], [condition], [])
        assert result
        assert len(result) == 1
        assert result[0]["JobID"] == 3

        # Search a specific scalar condition: JobID lt 3
        condition = ScalarSearchSpec(
            parameter="JobID", operator=ScalarSearchOperator.LESS_THAN, value=3
        )
        result = await job_db.search([], [condition], [])
        assert result
        assert len(result) == 2
        assert result[0]["JobID"] == 1
        assert result[1]["JobID"] == 2

        # Search a specific scalar condition: JobID neq 3
        condition = ScalarSearchSpec(
            parameter="JobID", operator=ScalarSearchOperator.NOT_EQUAL, value=3
        )
        result = await job_db.search([], [condition], [])
        assert result
        assert len(result) == 99
        assert all(r["JobID"] != 3 for r in result)

        # Search a specific scalar condition: JobID eq 5873 (does not exist)
        condition = ScalarSearchSpec(
            parameter="JobID", operator=ScalarSearchOperator.EQUAL, value=5873
        )
        result = await job_db.search([], [condition], [])
        assert not result

        # Search a specific vector condition: JobID in 1,2,3
        condition = VectorSearchSpec(
            parameter="JobID", operator=VectorSearchOperator.IN, values=[1, 2, 3]
        )
        result = await job_db.search([], [condition], [])
        assert result
        assert len(result) == 3
        assert all(r["JobID"] in [1, 2, 3] for r in result)

        # Search a specific vector condition: JobID in 1,2,5873 (one of them does not exist)
        condition = VectorSearchSpec(
            parameter="JobID", operator=VectorSearchOperator.IN, values=[1, 2, 5873]
        )
        result = await job_db.search([], [condition], [])
        assert result
        assert len(result) == 2
        assert all(r["JobID"] in [1, 2] for r in result)

        # Search for multiple conditions based on different parameters: JobID eq 70, JobID in 4,5,6
        condition1 = ScalarSearchSpec(
            parameter="Owner", operator=ScalarSearchOperator.EQUAL, value="owner4"
        )
        condition2 = VectorSearchSpec(
            parameter="JobID", operator=VectorSearchOperator.IN, values=[4, 5, 6]
        )
        result = await job_db.search([], [condition1, condition2], [])
        assert result
        assert len(result) == 1
        assert result[0]["JobID"] == 5
        assert result[0]["Owner"] == "owner4"

        # Search for multiple conditions based on the same parameter: JobID eq 70, JobID in 4,5,6
        condition1 = ScalarSearchSpec(
            parameter="JobID", operator=ScalarSearchOperator.EQUAL, value=70
        )
        condition2 = VectorSearchSpec(
            parameter="JobID", operator=VectorSearchOperator.IN, values=[4, 5, 6]
        )
        result = await job_db.search([], [condition1, condition2], [])
        assert not result


async def test_search_sorts(job_db):
    """"""
    async with job_db as job_db:
        result = await asyncio.gather(
            *(
                job_db.insert(
                    f"JDL{i}",
                    f"owner{i}",
                    "owner_group1" if i < 50 else "owner_group2",
                    "New",
                    "dfdfds",
                    "lhcb",
                )
                for i in range(100)
            )
        )

    async with job_db as job_db:
        # Search and sort by JobID in ascending order
        sort = SortSpec(parameter="JobID", direction=SortDirection.ASC)
        result = await job_db.search([], [], [sort])
        assert result
        for i, r in enumerate(result):
            assert r["JobID"] == i + 1

        # Search and sort by JobID in descending order
        sort = SortSpec(parameter="JobID", direction=SortDirection.DESC)
        result = await job_db.search([], [], [sort])
        assert result
        for i, r in enumerate(result):
            assert r["JobID"] == 100 - i

        # Search and sort by Owner in ascending order
        sort = SortSpec(parameter="Owner", direction=SortDirection.ASC)
        result = await job_db.search([], [], [sort])
        assert result
        # Assert that owner10 is before owner2 because of the lexicographical order
        assert result[2]["Owner"] == "owner10"
        assert result[12]["Owner"] == "owner2"

        # Search and sort by Owner in descending order
        sort = SortSpec(parameter="Owner", direction=SortDirection.DESC)
        result = await job_db.search([], [], [sort])
        assert result
        # Assert that owner10 is before owner2 because of the lexicographical order
        assert result[97]["Owner"] == "owner10"
        assert result[87]["Owner"] == "owner2"

        # Search and sort by OwnerGroup in ascending order and JobID in descending order
        sort1 = SortSpec(parameter="OwnerGroup", direction=SortDirection.ASC)
        sort2 = SortSpec(parameter="JobID", direction=SortDirection.DESC)
        result = await job_db.search([], [], [sort1, sort2])
        assert result
        assert result[0]["OwnerGroup"] == "owner_group1"
        assert result[0]["JobID"] == 50
        assert result[99]["OwnerGroup"] == "owner_group2"
        assert result[99]["JobID"] == 51


async def test_search_pagination(job_db):
    """Test that we can search for jobs in the database."""
    # TODO: Implement pagination
    pass


async def test_set_job_command_invalid_job_id(job_db: JobDB):
    """Test that setting a command for a non-existent job raises JobNotFound."""
    async with job_db as job_db:
        with pytest.raises(JobNotFound):
            await job_db.set_job_command(123456, "test_command")
