from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from diracx.core.exceptions import InvalidQueryError
from diracx.core.models import (
    ScalarSearchOperator,
    ScalarSearchSpec,
    SortDirection,
    SortSpec,
    VectorSearchOperator,
    VectorSearchSpec,
)
from diracx.db.sql.job.db import JobDB


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


@pytest.fixture
async def populated_job_db(job_db):
    """Populate the in-memory JobDB with 100 jobs using DAL calls."""
    async with job_db as db:
        jobs_to_insert = {}
        # Insert 100 jobs directly via the DAL.
        for i in range(100):
            compressed_jdl = f"CompressedJDL{i}"
            job_id = await db.create_job(compressed_jdl)
            jobs_to_insert[job_id] = {
                "JobID": job_id,
                "Status": "New",
                "Owner": f"owner{i}",
                "OwnerGroup": "owner_group1" if i < 50 else "owner_group2",
                "VO": "lhcb",
            }
        await db.insert_job_attributes(jobs_to_insert)
    yield job_db


async def test_search_parameters(populated_job_db):
    """Test that we can search specific parameters for jobs in the database."""
    async with populated_job_db as job_db:
        # Search a specific parameter: JobID
        total, result = await job_db.search(["JobID"], [], [])
        assert total == 100
        assert result
        for r in result:
            assert r.keys() == {"JobID"}

        # Search a specific parameter: Status
        total, result = await job_db.search(["Status"], [], [])
        assert total == 100
        assert result
        for r in result:
            assert r.keys() == {"Status"}

        # Search for multiple parameters: JobID, Status
        total, result = await job_db.search(["JobID", "Status"], [], [])
        assert total == 100
        assert result
        for r in result:
            assert r.keys() == {"JobID", "Status"}

        # Search for a specific parameter but use distinct: Status
        total, result = await job_db.search(["Status"], [], [], distinct=True)
        assert total == 1
        assert result

        # Search for a non-existent parameter: Dummy
        with pytest.raises(InvalidQueryError):
            total, result = await job_db.search(["Dummy"], [], [])


async def test_search_conditions(populated_job_db):
    """Test that we can search for specific jobs in the database."""
    async with populated_job_db as job_db:
        # Search a specific scalar condition: JobID eq 3
        condition = ScalarSearchSpec(
            parameter="JobID", operator=ScalarSearchOperator.EQUAL, value=3
        )
        total, result = await job_db.search([], [condition], [])
        assert total == 1
        assert result
        assert len(result) == 1
        assert result[0]["JobID"] == 3

        # Search a specific scalar condition: JobID lt 3
        condition = ScalarSearchSpec(
            parameter="JobID", operator=ScalarSearchOperator.LESS_THAN, value=3
        )
        total, result = await job_db.search([], [condition], [])
        assert total == 2
        assert result
        assert len(result) == 2
        assert result[0]["JobID"] == 1
        assert result[1]["JobID"] == 2

        # Search a specific scalar condition: JobID neq 3
        condition = ScalarSearchSpec(
            parameter="JobID", operator=ScalarSearchOperator.NOT_EQUAL, value=3
        )
        total, result = await job_db.search([], [condition], [])
        assert total == 99
        assert result
        assert len(result) == 99
        assert all(r["JobID"] != 3 for r in result)

        # Search a specific scalar condition: JobID eq 5873 (does not exist)
        condition = ScalarSearchSpec(
            parameter="JobID", operator=ScalarSearchOperator.EQUAL, value=5873
        )
        total, result = await job_db.search([], [condition], [])
        assert not result

        # Search a specific vector condition: JobID in 1,2,3
        condition = VectorSearchSpec(
            parameter="JobID", operator=VectorSearchOperator.IN, values=[1, 2, 3]
        )
        total, result = await job_db.search([], [condition], [])
        assert total == 3
        assert result
        assert len(result) == 3
        assert all(r["JobID"] in [1, 2, 3] for r in result)

        # Search a specific vector condition: JobID in 1,2,5873 (one of them does not exist)
        condition = VectorSearchSpec(
            parameter="JobID", operator=VectorSearchOperator.IN, values=[1, 2, 5873]
        )
        total, result = await job_db.search([], [condition], [])
        assert total == 2
        assert result
        assert len(result) == 2
        assert all(r["JobID"] in [1, 2] for r in result)

        # Search a specific vector condition: JobID not in 1,2,3
        condition = VectorSearchSpec(
            parameter="JobID", operator=VectorSearchOperator.NOT_IN, values=[1, 2, 3]
        )
        total, result = await job_db.search([], [condition], [])
        assert total == 97
        assert result
        assert len(result) == 97
        assert all(r["JobID"] not in [1, 2, 3] for r in result)

        # Search a specific vector condition: JobID not in 1,2,5873 (one of them does not exist)
        condition = VectorSearchSpec(
            parameter="JobID", operator=VectorSearchOperator.NOT_IN, values=[1, 2, 5873]
        )
        total, result = await job_db.search([], [condition], [])
        assert total == 98
        assert result
        assert len(result) == 98
        assert all(r["JobID"] not in [1, 2] for r in result)

        # Search for multiple conditions based on different parameters: JobID eq 70, JobID in 4,5,6
        condition1 = ScalarSearchSpec(
            parameter="Owner", operator=ScalarSearchOperator.EQUAL, value="owner4"
        )
        condition2 = VectorSearchSpec(
            parameter="JobID", operator=VectorSearchOperator.IN, values=[4, 5, 6]
        )
        total, result = await job_db.search([], [condition1, condition2], [])
        assert total == 1
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
        total, result = await job_db.search([], [condition1, condition2], [])
        assert total == 0
        assert not result


async def test_search_sorts(populated_job_db):
    """Test that we can search for jobs in the database and sort the results."""
    async with populated_job_db as job_db:
        # Search and sort by JobID in ascending order
        sort = SortSpec(parameter="JobID", direction=SortDirection.ASC)
        total, result = await job_db.search([], [], [sort])
        assert total == 100
        assert result
        for i, r in enumerate(result):
            assert r["JobID"] == i + 1

        # Search and sort by JobID in descending order
        sort = SortSpec(parameter="JobID", direction=SortDirection.DESC)
        total, result = await job_db.search([], [], [sort])
        assert total == 100
        assert result
        for i, r in enumerate(result):
            assert r["JobID"] == 100 - i

        # Search and sort by Owner in ascending order
        sort = SortSpec(parameter="Owner", direction=SortDirection.ASC)
        total, result = await job_db.search([], [], [sort])
        assert total == 100
        assert result
        # Assert that owner10 is before owner2 because of the lexicographical order
        assert result[2]["Owner"] == "owner10"
        assert result[12]["Owner"] == "owner2"

        # Search and sort by Owner in descending order
        sort = SortSpec(parameter="Owner", direction=SortDirection.DESC)
        total, result = await job_db.search([], [], [sort])
        assert total == 100
        assert result
        # Assert that owner10 is before owner2 because of the lexicographical order
        assert result[97]["Owner"] == "owner10"
        assert result[87]["Owner"] == "owner2"

        # Search and sort by OwnerGroup in ascending order and JobID in descending order
        sort1 = SortSpec(parameter="OwnerGroup", direction=SortDirection.ASC)
        sort2 = SortSpec(parameter="JobID", direction=SortDirection.DESC)
        total, result = await job_db.search([], [], [sort1, sort2])
        assert total == 100
        assert result
        assert result[0]["OwnerGroup"] == "owner_group1"
        assert result[0]["JobID"] == 50
        assert result[99]["OwnerGroup"] == "owner_group2"
        assert result[99]["JobID"] == 51


async def test_search_pagination(populated_job_db):
    """Test that we can search for jobs in the database."""
    async with populated_job_db as job_db:
        # Search for the first 10 jobs
        total, result = await job_db.search([], [], [], per_page=10, page=1)
        assert total == 100
        assert result
        assert len(result) == 10
        assert result[0]["JobID"] == 1

        # Search for the second 10 jobs
        total, result = await job_db.search([], [], [], per_page=10, page=2)
        assert total == 100
        assert result
        assert len(result) == 10
        assert result[0]["JobID"] == 11

        # Search for the last 10 jobs
        total, result = await job_db.search([], [], [], per_page=10, page=10)
        assert total == 100
        assert result
        assert len(result) == 10
        assert result[0]["JobID"] == 91

        # Search for the second 50 jobs
        total, result = await job_db.search([], [], [], per_page=50, page=2)
        assert total == 100
        assert result
        assert len(result) == 50
        assert result[0]["JobID"] == 51

        # Invalid page number
        total, result = await job_db.search([], [], [], per_page=10, page=11)
        assert total == 100
        assert not result

        # Invalid page number
        with pytest.raises(InvalidQueryError):
            result = await job_db.search([], [], [], per_page=10, page=0)

        # Invalid per_page number
        with pytest.raises(InvalidQueryError):
            result = await job_db.search([], [], [], per_page=0, page=1)


async def test_set_job_commands_invalid_job_id(job_db: JobDB):
    """Test that setting a command for a non-existent job raises JobNotFound."""
    async with job_db as job_db:
        with pytest.raises(IntegrityError):
            await job_db.set_job_commands([(123456, "test_command", "")])
