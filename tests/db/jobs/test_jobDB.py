from __future__ import annotations

import asyncio

import pytest

from diracx.core.exceptions import JobNotFound
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


async def test_some_asyncio_code(job_db):
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
        result = await job_db.search(["JobID"], [], [])
        assert result


async def test_set_job_command_invalid_job_id(job_db: JobDB):
    async with job_db as job_db:
        with pytest.raises(JobNotFound):
            await job_db.set_job_command(123456, "test_command")
