from __future__ import annotations

import asyncio

import pytest

from diracx.db.jobs.db import JobDB


@pytest.fixture
async def job_db(tmp_path):
    job_db = JobDB("sqlite+aiosqlite:///:memory:")
    async with job_db.engine_context():
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
