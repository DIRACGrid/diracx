from __future__ import annotations

import asyncio

from pytest import mark

from diracx.db.jobs.db import JobDB


@mark.asyncio
async def test_some_asyncio_code(job_engine):
    async with JobDB() as job_db:
        start, end, total, result = await job_db.search(["JobID"], [], [])
        assert not result
        assert start == 0
        assert end == 0
        assert total == 0

        result = await asyncio.gather(
            *(
                job_db.insert(
                    f"JDL{i}",
                    "owner",
                    "owner_dn",
                    "owner_group",
                    "New",
                    "dfdfds",
                    "lhcb",
                )
                for i in range(100)
            )
        )

    async with JobDB() as job_db:
        result = await job_db.search(["JobID"], [], [])
        assert result
