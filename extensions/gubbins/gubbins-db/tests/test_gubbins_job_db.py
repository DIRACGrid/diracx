from __future__ import annotations

from typing import AsyncGenerator

import pytest
from sqlalchemy import event

from gubbins.db.sql import GubbinsJobDB


@pytest.fixture
async def gubbins_db() -> AsyncGenerator[GubbinsJobDB, None]:
    gubbins_db = GubbinsJobDB("sqlite+aiosqlite:///:memory:")
    async with gubbins_db.engine_context():
        async with gubbins_db.engine.begin() as conn:
            # set PRAGMA foreign_keys=ON if sqlite
            if gubbins_db._db_url.startswith("sqlite"):
                await conn.exec_driver_sql("PRAGMA foreign_keys=ON")
            await conn.run_sync(gubbins_db.metadata.create_all)
        yield gubbins_db


@pytest.fixture
async def populated_job_db(job_db):
    """Populate the in-memory JobDB with 100 jobs using DAL calls."""

    yield job_db


async def test_gubbins_info(gubbins_db):
    """
    This test makes sure that we can:
    * use a method from the parent db (insert)
    * use a method from a child db (insert_gubbins_info)
    * use a method modified in the child db (getJobJDL)
    """
    async with gubbins_db as gubbins_db:
        compressed_jdl = "CompressedJDL"
        job_id = await gubbins_db.create_job(compressed_jdl)
        job_attr = {
            "JobID": job_id,
            "Status": "New",
            "MinorStatus": "dfdfds",
            "Owner": "owner_toto",
            "OwnerGroup": "owner_group1",
            "VO": "lhcb",
        }
        await gubbins_db.insert_job_attributes({job_id: job_attr})

        await gubbins_db.insert_gubbins_info(job_id, "info")

        result = await gubbins_db.get_job_jdls([job_id], original=True)
        assert result == {1: "CompressedJDL"}

        result = await gubbins_db.get_job_jdls([job_id], original=True, with_info=True)
        assert len(result) == 1
        assert result[1].get("JDL")
        assert result[1].get("Info") == "info"


async def test_gubbins_info_across_chunks(gubbins_db, monkeypatch):
    """Long lists of job IDs are looked up in chunks, as in the parent class."""
    monkeypatch.setattr("diracx.db.sql.utils.chunks.IN_CHUNK_SIZE", 2)
    info_queries: list[str] = []

    @event.listens_for(gubbins_db.engine.sync_engine, "before_cursor_execute")
    def record(conn, cursor, statement, parameters, context, executemany):
        if 'FROM "GubbinsInfo"' in statement:
            info_queries.append(statement)

    async with gubbins_db as db:
        job_ids = []
        for i in range(5):
            job_id = await db.create_job(f"JDL{i}")
            await db.insert_job_attributes(
                {
                    job_id: {
                        "JobID": job_id,
                        "Status": "New",
                        "Owner": "owner",
                        "OwnerGroup": "owner_group1",
                        "VO": "lhcb",
                    }
                }
            )
            await db.insert_gubbins_info(job_id, f"info{i}")
            job_ids.append(job_id)

        result = await db.get_job_jdls(job_ids, original=True, with_info=True)

    assert result == {
        job_id: {"JDL": f"JDL{i}", "Info": f"info{i}"}
        for i, job_id in enumerate(job_ids)
    }
    # Five jobs in chunks of two
    assert len(info_queries) == 3
