"""DB-level tests for PilotAgentsDB.

Only functionality that has no HTTP route (the job-to-pilot mapping) is
tested here; everything else is covered by the router-level tests in
diracx-routers/tests/pilots/.
"""

from __future__ import annotations

import pytest

from diracx.core.exceptions import (
    PilotAlreadyAssociatedWithJobError,
    PilotNotFoundError,
)
from diracx.core.models.search import (
    VectorSearchOperator,
    VectorSearchSpec,
)
from diracx.db.sql.pilot.db import PilotAgentsDB


@pytest.fixture
async def pilot_db():
    db = PilotAgentsDB("sqlite+aiosqlite:///:memory:")
    async with db.engine_context():
        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.create_all)
        yield db


async def _get_pilot_id(pilot_db, stamp: str) -> int:
    async with pilot_db as db:
        _, rows = await db.search(
            parameters=["PilotID"],
            search=[
                VectorSearchSpec(
                    parameter="PilotStamp",
                    operator=VectorSearchOperator.IN,
                    values=[stamp],
                )
            ],
            sorts=[],
        )
    return rows[0]["PilotID"]


async def test_assign_jobs_to_pilot(pilot_db):
    async with pilot_db as db:
        await db.register_pilots(pilot_stamps=["s1"], vo="lhcb")

    pilot_id = await _get_pilot_id(pilot_db, "s1")

    async with pilot_db as db:
        await db.assign_jobs_to_pilot(
            [
                {"PilotID": pilot_id, "JobID": 10, "StartTime": "2024-01-01T00:00:00Z"},
                {"PilotID": pilot_id, "JobID": 20, "StartTime": "2024-01-01T00:00:00Z"},
            ]
        )

    async with pilot_db as db:
        job_ids = await db.get_job_ids_for_stamps(["s1"])
    assert sorted(job_ids) == [10, 20]


async def test_assign_jobs_to_pilot_duplicate_raises(pilot_db):
    async with pilot_db as db:
        await db.register_pilots(pilot_stamps=["s1"], vo="lhcb")

    pilot_id = await _get_pilot_id(pilot_db, "s1")

    async with pilot_db as db:
        await db.assign_jobs_to_pilot(
            [{"PilotID": pilot_id, "JobID": 10, "StartTime": "2024-01-01T00:00:00Z"}]
        )

    with pytest.raises(PilotAlreadyAssociatedWithJobError):
        async with pilot_db as db:
            await db.assign_jobs_to_pilot(
                [
                    {
                        "PilotID": pilot_id,
                        "JobID": 10,
                        "StartTime": "2024-01-01T00:00:00Z",
                    }
                ]
            )


async def test_get_job_ids_for_stamps(pilot_db):
    async with pilot_db as db:
        await db.register_pilots(pilot_stamps=["s1", "s2"], vo="lhcb")

    pilot_id = await _get_pilot_id(pilot_db, "s1")

    async with pilot_db as db:
        await db.assign_jobs_to_pilot(
            [{"PilotID": pilot_id, "JobID": 100, "StartTime": "2024-01-01T00:00:00Z"}]
        )

    async with pilot_db as db:
        assert await db.get_job_ids_for_stamps(["s1"]) == [100]
        assert await db.get_job_ids_for_stamps(["s2"]) == []
        assert await db.get_job_ids_for_stamps([]) == []


async def test_get_pilot_ids_for_job_ids(pilot_db):
    async with pilot_db as db:
        await db.register_pilots(pilot_stamps=["s1"], vo="lhcb")

    pilot_id = await _get_pilot_id(pilot_db, "s1")

    async with pilot_db as db:
        await db.assign_jobs_to_pilot(
            [{"PilotID": pilot_id, "JobID": 200, "StartTime": "2024-01-01T00:00:00Z"}]
        )

    async with pilot_db as db:
        assert await db.get_pilot_ids_for_job_ids([200]) == [pilot_id]
        assert await db.get_pilot_ids_for_job_ids([999]) == []
        assert await db.get_pilot_ids_for_job_ids([]) == []


@pytest.fixture
def small_chunks(monkeypatch):
    """Split IN lists into chunks of two so a few rows cross chunk boundaries."""
    monkeypatch.setattr("diracx.db.sql.utils.chunks.IN_CHUNK_SIZE", 2)


async def test_update_pilot_metadata_across_chunks(pilot_db, small_chunks):
    stamps = [f"s{i}" for i in range(5)]
    async with pilot_db as db:
        await db.register_pilots(pilot_stamps=stamps, vo="lhcb")
        await db.update_pilot_metadata(
            {
                stamp: {"Queue": f"queue{i}"}
                if i % 2
                else {"Queue": f"queue{i}", "GridSite": f"site{i}"}
                for i, stamp in enumerate(stamps)
            }
        )
        _, rows = await db.search(
            parameters=["PilotStamp", "Queue", "GridSite"],
            search=[
                VectorSearchSpec(
                    parameter="PilotStamp",
                    operator=VectorSearchOperator.IN,
                    values=stamps,
                )
            ],
            sorts=[],
        )
    assert sorted(rows, key=lambda row: row["PilotStamp"]) == [
        {
            "PilotStamp": stamp,
            "Queue": f"queue{i}",
            "GridSite": "Unknown" if i % 2 else f"site{i}",
        }
        for i, stamp in enumerate(stamps)
    ]


async def test_update_pilot_metadata_missing_stamp_across_chunks(
    pilot_db, small_chunks
):
    # The row counts of every chunk are summed before being compared
    async with pilot_db as db:
        await db.register_pilots(pilot_stamps=["s0", "s1", "s2"], vo="lhcb")
    with pytest.raises(PilotNotFoundError):
        async with pilot_db as db:
            await db.update_pilot_metadata(
                {stamp: {"Queue": "q"} for stamp in ["s0", "s1", "s2", "s3"]}
            )
