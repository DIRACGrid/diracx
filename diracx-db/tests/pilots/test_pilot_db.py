from __future__ import annotations

import pytest

from diracx.core.exceptions import (
    PilotAlreadyAssociatedWithJobError,
    PilotNotFoundError,
)
from diracx.core.models.pilot import PilotMetadata, PilotStatus
from diracx.core.models.search import (
    VectorSearchOperator,
    VectorSearchSpec,
)
from diracx.db.sql.pilots.db import PilotAgentsDB


@pytest.fixture
async def pilot_db():
    db = PilotAgentsDB("sqlite+aiosqlite:///:memory:")
    async with db.engine_context():
        async with db.engine.begin() as conn:
            await conn.run_sync(db.metadata.create_all)
        yield db


@pytest.fixture
async def populated_pilot_db(pilot_db):
    async with pilot_db as db:
        await db.register_pilots(
            pilot_stamps=[f"stamp_{i}" for i in range(1, 21)],
            vo="lhcb",
            grid_type="DIRAC",
            grid_site="Site.A",
            destination_site="Site.A",
        )
    yield pilot_db


# -------------------------------------------------------------------------
# register_pilots
# -------------------------------------------------------------------------


async def test_register_pilots_with_references(pilot_db):
    async with pilot_db as db:
        await db.register_pilots(
            pilot_stamps=["s1", "s2"],
            vo="lhcb",
            pilot_references={"s1": "ref-1"},
        )

    async with pilot_db as db:
        _, rows = await db.search_pilots(
            parameters=["PilotStamp", "PilotJobReference"],
            search=[],
            sorts=[],
        )
    by_stamp = {r["PilotStamp"]: r for r in rows}
    assert by_stamp["s1"]["PilotJobReference"] == "ref-1"
    assert by_stamp["s2"]["PilotJobReference"] == "s2"


# -------------------------------------------------------------------------
# assign_jobs_to_pilot
# -------------------------------------------------------------------------


async def _get_pilot_id(pilot_db, stamp: str) -> int:
    async with pilot_db as db:
        _, rows = await db.search_pilots(
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
        job_ids = await db.job_ids_for_stamps(["s1"])
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


# -------------------------------------------------------------------------
# update_pilot_metadata
# -------------------------------------------------------------------------


async def test_update_pilot_metadata_partial_fields(populated_pilot_db):
    async with populated_pilot_db as db:
        await db.update_pilot_metadata(
            {
                "stamp_1": PilotMetadata(BenchMark=42.0),
                "stamp_2": PilotMetadata(Status=PilotStatus.RUNNING),
            }
        )

    async with populated_pilot_db as db:
        _, rows = await db.search_pilots(
            parameters=["PilotStamp", "BenchMark", "Status"],
            search=[
                VectorSearchSpec(
                    parameter="PilotStamp",
                    operator=VectorSearchOperator.IN,
                    values=["stamp_1", "stamp_2"],
                )
            ],
            sorts=[],
        )
    by_stamp = {r["PilotStamp"]: r for r in rows}
    assert by_stamp["stamp_1"]["BenchMark"] == 42.0
    assert by_stamp["stamp_1"]["Status"] == PilotStatus.SUBMITTED
    assert by_stamp["stamp_2"]["Status"] == PilotStatus.RUNNING
    assert by_stamp["stamp_2"]["BenchMark"] == 0.0


async def test_update_pilot_metadata_unknown_stamp_raises(populated_pilot_db):
    with pytest.raises(PilotNotFoundError):
        async with populated_pilot_db as db:
            await db.update_pilot_metadata(
                {"nonexistent": PilotMetadata(Status=PilotStatus.DONE)}
            )


async def test_update_pilot_metadata_empty_mapping(populated_pilot_db):
    # Must not raise
    async with populated_pilot_db as db:
        await db.update_pilot_metadata({})


async def test_update_pilot_metadata_refreshes_last_update_time(populated_pilot_db):
    """Any metadata update also bumps LastUpdateTime."""
    async with populated_pilot_db as db:
        _, before = await db.search_pilots(
            parameters=["LastUpdateTime"],
            search=[
                VectorSearchSpec(
                    parameter="PilotStamp",
                    operator=VectorSearchOperator.IN,
                    values=["stamp_1"],
                )
            ],
            sorts=[],
        )

    async with populated_pilot_db as db:
        await db.update_pilot_metadata({"stamp_1": PilotMetadata(BenchMark=1.0)})

    async with populated_pilot_db as db:
        _, after = await db.search_pilots(
            parameters=["LastUpdateTime"],
            search=[
                VectorSearchSpec(
                    parameter="PilotStamp",
                    operator=VectorSearchOperator.IN,
                    values=["stamp_1"],
                )
            ],
            sorts=[],
        )
    assert after[0]["LastUpdateTime"] > before[0]["LastUpdateTime"]


# -------------------------------------------------------------------------
# job_ids_for_stamps / pilot_ids_for_job_ids
# -------------------------------------------------------------------------


async def test_job_ids_for_stamps(populated_pilot_db):
    pilot_id = await _get_pilot_id(populated_pilot_db, "stamp_1")

    async with populated_pilot_db as db:
        await db.assign_jobs_to_pilot(
            [{"PilotID": pilot_id, "JobID": 100, "StartTime": "2024-01-01T00:00:00Z"}]
        )

    async with populated_pilot_db as db:
        job_ids = await db.job_ids_for_stamps(["stamp_1"])
    assert job_ids == [100]


async def test_job_ids_for_stamps_empty(pilot_db):
    async with pilot_db as db:
        assert await db.job_ids_for_stamps([]) == []


async def test_pilot_ids_for_job_ids(populated_pilot_db):
    pilot_id = await _get_pilot_id(populated_pilot_db, "stamp_1")

    async with populated_pilot_db as db:
        await db.assign_jobs_to_pilot(
            [{"PilotID": pilot_id, "JobID": 200, "StartTime": "2024-01-01T00:00:00Z"}]
        )

    async with populated_pilot_db as db:
        pids = await db.pilot_ids_for_job_ids([200])
    assert pids == [pilot_id]


async def test_pilot_ids_for_job_ids_empty(pilot_db):
    async with pilot_db as db:
        assert await db.pilot_ids_for_job_ids([]) == []
