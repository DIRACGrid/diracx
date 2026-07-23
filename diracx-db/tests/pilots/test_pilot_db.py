from __future__ import annotations

import pytest

from diracx.core.exceptions import (
    PilotAlreadyAssociatedWithJobError,
    PilotNotFoundError,
)
from diracx.core.models.pilot import PilotMetadata, PilotStatus
from diracx.core.models.search import (
    ScalarSearchOperator,
    ScalarSearchSpec,
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


async def test_register_pilots(pilot_db):
    async with pilot_db as db:
        await db.register_pilots(
            pilot_stamps=["a", "b", "c"],
            vo="lhcb",
            grid_type="DIRAC",
            grid_site="Site.A",
            destination_site="Site.A",
        )

    async with pilot_db as db:
        _, rows = await db.search_pilots(
            parameters=["PilotStamp", "VO", "GridType"],
            search=[],
            sorts=[],
        )
    stamps = {r["PilotStamp"] for r in rows}
    assert stamps == {"a", "b", "c"}
    assert all(r["VO"] == "lhcb" for r in rows)


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


async def test_register_pilots_default_grid_type(pilot_db):
    async with pilot_db as db:
        await db.register_pilots(
            pilot_stamps=["s1"],
            vo="lhcb",
        )

    async with pilot_db as db:
        _, rows = await db.search_pilots(
            parameters=["GridType"],
            search=[],
            sorts=[],
        )
    assert rows[0]["GridType"] == "DIRAC"


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
# delete_pilots
# -------------------------------------------------------------------------


async def test_delete_pilots(pilot_db):
    async with pilot_db as db:
        await db.register_pilots(pilot_stamps=["s1", "s2"], vo="lhcb")

    pilot_id = await _get_pilot_id(pilot_db, "s1")

    async with pilot_db as db:
        await db.delete_pilots([pilot_id])

    async with pilot_db as db:
        _, rows = await db.search_pilots(
            parameters=["PilotStamp"],
            search=[],
            sorts=[],
        )
    stamps = {r["PilotStamp"] for r in rows}
    assert stamps == {"s2"}


# -------------------------------------------------------------------------
# remove_jobs_from_pilots
# -------------------------------------------------------------------------


async def test_remove_jobs_from_pilots(pilot_db):
    async with pilot_db as db:
        await db.register_pilots(pilot_stamps=["s1"], vo="lhcb")

    pilot_id = await _get_pilot_id(pilot_db, "s1")

    async with pilot_db as db:
        await db.assign_jobs_to_pilot(
            [{"PilotID": pilot_id, "JobID": 10, "StartTime": "2024-01-01T00:00:00Z"}]
        )

    async with pilot_db as db:
        await db.remove_jobs_from_pilots([pilot_id])

    async with pilot_db as db:
        job_ids = await db.job_ids_for_stamps(["s1"])
    assert job_ids == []


# -------------------------------------------------------------------------
# delete_pilot_logs / insert_pilot_output / get_pilot_output
# -------------------------------------------------------------------------


async def test_insert_and_get_pilot_output(pilot_db):
    async with pilot_db as db:
        await db.register_pilots(pilot_stamps=["s1"], vo="lhcb")

    pilot_id = await _get_pilot_id(pilot_db, "s1")

    async with pilot_db as db:
        await db.insert_pilot_output(pilot_id, "stdout", "stderr")

    async with pilot_db as db:
        output = await db.get_pilot_output(pilot_id)
    assert output == {"std_output": "stdout", "std_error": "stderr"}


async def test_get_pilot_output_returns_none(pilot_db):
    async with pilot_db as db:
        await db.register_pilots(pilot_stamps=["s1"], vo="lhcb")

    pilot_id = await _get_pilot_id(pilot_db, "s1")

    async with pilot_db as db:
        assert await db.get_pilot_output(pilot_id) is None


async def test_insert_pilot_output_upserts(pilot_db):
    async with pilot_db as db:
        await db.register_pilots(pilot_stamps=["s1"], vo="lhcb")

    pilot_id = await _get_pilot_id(pilot_db, "s1")

    async with pilot_db as db:
        await db.insert_pilot_output(pilot_id, "first", "first")
        await db.insert_pilot_output(pilot_id, "second", "second")

    async with pilot_db as db:
        output = await db.get_pilot_output(pilot_id)
    assert output == {"std_output": "second", "std_error": "second"}


async def test_delete_pilot_logs(pilot_db):
    async with pilot_db as db:
        await db.register_pilots(pilot_stamps=["s1"], vo="lhcb")

    pilot_id = await _get_pilot_id(pilot_db, "s1")

    async with pilot_db as db:
        await db.insert_pilot_output(pilot_id, "out", "err")

    async with pilot_db as db:
        await db.delete_pilot_logs([pilot_id])

    async with pilot_db as db:
        assert await db.get_pilot_output(pilot_id) is None


# -------------------------------------------------------------------------
# update_pilot_metadata
# -------------------------------------------------------------------------


async def test_update_pilot_metadata_partial_fields(populated_pilot_db):
    async with populated_pilot_db as db:
        await db.update_pilot_metadata(
            [
                PilotMetadata(PilotStamp="stamp_1", BenchMark=42.0),
                PilotMetadata(PilotStamp="stamp_2", Status=PilotStatus.RUNNING),
            ]
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
                [PilotMetadata(PilotStamp="nonexistent", Status=PilotStatus.DONE)]
            )


async def test_update_pilot_metadata_empty_list(populated_pilot_db):
    async with populated_pilot_db as db:
        await db.update_pilot_metadata([])


# -------------------------------------------------------------------------
# search_pilots
# -------------------------------------------------------------------------


async def test_search_pilots_all(populated_pilot_db):
    async with populated_pilot_db as db:
        total, rows = await db.search_pilots(
            parameters=["PilotStamp"],
            search=[],
            sorts=[],
        )
    assert total == 20
    assert len(rows) == 20


async def test_search_pilots_filter_by_status(populated_pilot_db):
    async with populated_pilot_db as db:
        await db.update_pilot_metadata(
            [PilotMetadata(PilotStamp="stamp_1", Status=PilotStatus.RUNNING)]
        )

    async with populated_pilot_db as db:
        _, rows = await db.search_pilots(
            parameters=["PilotStamp", "Status"],
            search=[
                ScalarSearchSpec(
                    parameter="Status",
                    operator=ScalarSearchOperator.EQUAL,
                    value=PilotStatus.RUNNING,
                )
            ],
            sorts=[],
        )
    assert len(rows) == 1
    assert rows[0]["PilotStamp"] == "stamp_1"


async def test_search_pilots_pagination(populated_pilot_db):
    async with populated_pilot_db as db:
        total, rows = await db.search_pilots(
            parameters=["PilotStamp"],
            search=[],
            sorts=[],
            per_page=5,
            page=1,
        )
    assert total == 20
    assert len(rows) == 5


async def test_search_pilots_empty_list(populated_pilot_db):
    async with populated_pilot_db as db:
        _, rows = await db.search_pilots(
            parameters=["PilotStamp"],
            search=[
                VectorSearchSpec(
                    parameter="PilotStamp",
                    operator=VectorSearchOperator.IN,
                    values=["nonexistent"],
                )
            ],
            sorts=[],
        )
    assert rows == []


# -------------------------------------------------------------------------
# pilot_summary
# -------------------------------------------------------------------------


async def test_pilot_summary_by_status(populated_pilot_db):
    async with populated_pilot_db as db:
        await db.update_pilot_metadata(
            [
                PilotMetadata(PilotStamp=f"stamp_{i}", Status=PilotStatus.RUNNING)
                for i in range(1, 11)
            ]
        )

    async with populated_pilot_db as db:
        result = await db.pilot_summary(
            group_by=["Status"],
            search=[],
        )
    counts = {r["Status"]: r["count"] for r in result}
    assert counts[PilotStatus.RUNNING] == 10
    assert counts[PilotStatus.SUBMITTED] == 10


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
