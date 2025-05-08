from __future__ import annotations

import pytest

from diracx.core.exceptions import (
    PilotAlreadyAssociatedWithJobError,
    PilotNotFoundError,
)
from diracx.core.models import PilotFieldsMapping, PilotStatus
from diracx.db.sql.pilot_agents.db import PilotAgentsDB

MAIN_VO = "lhcb"
N = 100


@pytest.fixture
async def pilot_agents_db(tmp_path):
    agents_db = PilotAgentsDB("sqlite+aiosqlite:///:memory:")
    async with agents_db.engine_context():
        async with agents_db.engine.begin() as conn:
            await conn.run_sync(agents_db.metadata.create_all)
        yield agents_db


@pytest.fixture
async def add_stamps(pilot_agents_db):
    async with pilot_agents_db as pilot_agents_db:
        # Add pilots
        refs = [f"ref_{i}" for i in range(N)]
        stamps = [f"stamp_{i}" for i in range(N)]
        pilot_references = dict(zip(stamps, refs))

        vo = MAIN_VO

        await pilot_agents_db.add_pilots_bulk(
            stamps, vo, grid_type="DIRAC", pilot_references=pilot_references
        )

        pilots = await pilot_agents_db.get_pilots_by_stamp_bulk(stamps)

        return pilots


@pytest.mark.asyncio
async def test_insert_and_select(pilot_agents_db: PilotAgentsDB):

    async with pilot_agents_db as pilot_agents_db:
        # Add pilots
        refs = [f"ref_{i}" for i in range(10)]
        stamps = [f"stamp_{i}" for i in range(10)]
        pilot_references = dict(zip(stamps, refs))

        await pilot_agents_db.add_pilots_bulk(
            stamps, MAIN_VO, grid_type="DIRAC", pilot_references=pilot_references
        )

        await pilot_agents_db.add_pilots_bulk(
            stamps, MAIN_VO, grid_type="DIRAC", pilot_references=None
        )


@pytest.mark.asyncio
async def test_insert_and_select_single_then_modify(pilot_agents_db: PilotAgentsDB):

    async with pilot_agents_db as pilot_agents_db:
        pilot_stamp = "pilot-reference-test"
        await pilot_agents_db.add_pilots_bulk(
            vo=MAIN_VO,
            pilot_stamps=[pilot_stamp],
            grid_type="grid-type",
        )

        res = await pilot_agents_db.get_pilots_by_stamp_bulk([pilot_stamp])

        assert len(res) == 1

        pilot = res[0]

        with pytest.raises(PilotNotFoundError):
            await pilot_agents_db.get_pilots_by_stamp_bulk(["I am a fake stamp"])

        # Assert values
        assert pilot["VO"] == MAIN_VO
        assert pilot["PilotStamp"] == pilot_stamp
        assert pilot["GridType"] == "grid-type"
        assert pilot["BenchMark"] == 0.0
        assert pilot["Status"] == "Submitted"
        assert pilot["StatusReason"] == "Unknown"
        assert not pilot["AccountingSent"]

        #
        # Modify a pilot, then check if every change is done
        #
        await pilot_agents_db.update_pilot_fields_bulk(
            [
                PilotFieldsMapping(
                    PilotStamp=pilot_stamp,
                    BenchMark=1.0,
                    StatusReason="NewReason",
                    AccountingSent=True,
                    Status=PilotStatus.WAITING,
                )
            ]
        )

        res = await pilot_agents_db.get_pilots_by_stamp_bulk([pilot_stamp])

        assert len(res) == 1

        pilot = res[0]

        with pytest.raises(PilotNotFoundError):
            await pilot_agents_db.get_pilots_by_stamp_bulk(["I am a fake stamp"])

        # Set values
        assert pilot["VO"] == MAIN_VO
        assert pilot["PilotStamp"] == pilot_stamp
        assert pilot["GridType"] == "grid-type"
        assert pilot["BenchMark"] == 1.0
        assert pilot["Status"] == PilotStatus.WAITING
        assert pilot["StatusReason"] == "NewReason"
        assert pilot["AccountingSent"]


@pytest.mark.asyncio
async def test_associate_pilot_with_job_and_get_it(pilot_agents_db: PilotAgentsDB):
    """We will proceed in few steps.

    1. Create a pilot
    2. Verify that he is not associated with any job
    3. Associate with jobs
    4. Verify that he is associate with this job
    5. Associate with jobs that he already has and two that he has not
    6. Associate with jobs that he has not, but were involved in a crash
    """
    async with pilot_agents_db as pilot_agents_db:
        pilot_stamp = "pilot-reference-test"
        # Add pilot
        await pilot_agents_db.add_pilots_bulk(
            vo=MAIN_VO,
            pilot_stamps=[pilot_stamp],
            grid_type="grid-type",
        )

        res = await pilot_agents_db.get_pilots_by_stamp_bulk([pilot_stamp])

        assert len(res) == 1

        pilot = res[0]

        # Verify that he has no jobs
        assert (
            len(await pilot_agents_db.get_pilot_jobs_ids_by_pilot_id(pilot["PilotID"]))
            == 0
        )

        # Associate pilot with jobs
        pilot_jobs = [1, 2, 3]
        await pilot_agents_db.associate_pilot_with_jobs(pilot_stamp, pilot_jobs)

        # Verify that he has all jobs
        db_jobs = await pilot_agents_db.get_pilot_jobs_ids_by_pilot_id(pilot["PilotID"])
        # We test both length and if every job is included if for any reason we have duplicates
        assert all(job in db_jobs for job in pilot_jobs)
        assert len(pilot_jobs) == len(db_jobs)

        # Associate pilot with a job that he already has, and one that he has not
        pilot_jobs = [10, 1, 5]
        with pytest.raises(PilotAlreadyAssociatedWithJobError):
            await pilot_agents_db.associate_pilot_with_jobs(pilot_stamp, pilot_jobs)

        # Associate pilot with jobs that he has not, but was previously in an error
        # To test that the rollback worked
        pilot_jobs = [5, 10]
        await pilot_agents_db.associate_pilot_with_jobs(pilot_stamp, pilot_jobs)
