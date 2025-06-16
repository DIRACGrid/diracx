from __future__ import annotations

from datetime import datetime, timezone

import pytest

from diracx.core.exceptions import (
    PilotAlreadyAssociatedWithJobError,
)
from diracx.core.models import (
    PilotFieldsMapping,
    PilotStatus,
)

from diracx.db.sql.pilots.db import PilotAgentsDB


from .utils import (
    add_stamps,  # noqa: F401
    create_old_pilots_environment,  # noqa: F401
    create_timed_pilots,  # noqa: F401
    get_pilot_jobs_ids_by_pilot_id,
    get_pilots_by_stamp,
)

MAIN_VO = "lhcb"
N = 100


@pytest.fixture
async def pilot_db(tmp_path):
    agents_db = PilotAgentsDB("sqlite+aiosqlite:///:memory:")
    async with agents_db.engine_context():
        async with agents_db.engine.begin() as conn:
            await conn.run_sync(agents_db.metadata.create_all)
        yield agents_db



@pytest.mark.asyncio
async def test_insert_and_select(pilot_db: PilotAgentsDB):
    async with pilot_db as pilot_db:
        # Add pilots
        refs = [f"ref_{i}" for i in range(10)]
        stamps = [f"stamp_{i}" for i in range(10)]
        pilot_references = dict(zip(stamps, refs))

        await pilot_db.add_pilots(
            stamps, MAIN_VO, grid_type="DIRAC", pilot_references=pilot_references
        )

        # Accept duplicates because it is checked by the logic
        await pilot_db.add_pilots(
            stamps, MAIN_VO, grid_type="DIRAC", pilot_references=None
        )


@pytest.mark.asyncio
async def test_insert_and_delete(pilot_db: PilotAgentsDB):
    async with pilot_db as pilot_db:
        # Add pilots
        refs = [f"ref_{i}" for i in range(2)]
        stamps = [f"stamp_{i}" for i in range(2)]
        pilot_references = dict(zip(stamps, refs))

        await pilot_db.add_pilots(
            stamps, MAIN_VO, grid_type="DIRAC", pilot_references=pilot_references
        )

        # Works, the pilots exists
        res = await get_pilots_by_stamp(pilot_db, [stamps[0]])
        await get_pilots_by_stamp(pilot_db, [stamps[0]])

        # We delete the first pilot
        await pilot_db.delete_pilots([res[0]["PilotID"]])

        # We get the 2nd pilot that is not delete (no error)
        await get_pilots_by_stamp(pilot_db, [stamps[1]])
        # We get the 1st pilot that is delete (error)

        assert not await get_pilots_by_stamp(pilot_db, [stamps[0]])


@pytest.mark.asyncio
async def test_insert_and_select_single_then_modify(pilot_db: PilotAgentsDB):
    async with pilot_db as pilot_db:
        pilot_stamp = "stamp-test"
        await pilot_db.add_pilots(
            vo=MAIN_VO,
            pilot_stamps=[pilot_stamp],
            grid_type="grid-type",
        )

        res = await get_pilots_by_stamp(pilot_db, [pilot_stamp])
        assert len(res) == 1
        pilot = res[0]

        # Assert values
        assert pilot["VO"] == MAIN_VO
        assert pilot["PilotStamp"] == pilot_stamp
        assert pilot["GridType"] == "grid-type"
        assert pilot["BenchMark"] == 0.0
        assert pilot["Status"] == PilotStatus.SUBMITTED
        assert pilot["StatusReason"] == "Unknown"
        assert not pilot["AccountingSent"]

        #
        # Modify a pilot, then check if every change is done
        #
        await pilot_db.update_pilot_fields(
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

        res = await get_pilots_by_stamp(pilot_db, [pilot_stamp])
        assert len(res) == 1
        pilot = res[0]

        # Set values
        assert pilot["VO"] == MAIN_VO
        assert pilot["PilotStamp"] == pilot_stamp
        assert pilot["GridType"] == "grid-type"
        assert pilot["BenchMark"] == 1.0
        assert pilot["Status"] == PilotStatus.WAITING
        assert pilot["StatusReason"] == "NewReason"
        assert pilot["AccountingSent"]


@pytest.mark.asyncio
async def test_associate_pilot_with_job_and_get_it(pilot_db: PilotAgentsDB):
    """We will proceed in few steps.

    1. Create a pilot
    2. Verify that he is not associated with any job
    3. Associate with jobs
    4. Verify that he is associate with this job
    5. Associate with jobs that he already has and two that he has not
    6. Associate with jobs that he has not, but were involved in a crash
    """
    async with pilot_db as pilot_db:
        pilot_stamp = "stamp-test"
        # Add pilot
        await pilot_db.add_pilots(
            vo=MAIN_VO,
            pilot_stamps=[pilot_stamp],
            grid_type="grid-type",
        )

        res = await get_pilots_by_stamp(pilot_db, [pilot_stamp])
        assert len(res) == 1
        pilot = res[0]
        pilot_id = pilot["PilotID"]

        # Verify that he has no jobs
        assert len(await get_pilot_jobs_ids_by_pilot_id(pilot_db, pilot_id)) == 0

        now = datetime.now(tz=timezone.utc)

        # Associate pilot with jobs
        pilot_jobs = [1, 2, 3]
        # Prepare the list of dictionaries for bulk insertion
        job_to_pilot_mapping = [
            {"PilotID": pilot_id, "JobID": job_id, "StartTime": now}
            for job_id in pilot_jobs
        ]
        await pilot_db.add_jobs_to_pilot(job_to_pilot_mapping)

        # Verify that he has all jobs
        db_jobs = await get_pilot_jobs_ids_by_pilot_id(pilot_db, pilot_id)
        # We test both length and if every job is included if for any reason we have duplicates
        assert all(job in db_jobs for job in pilot_jobs)
        assert len(pilot_jobs) == len(db_jobs)

        # Associate pilot with a job that he already has, and one that he has not
        pilot_jobs = [10, 1, 5]
        with pytest.raises(PilotAlreadyAssociatedWithJobError):
            # Prepare the list of dictionaries for bulk insertion
            job_to_pilot_mapping = [
                {"PilotID": pilot_id, "JobID": job_id, "StartTime": now}
                for job_id in pilot_jobs
            ]
            await pilot_db.add_jobs_to_pilot(job_to_pilot_mapping)

        # Associate pilot with jobs that he has not, but was previously in an error
        # To test that the rollback worked
        pilot_jobs = [5, 10]
        # Prepare the list of dictionaries for bulk insertion
        job_to_pilot_mapping = [
            {"PilotID": pilot_id, "JobID": job_id, "StartTime": now}
            for job_id in pilot_jobs
        ]
        await pilot_db.add_jobs_to_pilot(job_to_pilot_mapping)
