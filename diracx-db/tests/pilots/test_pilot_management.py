from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy.sql import update

from diracx.core.exceptions import (
    PilotAlreadyAssociatedWithJobError,
    PilotNotFoundError,
)
from diracx.core.models import (
    PilotFieldsMapping,
    PilotStatus,
    ScalarSearchOperator,
    ScalarSearchSpec,
    VectorSearchOperator,
    VectorSearchSpec,
)
from diracx.db.sql.pilots.db import PilotAgentsDB
from diracx.db.sql.pilots.schema import PilotAgents

MAIN_VO = "lhcb"
N = 100


@pytest.fixture
async def pilot_db(tmp_path):
    agents_db = PilotAgentsDB("sqlite+aiosqlite:///:memory:")
    async with agents_db.engine_context():
        async with agents_db.engine.begin() as conn:
            await conn.run_sync(agents_db.metadata.create_all)
        yield agents_db


async def get_pilot_jobs_ids_by_pilot_id(
    pilot_db: PilotAgentsDB, pilot_id: int
) -> list[int]:
    _, jobs = await pilot_db.search_pilot_to_job_mapping(
        parameters=["JobID"],
        search=[
            ScalarSearchSpec(
                parameter="PilotID",
                operator=ScalarSearchOperator.EQUAL,
                value=pilot_id,
            )
        ],
        sorts=[],
        distinct=True,
        per_page=10000,
    )

    return [job["JobID"] for job in jobs]


async def get_pilots_by_stamp_bulk(
    pilot_db: PilotAgentsDB, pilot_stamps: list[str], parameters: list[str] = []
) -> list[dict[Any, Any]]:
    _, pilots = await pilot_db.search_pilots(
        parameters=parameters,
        search=[
            VectorSearchSpec(
                parameter="PilotStamp",
                operator=VectorSearchOperator.IN,
                values=pilot_stamps,
            )
        ],
        sorts=[],
        distinct=True,
        per_page=1000,
    )

    # Custom handling, to see which pilot_stamp does not exist (if so, say which one)
    found_keys = {row["PilotStamp"] for row in pilots}
    missing = set(pilot_stamps) - found_keys

    if missing:
        raise PilotNotFoundError(
            data={"pilot_stamp": str(missing)},
            detail=str(missing),
            non_existing_pilots=missing,
        )

    return pilots


@pytest.fixture
async def add_stamps(pilot_db):
    async def _add_stamps(start_n=0):
        async with pilot_db as db:
            # Add pilots
            refs = [f"ref_{i}" for i in range(start_n, start_n + N)]
            stamps = [f"stamp_{i}" for i in range(start_n, start_n + N)]
            pilot_references = dict(zip(stamps, refs))

            vo = MAIN_VO

            await db.add_pilots_bulk(
                stamps, vo, grid_type="DIRAC", pilot_references=pilot_references
            )

            pilots = await get_pilots_by_stamp_bulk(db, stamps)

            return pilots

    return _add_stamps


@pytest.fixture
async def create_timed_pilots(pilot_db, add_stamps):
    async def _create_timed_pilots(
        old_date: datetime, aborted: bool = False, start_n=0
    ):
        # Get pilots
        pilots = await add_stamps(start_n)

        async with pilot_db as db:
            # Update manually their age
            # Collect PilotStamps
            pilot_stamps = [pilot["PilotStamp"] for pilot in pilots]

            stmt = (
                update(PilotAgents)
                .where(PilotAgents.pilot_stamp.in_(pilot_stamps))
                .values(SubmissionTime=old_date)
            )

            if aborted:
                stmt = stmt.values(Status=PilotStatus.ABORTED)

            res = await db.conn.execute(stmt)
            assert res.rowcount == len(pilot_stamps)

            pilots = await get_pilots_by_stamp_bulk(db, pilot_stamps)
            return pilots

    return _create_timed_pilots


@pytest.fixture
async def create_old_pilots_environment(pilot_db, create_timed_pilots):
    non_aborted_recent = await create_timed_pilots(
        datetime(2025, 1, 1, tzinfo=timezone.utc), False, N
    )
    aborted_recent = await create_timed_pilots(
        datetime(2025, 1, 1, tzinfo=timezone.utc), True, 2 * N
    )

    aborted_very_old = await create_timed_pilots(
        datetime(2003, 3, 10, tzinfo=timezone.utc), True, 3 * N
    )
    non_aborted_very_old = await create_timed_pilots(
        datetime(2003, 3, 10, tzinfo=timezone.utc), False, 4 * N
    )

    pilot_number = 4 * N

    assert pilot_number == (
        len(non_aborted_recent)
        + len(aborted_recent)
        + len(aborted_very_old)
        + len(non_aborted_very_old)
    )

    # Phase 0. Verify that we have the right environment
    async with pilot_db as pilot_db:
        # Ensure that we can get every pilot (only get first of each group)
        await get_pilots_by_stamp_bulk(pilot_db, [non_aborted_recent[0]["PilotStamp"]])
        await get_pilots_by_stamp_bulk(pilot_db, [aborted_recent[0]["PilotStamp"]])
        await get_pilots_by_stamp_bulk(pilot_db, [aborted_very_old[0]["PilotStamp"]])
        await get_pilots_by_stamp_bulk(
            pilot_db, [non_aborted_very_old[0]["PilotStamp"]]
        )

    return non_aborted_recent, aborted_recent, non_aborted_very_old, aborted_very_old


@pytest.mark.asyncio
async def test_insert_and_select(pilot_db: PilotAgentsDB):
    async with pilot_db as pilot_db:
        # Add pilots
        refs = [f"ref_{i}" for i in range(10)]
        stamps = [f"stamp_{i}" for i in range(10)]
        pilot_references = dict(zip(stamps, refs))

        await pilot_db.add_pilots_bulk(
            stamps, MAIN_VO, grid_type="DIRAC", pilot_references=pilot_references
        )

        # Accept duplicates because it is checked by the logic
        await pilot_db.add_pilots_bulk(
            stamps, MAIN_VO, grid_type="DIRAC", pilot_references=None
        )


@pytest.mark.asyncio
async def test_insert_and_delete(pilot_db: PilotAgentsDB):
    async with pilot_db as pilot_db:
        # Add pilots
        refs = [f"ref_{i}" for i in range(2)]
        stamps = [f"stamp_{i}" for i in range(2)]
        pilot_references = dict(zip(stamps, refs))

        await pilot_db.add_pilots_bulk(
            stamps, MAIN_VO, grid_type="DIRAC", pilot_references=pilot_references
        )

        # Works, the pilots exists
        await get_pilots_by_stamp_bulk(pilot_db, [stamps[0]])
        await get_pilots_by_stamp_bulk(pilot_db, [stamps[0]])

        # We delete the first pilot
        await pilot_db.delete_pilots_by_stamps_bulk([stamps[0]])

        # We get the 2nd pilot that is not delete (no error)
        await get_pilots_by_stamp_bulk(pilot_db, [stamps[1]])
        # We get the 1st pilot that is delete (error)
        with pytest.raises(PilotNotFoundError):
            await get_pilots_by_stamp_bulk(pilot_db, [stamps[0]])


@pytest.mark.asyncio
async def test_insert_and_delete_only_old_aborted(
    pilot_db: PilotAgentsDB, create_old_pilots_environment
):
    non_aborted_recent, aborted_recent, non_aborted_very_old, aborted_very_old = (
        create_old_pilots_environment
    )

    async with pilot_db as pilot_db:
        # Delete all aborted that were born before 2020
        # Every aborted that are old may be delete
        await pilot_db.clear_pilots_bulk(
            datetime(2020, 1, 1, tzinfo=timezone.utc), True
        )

        # Assert who still live
        for normally_exiting_pilot_list in [
            non_aborted_recent,
            aborted_recent,
            non_aborted_very_old,
        ]:
            stamps = [pilot["PilotStamp"] for pilot in normally_exiting_pilot_list]

            await get_pilots_by_stamp_bulk(pilot_db, stamps)

        # Assert who normally does not live
        for normally_deleted_pilot_list in [aborted_very_old]:
            stamps = [pilot["PilotStamp"] for pilot in normally_deleted_pilot_list]

            with pytest.raises(PilotNotFoundError):
                await get_pilots_by_stamp_bulk(pilot_db, stamps)


@pytest.mark.asyncio
async def test_insert_and_delete_old(
    pilot_db: PilotAgentsDB, create_old_pilots_environment
):
    non_aborted_recent, aborted_recent, non_aborted_very_old, aborted_very_old = (
        create_old_pilots_environment
    )

    async with pilot_db as pilot_db:
        # Delete all aborted that were born before 2020
        # Every aborted that are old may be delete
        await pilot_db.clear_pilots_bulk(
            datetime(2020, 1, 1, tzinfo=timezone.utc), False
        )

        # Assert who still live
        for normally_exiting_pilot_list in [
            non_aborted_recent,
            aborted_recent,
        ]:
            stamps = [pilot["PilotStamp"] for pilot in normally_exiting_pilot_list]

            await get_pilots_by_stamp_bulk(pilot_db, stamps)

        # Assert who normally does not live
        for normally_deleted_pilot_list in [
            aborted_very_old,
            non_aborted_very_old,
        ]:
            stamps = [pilot["PilotStamp"] for pilot in normally_deleted_pilot_list]

            with pytest.raises(PilotNotFoundError):
                await get_pilots_by_stamp_bulk(pilot_db, stamps)


@pytest.mark.asyncio
async def test_insert_and_delete_recent_only_aborted(
    pilot_db: PilotAgentsDB, create_old_pilots_environment
):
    non_aborted_recent, aborted_recent, non_aborted_very_old, aborted_very_old = (
        create_old_pilots_environment
    )

    async with pilot_db as pilot_db:
        # Delete all aborted that were born before 2020
        # Every aborted that are old may be delete
        await pilot_db.clear_pilots_bulk(
            datetime(2025, 3, 10, tzinfo=timezone.utc), True
        )

        # Assert who still live
        for normally_exiting_pilot_list in [non_aborted_recent, non_aborted_very_old]:
            stamps = [pilot["PilotStamp"] for pilot in normally_exiting_pilot_list]

            await get_pilots_by_stamp_bulk(pilot_db, stamps)

        # Assert who normally does not live
        for normally_deleted_pilot_list in [
            aborted_very_old,
            aborted_recent,
        ]:
            stamps = [pilot["PilotStamp"] for pilot in normally_deleted_pilot_list]

            with pytest.raises(PilotNotFoundError):
                await get_pilots_by_stamp_bulk(pilot_db, stamps)


@pytest.mark.asyncio
async def test_insert_and_delete_recent(
    pilot_db: PilotAgentsDB, create_old_pilots_environment
):
    non_aborted_recent, aborted_recent, non_aborted_very_old, aborted_very_old = (
        create_old_pilots_environment
    )

    async with pilot_db as pilot_db:
        # Delete all aborted that were born before 2020
        # Every aborted that are old may be delete
        await pilot_db.clear_pilots_bulk(
            datetime(2025, 3, 10, tzinfo=timezone.utc), False
        )

        # Assert who normally does not live
        for normally_deleted_pilot_list in [
            aborted_very_old,
            aborted_recent,
            non_aborted_recent,
            non_aborted_very_old,
        ]:
            stamps = [pilot["PilotStamp"] for pilot in normally_deleted_pilot_list]

            with pytest.raises(PilotNotFoundError):
                await get_pilots_by_stamp_bulk(pilot_db, stamps)


@pytest.mark.asyncio
async def test_insert_and_select_single_then_modify(pilot_db: PilotAgentsDB):
    async with pilot_db as pilot_db:
        pilot_stamp = "stamp-test"
        await pilot_db.add_pilots_bulk(
            vo=MAIN_VO,
            pilot_stamps=[pilot_stamp],
            grid_type="grid-type",
        )

        res = await get_pilots_by_stamp_bulk(pilot_db, [pilot_stamp])
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
        await pilot_db.update_pilot_fields_bulk(
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

        res = await get_pilots_by_stamp_bulk(pilot_db, [pilot_stamp])
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
        await pilot_db.add_pilots_bulk(
            vo=MAIN_VO,
            pilot_stamps=[pilot_stamp],
            grid_type="grid-type",
        )

        res = await get_pilots_by_stamp_bulk(pilot_db, [pilot_stamp])
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
        await pilot_db.add_jobs_to_pilot_bulk(job_to_pilot_mapping)

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
            await pilot_db.add_jobs_to_pilot_bulk(job_to_pilot_mapping)

        # Associate pilot with jobs that he has not, but was previously in an error
        # To test that the rollback worked
        pilot_jobs = [5, 10]
        # Prepare the list of dictionaries for bulk insertion
        job_to_pilot_mapping = [
            {"PilotID": pilot_id, "JobID": job_id, "StartTime": now}
            for job_id in pilot_jobs
        ]
        await pilot_db.add_jobs_to_pilot_bulk(job_to_pilot_mapping)
