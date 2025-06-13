from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy import update

from diracx.core.models import (
    ScalarSearchOperator,
    ScalarSearchSpec,
    VectorSearchOperator,
    VectorSearchSpec,
)
from diracx.db.sql.pilots.db import PilotAgentsDB
from diracx.db.sql.pilots.schema import PilotAgents

MAIN_VO = "lhcb"
N = 100

# ------------ Fetching data ------------


async def get_pilots_by_stamp(
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

    return pilots


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


# ------------ Creating data ------------


@pytest.fixture
async def add_stamps(pilot_db):
    async def _add_stamps(start_n=0):
        async with pilot_db as db:
            # Add pilots
            refs = [f"ref_{i}" for i in range(start_n, start_n + N)]
            stamps = [f"stamp_{i}" for i in range(start_n, start_n + N)]
            pilot_references = dict(zip(stamps, refs))

            vo = MAIN_VO

            await db.add_pilots(
                stamps, vo, grid_type="DIRAC", pilot_references=pilot_references
            )

            return await get_pilots_by_stamp(db, stamps)

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
                stmt = stmt.values(Status="Aborted")

            res = await db.conn.execute(stmt)
            assert res.rowcount == len(pilot_stamps)

            pilots = await get_pilots_by_stamp(db, pilot_stamps)
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
        await get_pilots_by_stamp(pilot_db, [non_aborted_recent[0]["PilotStamp"]])
        await get_pilots_by_stamp(pilot_db, [aborted_recent[0]["PilotStamp"]])
        await get_pilots_by_stamp(pilot_db, [aborted_very_old[0]["PilotStamp"]])
        await get_pilots_by_stamp(pilot_db, [non_aborted_very_old[0]["PilotStamp"]])

    return non_aborted_recent, aborted_recent, non_aborted_very_old, aborted_very_old
