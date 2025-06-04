from __future__ import annotations

from diracx.core.exceptions import PilotCantAccessJobError
from diracx.db.sql import PilotAgentsDB


async def get_pilot_jobs_ids_by_stamp(
    pilot_db: PilotAgentsDB, pilot_stamp: str
) -> list[int]:
    """Fetch pilot jobs by stamp."""
    pilot_ids = await pilot_db.get_pilot_ids_by_stamps([pilot_stamp])
    # Semantic assured by fetch_records_bulk_or_raises
    pilot_id = pilot_ids[0]

    return await pilot_db.get_pilot_jobs_ids_by_pilot_id(pilot_id)


async def verify_that_pilot_can_access_jobs(
    pilot_db: PilotAgentsDB, pilot_stamp: str, job_ids: list[int]
):
    # Get its jobs
    pilot_jobs = await get_pilot_jobs_ids_by_stamp(
        pilot_db=pilot_db, pilot_stamp=pilot_stamp
    )

    # Equivalent of issubset, but cleaner
    if set(job_ids) <= set(pilot_jobs):
        return

    forbidden_jobs_ids = set(job_ids) - set(pilot_jobs)

    if forbidden_jobs_ids:
        return PilotCantAccessJobError(
            data={"forbidden_jobs_ids": str(forbidden_jobs_ids)}
        )
