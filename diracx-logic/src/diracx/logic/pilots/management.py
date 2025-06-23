from __future__ import annotations

from datetime import datetime, timedelta, timezone

from diracx.core.exceptions import PilotAlreadyExistsError, PilotNotFoundError
from diracx.core.models import PilotFieldsMapping
from diracx.db.sql import PilotAgentsDB

from .query import (
    get_pilot_ids_by_stamps,
    get_pilot_jobs_ids_by_pilot_id,
    get_pilots_by_stamp_bulk,
)


async def register_new_pilots(
    pilot_db: PilotAgentsDB,
    pilot_stamps: list[str],
    vo: str,
    grid_type: str = "Dirac",
    pilot_job_references: dict[str, str] | None = None,
):
    # [IMPORTANT] Check unicity of pilot references
    # If a pilot already exists, it will undo everything and raise an error
    try:
        await get_pilots_by_stamp_bulk(pilot_db=pilot_db, pilot_stamps=pilot_stamps)
        raise PilotAlreadyExistsError(data={"pilot_stamps": str(pilot_stamps)})
    except PilotNotFoundError as e:
        # e.non_existing_pilots is set of the pilot that are not found
        # We can compare it with the pilot references that want to add
        # If both sets are the same, it means that every pilots is new, and so we can add them to the db
        # If not, it means that at least one is already in the db

        non_existing_pilots = e.non_existing_pilots
        pilots_that_already_exist = set(pilot_stamps) - non_existing_pilots

        if pilots_that_already_exist:
            raise PilotAlreadyExistsError(
                data={"pilot_stamps": str(pilots_that_already_exist)}
            ) from e

    await pilot_db.add_pilots_bulk(
        pilot_stamps=pilot_stamps,
        vo=vo,
        grid_type=grid_type,
        pilot_references=pilot_job_references,
    )


async def clear_pilots_bulk(
    pilot_db: PilotAgentsDB, age_in_days: int, delete_only_aborted: bool
):
    """Delete pilots that have been submitted before interval_in_days."""
    cutoff_date = datetime.now(tz=timezone.utc) - timedelta(days=age_in_days)

    await pilot_db.clear_pilots_bulk(
        cutoff_date=cutoff_date, delete_only_aborted=delete_only_aborted
    )


async def delete_pilots_by_stamps_bulk(
    pilot_db: PilotAgentsDB, pilot_stamps: list[str]
):
    await pilot_db.delete_pilots_by_stamps_bulk(pilot_stamps)


async def update_pilots_fields(
    pilot_db: PilotAgentsDB, pilot_stamps_to_fields_mapping: list[PilotFieldsMapping]
):
    await pilot_db.update_pilot_fields_bulk(pilot_stamps_to_fields_mapping)


async def add_jobs_to_pilot(
    pilot_db: PilotAgentsDB, pilot_stamp: str, pilot_jobs_ids: list[int]
):
    pilot_ids = await get_pilot_ids_by_stamps(
        pilot_db=pilot_db, pilot_stamps=[pilot_stamp]
    )
    pilot_id = pilot_ids[0]

    now = datetime.now(tz=timezone.utc)

    # Prepare the list of dictionaries for bulk insertion
    job_to_pilot_mapping = [
        {"PilotID": pilot_id, "JobID": job_id, "StartTime": now}
        for job_id in pilot_jobs_ids
    ]

    await pilot_db.add_jobs_to_pilot_bulk(
        job_to_pilot_mapping=job_to_pilot_mapping,
    )


async def get_pilot_jobs_ids_by_stamp(
    pilot_db: PilotAgentsDB, pilot_stamp: str
) -> list[int]:
    """Fetch pilot jobs by stamp."""
    pilot_ids = await get_pilot_ids_by_stamps(
        pilot_db=pilot_db, pilot_stamps=[pilot_stamp]
    )
    pilot_id = pilot_ids[0]

    return await get_pilot_jobs_ids_by_pilot_id(pilot_db=pilot_db, pilot_id=pilot_id)
