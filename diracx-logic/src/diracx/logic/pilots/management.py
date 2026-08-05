from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from diracx.core.exceptions import PilotAlreadyExistsError, PilotNotFoundError
from diracx.core.models.pilot import PilotMetadata
from diracx.db.sql import PilotAgentsDB

from .query import get_pilots_by_stamp


async def register_new_pilots(
    pilot_db: PilotAgentsDB,
    pilot_stamps: list[str],
    vo: str,
    grid_type: str,
    grid_site: str,
    destination_site: str,
    status: str,
    pilot_job_references: dict[str, str] | None,
):
    """Register a batch of new pilots.

    Raises `PilotAlreadyExistsError` if any stamp already exists.

    Uniqueness is best-effort: the DIRAC `PilotAgents` schema has no unique
    constraint on `PilotStamp` (only a non-unique key), so a concurrent
    registration of the same stamp from two processes could race past this
    check. In practice pilot stamps are cryptographically random UUIDs,
    making the collision window negligible.
    """
    existing_pilots = await get_pilots_by_stamp(
        pilot_db=pilot_db, pilot_stamps=pilot_stamps
    )

    if existing_pilots:
        found_keys = {pilot["PilotStamp"] for pilot in existing_pilots}
        raise PilotAlreadyExistsError(
            f"The following pilots already exist: {found_keys}"
        )

    await pilot_db.register_pilots(
        pilot_stamps=pilot_stamps,
        vo=vo,
        grid_type=grid_type,
        grid_site=grid_site,
        destination_site=destination_site,
        pilot_references=pilot_job_references,
        status=status,
    )


async def update_pilots_metadata(
    pilot_db: PilotAgentsDB,
    updates: dict[str, PilotMetadata],
):
    """Bulk-update pilot metadata, keyed by pilot stamp."""
    await pilot_db.update_pilot_metadata(updates)


async def assign_jobs_to_pilot(
    pilot_db: PilotAgentsDB, pilot_stamp: str, job_ids: list[int]
):
    """Associate jobs with a pilot identified by its stamp."""
    pilots = await get_pilots_by_stamp(
        pilot_db=pilot_db,
        pilot_stamps=[pilot_stamp],
        parameters=["PilotID"],
    )
    if not pilots:
        raise PilotNotFoundError(detail=f"pilot {pilot_stamp!r} does not exist")
    pilot_id = pilots[0]["PilotID"]

    job_to_pilot_mapping: list[dict[str, Any]] = [
        {
            "PilotID": pilot_id,
            "JobID": job_id,
            "StartTime": datetime.now(tz=timezone.utc),
        }
        for job_id in job_ids
    ]

    await pilot_db.assign_jobs_to_pilot(job_to_pilot_mapping=job_to_pilot_mapping)
