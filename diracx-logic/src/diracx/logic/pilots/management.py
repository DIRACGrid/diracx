from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from diracx.core.config import Config
from diracx.core.exceptions import PilotAlreadyExistsError, PilotNotFoundError
from diracx.core.models.pilot import PilotMetadata, PilotRegistrationParams
from diracx.db.sql import PilotAgentsDB

from .query import get_pilots_by_stamp


async def register_new_pilot(
    config: Config,
    pilot_db: PilotAgentsDB,
    registration: PilotRegistrationParams,
):
    """Register a new pilot.

    Raises `ValueError` if the VO is not in the registry, and
    `PilotAlreadyExistsError` if the stamp already exists.

    Uniqueness is best-effort: the DIRAC `PilotAgents` schema has no unique
    constraint on `PilotStamp` (only a non-unique key), so a concurrent
    registration of the same stamp from two processes could race past this
    check. In practice pilot stamps are cryptographically random UUIDs,
    making the collision window negligible.
    """
    # TODO: https://github.com/DIRACGrid/diracx/issues/1005
    # Also validate grid_type, grid_site and destination_site once the
    # Resources section of the CS is modeled in the Config schema.
    if registration.vo not in config.registry:
        raise ValueError(
            f"VO {registration.vo!r} is not registered in this installation."
        )

    stamp = registration.pilot_stamp
    if await get_pilots_by_stamp(pilot_db=pilot_db, pilot_stamps=[stamp]):
        raise PilotAlreadyExistsError(f"Pilot with stamp {stamp!r} already exists")

    await pilot_db.register_pilots(
        pilot_stamps=[stamp],
        vo=registration.vo,
        grid_type=registration.grid_type,
        grid_site=registration.grid_site,
        destination_site=registration.destination_site,
        pilot_references={stamp: registration.pilot_reference}
        if registration.pilot_reference
        else None,
        status=registration.pilot_status,
    )


async def update_pilots_metadata(
    pilot_db: PilotAgentsDB,
    updates: dict[str, PilotMetadata],
):
    """Bulk-update pilot metadata, keyed by pilot stamp.

    Unset fields (None) are preserved. `LastUpdateTime` is refreshed on
    every updated pilot.
    """
    fields_by_stamp = {
        stamp: metadata.model_dump(by_alias=True, exclude_none=True)
        for stamp, metadata in updates.items()
    }

    if not any(fields_by_stamp.values()):
        return

    now = datetime.now(tz=timezone.utc)
    for fields in fields_by_stamp.values():
        fields["LastUpdateTime"] = now

    await pilot_db.update_pilot_metadata(fields_by_stamp)


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
