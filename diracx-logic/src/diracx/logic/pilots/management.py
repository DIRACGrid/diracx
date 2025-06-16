from __future__ import annotations

from datetime import datetime, timedelta, timezone

from diracx.core.exceptions import PilotAlreadyExistsError, PilotNotFoundError
from diracx.core.models import PilotFieldsMapping
from diracx.core.exceptions import PilotAlreadyExistsError, PilotNotFoundError
from diracx.core.models import (
    PilotCredentialsInfo,
    PilotFieldsMapping,
    PilotSecretConstraints,
)
from diracx.core.settings import AuthSettings
from diracx.db.sql import PilotAgentsDB

from .auth import create_raw_secrets, update_secrets_constraints
from .query import (
    get_outdated_pilots,
    get_pilot_ids_by_stamps,
    get_pilot_jobs_ids_by_pilot_id,
    get_pilots_by_stamp,
)


async def register_new_pilots(
    pilot_db: PilotAgentsDB,
    pilot_stamps: list[str],
    vo: str,
    grid_type: str,
    grid_site: str,
    destination_site: str,
    status_reason: str,
    pilot_job_references: dict[str, str] | None,
    settings: AuthSettings,
    generate_secrets: bool = True,
    pilot_secret_use_count_max: int | None = None,
) -> list[PilotCredentialsInfo] | None:
    # [IMPORTANT] Check unicity of pilot references
    # If a pilot already exists, we raise an error (transaction will rollback)
    existing_pilots = await get_pilots_by_stamp(
        pilot_db=pilot_db, pilot_stamps=pilot_stamps
    )

    # If we found pilots from the list, this means some pilots already exists
    if len(existing_pilots) > 0:
        found_keys = {pilot["PilotStamp"] for pilot in existing_pilots}

        raise PilotAlreadyExistsError(data={"pilot_stamps": str(found_keys)})

    await pilot_db.add_pilots(
        pilot_stamps=pilot_stamps,
        vo=vo,
        grid_type=grid_type,
        grid_site=grid_site,
        destination_site=destination_site,
        pilot_references=pilot_job_references,
        status_reason=status_reason,
    )

    if not generate_secrets:
        return None

    pilot_secrets, expiration_dates_timestamps = await create_raw_secrets(
        n=len(pilot_stamps),
        pilot_db=pilot_db,
        settings=settings,
        pilot_secret_use_count_max=pilot_secret_use_count_max,
        secret_constraint=PilotSecretConstraints(VOs=[vo]),
    )

    constraints = {
        pilot_secret: PilotSecretConstraints(PilotStamps=[pilot_stamp], VOs=[vo])
        for pilot_secret, pilot_stamp in zip(pilot_secrets, pilot_stamps)
    }

    await update_secrets_constraints(
        pilot_db=pilot_db, secrets_to_constraints_dict=constraints
    )

    return [
        PilotCredentialsInfo(
            pilot_stamp=pilot_stamp,
            pilot_secret=secret,
            pilot_secret_expires_in=expires_in,
        )
        for pilot_stamp, secret, expires_in in zip(
            pilot_stamps, pilot_secrets, expiration_dates_timestamps
        )
    ]


async def delete_pilots(
    pilot_db: PilotAgentsDB,
    pilot_stamps: list[str] | None = None,
    age_in_days: int | None = None,
    delete_only_aborted: bool = True,
):
    if pilot_stamps:
        pilot_ids = await get_pilot_ids_by_stamps(
            pilot_db=pilot_db, pilot_stamps=pilot_stamps, allow_missing=True
        )
    else:
        assert age_in_days

        cutoff_date = datetime.now(tz=timezone.utc) - timedelta(days=age_in_days)

        pilots = await get_outdated_pilots(
            pilot_db=pilot_db,
            cutoff_date=cutoff_date,
            only_aborted=delete_only_aborted,
            parameters=["PilotID"],
        )

        pilot_ids = [pilot["PilotID"] for pilot in pilots]

    await pilot_db.remove_jobs_from_pilots(pilot_ids)
    await pilot_db.delete_pilot_logs(pilot_ids)
    await pilot_db.delete_pilots(pilot_ids)


async def update_pilots_fields(
    pilot_db: PilotAgentsDB, pilot_stamps_to_fields_mapping: list[PilotFieldsMapping]
):
    await pilot_db.update_pilot_fields(pilot_stamps_to_fields_mapping)


async def add_jobs_to_pilot(
    pilot_db: PilotAgentsDB, pilot_stamp: str, job_ids: list[int]
):
    pilot_ids = await get_pilot_ids_by_stamps(
        pilot_db=pilot_db, pilot_stamps=[pilot_stamp]
    )
    pilot_id = pilot_ids[0]

    now = datetime.now(tz=timezone.utc)

    # Prepare the list of dictionaries for bulk insertion
    job_to_pilot_mapping = [
        {"PilotID": pilot_id, "JobID": job_id, "StartTime": now} for job_id in job_ids
    ]

    await pilot_db.add_jobs_to_pilot(
        job_to_pilot_mapping=job_to_pilot_mapping,
    )


async def get_pilot_jobs_ids_by_stamp(
    pilot_db: PilotAgentsDB, pilot_stamp: str
) -> list[int]:
    """Fetch pilot jobs by stamp."""
    try:
        pilot_ids = await get_pilot_ids_by_stamps(
            pilot_db=pilot_db, pilot_stamps=[pilot_stamp]
        )
        pilot_id = pilot_ids[0]
    except PilotNotFoundError:
        return []

    return await get_pilot_jobs_ids_by_pilot_id(pilot_db=pilot_db, pilot_id=pilot_id)
