from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from diracx.core.exceptions import PilotAlreadyExistsError, PilotNotFoundError
from diracx.core.models.pilot import PilotMetadata, PilotStatus
from diracx.core.models.search import (
    ScalarSearchOperator,
    ScalarSearchSpec,
    SearchSpec,
)
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


async def delete_pilots(
    pilot_db: PilotAgentsDB,
    *,
    pilot_stamps: list[str] | None = None,
    age_in_days: int | None = None,
    delete_only_aborted: bool = True,
    vo_constraint: str | None = None,
):
    """Delete pilots by stamps or by age.

    Exactly one of `pilot_stamps` or `age_in_days` must be provided.

    The age-based branch is used by the maintenance task worker (not exposed
    on the public router). `vo_constraint` scopes an age-based deletion to
    a single VO; pass `None` for cross-VO cleanup.
    """
    if pilot_stamps is not None:
        pilots = await get_pilots_by_stamp(
            pilot_db=pilot_db,
            pilot_stamps=pilot_stamps,
            parameters=["PilotID"],
        )
        pilot_ids = [p["PilotID"] for p in pilots]
    elif age_in_days is not None:
        pilot_ids = await _list_pilots_for_age_cleanup(
            pilot_db=pilot_db,
            age_in_days=age_in_days,
            delete_only_aborted=delete_only_aborted,
            vo_constraint=vo_constraint,
        )
    else:
        raise ValueError("Exactly one of pilot_stamps or age_in_days must be provided.")

    if not pilot_ids:
        return

    await pilot_db.remove_jobs_from_pilots(pilot_ids)
    await pilot_db.delete_pilot_logs(pilot_ids)
    await pilot_db.delete_pilots(pilot_ids)


async def _list_pilots_for_age_cleanup(
    pilot_db: PilotAgentsDB,
    age_in_days: int,
    delete_only_aborted: bool,
    vo_constraint: str | None,
) -> list[int]:
    """Return pilot IDs older than `age_in_days`.

    Internal helper for age-based cleanup. The cutoff is compared server-side
    via the search layer; the datetime is serialised as an ISO-8601 string to
    avoid widening the search-spec type for this one caller.
    """
    cutoff = (datetime.now(tz=timezone.utc) - timedelta(days=age_in_days)).isoformat()

    search: list[SearchSpec] = [
        ScalarSearchSpec(
            parameter="SubmissionTime",
            operator=ScalarSearchOperator.LESS_THAN,
            value=cutoff,
        ),
    ]
    if vo_constraint is not None:
        search.append(
            ScalarSearchSpec(
                parameter="VO",
                operator=ScalarSearchOperator.EQUAL,
                value=vo_constraint,
            )
        )
    if delete_only_aborted:
        search.append(
            ScalarSearchSpec(
                parameter="Status",
                operator=ScalarSearchOperator.EQUAL,
                value=PilotStatus.ABORTED,
            )
        )

    _, pilots = await pilot_db.search_pilots(
        parameters=["PilotID"],
        search=search,
        sorts=[],
    )
    return [p["PilotID"] for p in pilots]


async def update_pilots_metadata(
    pilot_db: PilotAgentsDB,
    pilot_metadata: list[PilotMetadata],
):
    """Bulk-update pilot metadata."""
    await pilot_db.update_pilot_metadata(pilot_metadata)


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
