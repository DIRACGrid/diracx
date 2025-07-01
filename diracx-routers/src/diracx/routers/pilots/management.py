from __future__ import annotations

from http import HTTPStatus
from typing import Annotated

from fastapi import Body, HTTPException, Query, status

from diracx.core.exceptions import (
    PilotAlreadyAssociatedWithJobError,
    PilotAlreadyExistsError,
    PilotNotFoundError,
)
from diracx.core.models import (
    PilotFieldsMapping,
    PilotStatus,
)
from diracx.logic.pilots.management import (
    add_jobs_to_pilot as add_jobs_to_pilot_bl,
)
from diracx.logic.pilots.management import (
    delete_pilots as delete_pilots_bl,
)
from diracx.logic.pilots.management import (
    get_pilot_jobs_ids_by_stamp,
    register_new_pilots,
    update_pilots_fields,
)
from diracx.logic.pilots.query import get_pilot_ids_by_job_id

from ..dependencies import PilotAgentsDB
from ..fastapi_classes import DiracxRouter
from .access_policies import (
    ActionType,
    CheckPilotManagementPolicyCallable,
)

router = DiracxRouter()


@router.post("/")
async def add_pilot_stamps(
    pilot_db: PilotAgentsDB,
    pilot_stamps: Annotated[
        list[str],
        Body(description="List of the pilot stamps we want to add to the db."),
    ],
    vo: Annotated[
        str,
        Body(description="Virtual Organisation associated with the inserted pilots."),
    ],
    check_permissions: CheckPilotManagementPolicyCallable,
    grid_type: Annotated[str, Body(description="Grid type of the pilots.")] = "Dirac",
    grid_site: Annotated[str, Body(description="Pilots grid site.")] = "Unknown",
    destination_site: Annotated[
        str, Body(description="Pilots destination site.")
    ] = "NotAssigned",
    pilot_references: Annotated[
        dict[str, str] | None,
        Body(description="Association of a pilot reference with a pilot stamp."),
    ] = None,
    pilot_status: Annotated[
        PilotStatus, Body(description="Status of the pilots.")
    ] = PilotStatus.SUBMITTED,
):
    """Endpoint where a you can create pilots with their references.

    If a pilot stamp already exists, it will block the insertion.
    """
    # TODO: Verify that grid types, sites, destination sites, etc. are valids
    await check_permissions(action=ActionType.MANAGE_PILOTS)

    try:
        await register_new_pilots(
            pilot_db=pilot_db,
            pilot_stamps=pilot_stamps,
            vo=vo,
            grid_type=grid_type,
            grid_site=grid_site,
            destination_site=destination_site,
            pilot_job_references=pilot_references,
            status=pilot_status,
        )
    except PilotAlreadyExistsError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e)) from e


@router.delete("/", status_code=HTTPStatus.NO_CONTENT)
async def delete_pilots(
    pilot_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
    pilot_stamps: Annotated[
        list[str] | None, Query(description="Stamps of the pilots we want to delete.")
    ] = None,
    age_in_days: Annotated[
        int | None,
        Query(
            description=(
                "The number of days that define the maximum age of pilots to be deleted."
                "Pilots older than this age will be considered for deletion."
            )
        ),
    ] = None,
    delete_only_aborted: Annotated[
        bool,
        Query(
            description=(
                "Flag indicating whether to only delete pilots whose status is 'Aborted'."
                "If set to True, only pilots with the 'Aborted' status will be deleted."
                "It is set by default as True to avoid any mistake."
                "This flag is only used for deletion by time."
            )
        ),
    ] = False,
):
    """Endpoint to delete a pilot.

    Two features:

    1. Or you provide pilot_stamps, so you can delete pilots by their stamp
    2. Or you provide age_in_days, so you can delete pilots that lived more than age_in_days days.

    Note: If you delete a pilot, its logs and its associations with jobs WILL be deleted.
    """
    await check_permissions(
        action=ActionType.MANAGE_PILOTS,
    )

    if not pilot_stamps and not age_in_days:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="pilot_stamps or age_in_days have to be provided.",
        )

    await delete_pilots_bl(
        pilot_db=pilot_db,
        pilot_stamps=pilot_stamps,
        age_in_days=age_in_days,
        delete_only_aborted=delete_only_aborted,
    )


EXAMPLE_UPDATE_FIELDS = {
    "Update the BenchMark field": {
        "summary": "Update BenchMark",
        "description": "Update only the BenchMark for one pilot.",
        "value": {
            "pilot_stamps_to_fields_mapping": [
                {"PilotStamp": "the_pilot_stamp", "BenchMark": 1.0}
            ]
        },
    },
    "Update multiple statuses": {
        "summary": "Update multiple pilots",
        "description": "Update multiple pilots statuses.",
        "value": {
            "pilot_stamps_to_fields_mapping": [
                {"PilotStamp": "the_first_pilot_stamp", "Status": "Waiting"},
                {"PilotStamp": "the_second_pilot_stamp", "Status": "Waiting"},
            ]
        },
    },
}


@router.patch("/metadata", status_code=HTTPStatus.NO_CONTENT)
async def update_pilot_fields(
    pilot_stamps_to_fields_mapping: Annotated[
        list[PilotFieldsMapping],
        Body(
            description="(pilot_stamp, pilot_fields) mapping to change.",
            embed=True,
            openapi_examples=EXAMPLE_UPDATE_FIELDS,
        ),
    ],
    pilot_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
):
    """Modify a field of a pilot.

    Note: Only the fields in PilotFieldsMapping are mutable, except for the PilotStamp.
    """
    # Ensures stamps validity
    await check_permissions(action=ActionType.MANAGE_PILOTS)

    await update_pilots_fields(
        pilot_db=pilot_db,
        pilot_stamps_to_fields_mapping=pilot_stamps_to_fields_mapping,
    )


@router.get("/jobs")
async def get_pilot_jobs(
    pilot_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
    pilot_stamp: Annotated[
        str | None, Query(description="The stamp of the pilot.")
    ] = None,
    job_id: Annotated[int | None, Query(description="The ID of the job.")] = None,
) -> list[int]:
    """Endpoint only for DIRAC services, to get jobs of a pilot."""
    await check_permissions(action=ActionType.READ_PILOT_FIELDS)

    if pilot_stamp:
        return await get_pilot_jobs_ids_by_stamp(
            pilot_db=pilot_db,
            pilot_stamp=pilot_stamp,
        )
    elif job_id:
        return await get_pilot_ids_by_job_id(pilot_db=pilot_db, job_id=job_id)

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="You must provide either pilot_stamp or job_id",
    )


@router.patch("/jobs", status_code=HTTPStatus.NO_CONTENT)
async def add_jobs_to_pilot(
    pilot_db: PilotAgentsDB,
    pilot_stamp: Annotated[str, Body(description="The stamp of the pilot.")],
    job_ids: Annotated[
        list[int], Body(description="The jobs we want to add to the pilot.")
    ],
    check_permissions: CheckPilotManagementPolicyCallable,
):
    """Endpoint only for DIRAC services, to associate a pilot with a job."""
    await check_permissions(action=ActionType.MANAGE_PILOTS)

    try:
        await add_jobs_to_pilot_bl(
            pilot_db=pilot_db,
            pilot_stamp=pilot_stamp,
            job_ids=job_ids,
        )
    except PilotNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="This pilot does not exist."
        ) from e
    except PilotAlreadyAssociatedWithJobError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="This pilot is already associated with this job.",
        ) from e
