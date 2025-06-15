from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Annotated

from fastapi import Body, Depends, HTTPException, Query, status

from diracx.core.exceptions import (
    PilotAlreadyAssociatedWithJobError,
    PilotAlreadyExistsError,
    PilotNotFoundError,
)
from diracx.core.models import (
    PilotFieldsMapping,
)
from diracx.logic.pilots.management import (
    associate_pilot_with_jobs as associate_pilot_with_jobs_bl,
)
from diracx.logic.pilots.management import (
    clear_pilots_bulk,
    delete_pilots_by_stamps_bulk,
    register_new_pilots,
    update_pilots_fields,
)

from ..dependencies import PilotAgentsDB
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import (
    ActionType,
    CheckDiracServicesPolicyCallable,
    CheckPilotManagementPolicyCallable,
)

router = DiracxRouter()

logger = logging.getLogger(__name__)


@router.post("/management/pilot")
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
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckPilotManagementPolicyCallable,
    grid_type: Annotated[str, Body(description="Grid type of the pilots.")] = "Dirac",
    pilot_references: Annotated[
        dict[str, str] | None,
        Body(description="Association of a pilot reference with a pilot stamp."),
    ] = None,
):
    """Endpoint where a you can create pilots with their references.

    If a pilot stamp already exists, it will block the insertion.
    """
    await check_permissions(action=ActionType.CREATE_PILOT, vo=vo)

    try:
        await register_new_pilots(
            pilot_db=pilot_db,
            pilot_stamps=pilot_stamps,
            vo=vo,
            grid_type=grid_type,
            pilot_job_references=pilot_references,
        )

        # Logs credentials creation
        logger.debug(
            f"{user_info.preferred_username} added {len(pilot_stamps)} pilots."
        )
    except PilotAlreadyExistsError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e)) from e


@router.delete("/management/pilot", status_code=HTTPStatus.NO_CONTENT)
async def delete_pilots(
    pilot_stamps: Annotated[
        list[str], Query(description="Stamps of the pilots we want to delete.")
    ],
    pilot_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
):
    """Endpoint to delete a pilot.

    If at least one pilot is not found, it WILL rollback.
    """
    await check_permissions(
        action=ActionType.CHANGE_PILOT_FIELD,
        pilot_stamps=pilot_stamps,
        pilot_db=pilot_db,
    )

    try:
        await delete_pilots_by_stamps_bulk(pilot_db=pilot_db, pilot_stamps=pilot_stamps)
    except PilotNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one pilot has not been found.",
        ) from e


@router.delete("/management/pilot/interval", status_code=HTTPStatus.NO_CONTENT)
async def clear_pilots(
    pilot_db: PilotAgentsDB,
    age_in_days: Annotated[
        int,
        Query(
            description=(
                "The number of days that define the maximum age of pilots to be deleted."
                "Pilots older than this age will be considered for deletion."
            )
        ),
    ],
    check_permissions: CheckDiracServicesPolicyCallable,
    delete_only_aborted: Annotated[
        bool,
        Query(
            description=(
                "Flag indicating whether to only delete pilots whose status is 'Aborted'."
                "If set to True, only pilots with the 'Aborted' status will be deleted."
                "It is set by default as True to avoid any mistake."
            )
        ),
    ] = True,
):
    """Endpoint for DIRAC to delete all pilots that lived more than age_in_days."""
    await check_permissions()

    if age_in_days < 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="age_in_days must be positive.",
        )

    await clear_pilots_bulk(
        pilot_db=pilot_db,
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


@router.patch("/management/pilot", status_code=HTTPStatus.NO_CONTENT)
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
    # TODO: Add an example for openapi
    pilot_stamps = [mapping.PilotStamp for mapping in pilot_stamps_to_fields_mapping]

    # Ensures stamps validity
    await check_permissions(
        action=ActionType.CHANGE_PILOT_FIELD,
        pilot_db=pilot_db,
        pilot_stamps=pilot_stamps,
    )

    await update_pilots_fields(
        pilot_db=pilot_db,
        pilot_stamps_to_fields_mapping=pilot_stamps_to_fields_mapping,
    )


@router.patch("/management/jobs", status_code=HTTPStatus.NO_CONTENT)
async def associate_pilot_with_jobs(
    pilot_db: PilotAgentsDB,
    pilot_stamp: Annotated[str, Body(description="The stamp of the pilot.")],
    pilot_jobs_ids: Annotated[
        list[int], Body(description="The jobs we want to add to the pilot.")
    ],
    check_permissions: CheckDiracServicesPolicyCallable,
):
    """Endpoint only for DIRAC services, to associate a pilot with a job."""
    await check_permissions()

    try:
        await associate_pilot_with_jobs_bl(
            pilot_db=pilot_db,
            pilot_stamp=pilot_stamp,
            pilot_jobs_ids=pilot_jobs_ids,
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
