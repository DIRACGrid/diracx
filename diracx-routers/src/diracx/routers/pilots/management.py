from __future__ import annotations

from http import HTTPStatus
from typing import Annotated

from fastapi import Body, Depends, HTTPException, Query, status

from diracx.core.models.pilot import PilotMetadata, PilotStatus
from diracx.core.properties import GENERIC_PILOT, JOB_ADMINISTRATOR
from diracx.db.sql import PilotAgentsDB
from diracx.logic.pilots.management import (
    delete_pilots as delete_pilots_bl,
)
from diracx.logic.pilots.management import (
    register_new_pilots,
    update_pilots_metadata,
)
from diracx.routers.utils.users import AuthorizedUserInfo, verify_dirac_access_token

from ..fastapi_classes import DiracxRouter
from .access_policies import (
    ActionType,
    CheckPilotManagementPolicyCallable,
)

router = DiracxRouter()


@router.post("/")
async def register_pilots(
    pilot_db: PilotAgentsDB,
    pilot_stamps: Annotated[
        list[str],
        Body(description="Stamps of the pilots to create."),
    ],
    vo: Annotated[str, Body(description="Pilot virtual organization.")],
    check_permissions: CheckPilotManagementPolicyCallable,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
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
        PilotStatus, Body(description="Initial status of the pilots.")
    ] = PilotStatus.SUBMITTED,
):
    """Register a batch of pilots with their references.

    If any stamp already exists, the whole batch is rejected with a 409.
    """
    # TODO: Verify that grid types, sites, destination sites, etc. are valid
    await check_permissions(
        action=ActionType.MANAGE_PILOTS,
        allow_legacy_pilots=True,  # dirac-admin-add-pilot
    )

    # Limit the damage a stolen pilot credential can do: a pilot identity
    # can only register a single stamp per call.
    if GENERIC_PILOT in user_info.properties and len(pilot_stamps) != 1:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to register more than one pilot.",
        )

    if JOB_ADMINISTRATOR not in user_info.properties and vo != user_info.vo:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Pilots can only be registered for your own VO.",
        )

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


@router.delete("/", status_code=HTTPStatus.NO_CONTENT)
async def delete_pilots(
    pilot_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
    pilot_stamps: Annotated[
        list[str], Query(description="Stamps of the pilots to delete.", min_length=1)
    ],
):
    """Delete pilots by stamp.

    Deletes the pilot rows as well as their logs and job associations.

    Age-based retention cleanup is deliberately *not* exposed here: it is
    handled by the maintenance task worker. See
    `diracx.logic.pilots.management.delete_pilots`.
    """
    await check_permissions(
        action=ActionType.MANAGE_PILOTS,
        pilot_db=pilot_db,
        pilot_stamps=pilot_stamps,
    )

    await delete_pilots_bl(pilot_db=pilot_db, pilot_stamps=pilot_stamps)


EXAMPLE_UPDATE_METADATA = {
    "Update the BenchMark field": {
        "summary": "Update BenchMark",
        "description": "Update only the BenchMark for one pilot.",
        "value": {
            "pilot_metadata": [{"PilotStamp": "the_pilot_stamp", "BenchMark": 1.0}]
        },
    },
    "Update multiple statuses": {
        "summary": "Update multiple pilots",
        "description": "Update statuses for multiple pilots at once.",
        "value": {
            "pilot_metadata": [
                {"PilotStamp": "first_stamp", "Status": "Waiting"},
                {"PilotStamp": "second_stamp", "Status": "Waiting"},
            ]
        },
    },
}


@router.patch("/metadata", status_code=HTTPStatus.NO_CONTENT)
async def update_pilot_metadata(
    pilot_metadata: Annotated[
        list[PilotMetadata],
        Body(
            description="Pilot metadata mappings to apply.",
            embed=True,
            openapi_examples=EXAMPLE_UPDATE_METADATA,  # type: ignore
        ),
    ],
    pilot_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
):
    """Update pilot metadata (status, benchmark, etc.).

    Only fields defined in `PilotMetadata` are mutable. `PilotStamp`
    identifies the row and cannot be changed.
    """
    pilot_stamps = [m.PilotStamp for m in pilot_metadata]
    await check_permissions(
        action=ActionType.MANAGE_PILOTS,
        pilot_db=pilot_db,
        pilot_stamps=pilot_stamps,
        allow_legacy_pilots=True,  # dirac-admin-add-pilot
    )

    # Limit the damage a stolen pilot credential can do: a pilot identity
    # can only modify a single stamp per call.
    if GENERIC_PILOT in user_info.properties and len(pilot_stamps) != 1:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to modify more than one pilot.",
        )

    await update_pilots_metadata(
        pilot_db=pilot_db,
        pilot_metadata=pilot_metadata,
    )
