from __future__ import annotations

from http import HTTPStatus
from typing import Annotated

from fastapi import Body, HTTPException

from diracx.core.exceptions import PilotAlreadyExistsError, PilotNotFoundError
from diracx.core.models.pilot import PilotMetadata, PilotStatus
from diracx.db.sql import PilotAgentsDB
from diracx.logic.pilots.management import (
    register_new_pilots,
    update_pilots_metadata,
)

from ..fastapi_classes import DiracxRouter
from .access_policies import (
    ActionType,
    CheckPilotManagementPolicyCallable,
)

router = DiracxRouter()


@router.post("/")
async def register_pilot(
    pilot_db: PilotAgentsDB,
    pilot_stamp: Annotated[
        str,
        Body(description="Stamp of the pilot to create."),
    ],
    vo: Annotated[str, Body(description="Pilot virtual organization.")],
    check_permissions: CheckPilotManagementPolicyCallable,
    grid_type: Annotated[str, Body(description="Grid type of the pilot.")] = "DIRAC",
    grid_site: Annotated[str, Body(description="Pilot grid site.")] = "Unknown",
    destination_site: Annotated[
        str, Body(description="Pilot destination site.")
    ] = "NotAssigned",
    pilot_reference: Annotated[
        str | None,
        Body(description="Pilot reference."),
    ] = None,
    pilot_status: Annotated[
        PilotStatus, Body(description="Initial status of the pilot.")
    ] = PilotStatus.SUBMITTED,
):
    """Register a pilot with its reference.

    If the stamp already exists, the registration is rejected with a 409.
    """
    # TODO: Verify that grid types, sites, destination sites, etc. are valid
    # Legacy (X.509 / GENERIC_PILOT) pilot identities may self-register:
    # pilots started in the vacuum have no SiteDirector to register them.
    # This mirrors dirac-admin-add-pilot in legacy DIRAC. The route takes a
    # single stamp per call, which bounds what a stolen credential can do.
    await check_permissions(
        action=ActionType.MANAGE_PILOTS,
        target_vo=vo,
        allow_legacy_pilots=True,
    )

    try:
        await register_new_pilots(
            pilot_db=pilot_db,
            pilot_stamps=[pilot_stamp],
            vo=vo,
            grid_type=grid_type,
            grid_site=grid_site,
            destination_site=destination_site,
            pilot_job_references={pilot_stamp: pilot_reference}
            if pilot_reference
            else None,
            status=pilot_status,
        )
    except PilotAlreadyExistsError as e:
        raise HTTPException(status_code=HTTPStatus.CONFLICT, detail=str(e)) from e


EXAMPLE_UPDATE_METADATA = {
    "Update the BenchMark field": {
        "summary": "Update BenchMark",
        "description": "Update only the BenchMark for one pilot.",
        "value": {"the_pilot_stamp": {"BenchMark": 1.0}},
    },
    "Update multiple statuses": {
        "summary": "Update multiple pilots",
        "description": "Update statuses for multiple pilots at once.",
        "value": {
            "first_stamp": {"Status": "Waiting"},
            "second_stamp": {"Status": "Waiting"},
        },
    },
}


@router.patch("/metadata", status_code=HTTPStatus.NO_CONTENT)
async def update_pilot_metadata(
    updates: Annotated[
        dict[str, PilotMetadata],
        Body(
            description="Mapping from pilot stamp to the metadata to apply.",
            openapi_examples=EXAMPLE_UPDATE_METADATA,  # type: ignore
        ),
    ],
    pilot_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
):
    """Update pilot metadata (status, benchmark, etc.).

    Only fields defined in `PilotMetadata` are mutable. The pilot stamp
    (the mapping key) identifies the pilot and cannot be changed.
    """
    pilot_stamps = list(updates)
    # Legacy pilot identities may self-update (dirac-admin-add-pilot
    # --status); the policy caps them to a single pilot stamp per call.
    await check_permissions(
        action=ActionType.MANAGE_PILOTS,
        pilot_db=pilot_db,
        pilot_stamps=pilot_stamps,
        allow_legacy_pilots=True,
    )

    try:
        await update_pilots_metadata(
            pilot_db=pilot_db,
            updates=updates,
        )
    except PilotNotFoundError as e:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(e)) from e
