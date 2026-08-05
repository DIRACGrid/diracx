from __future__ import annotations

from http import HTTPStatus
from typing import Annotated

from fastapi import Body, HTTPException

from diracx.core.exceptions import PilotAlreadyExistsError, PilotNotFoundError
from diracx.core.models.pilot import PilotMetadata, PilotRegistrationParams
from diracx.db.sql import PilotAgentsDB
from diracx.logic.pilots import (
    register_new_pilot,
    update_pilots_metadata,
)

from ..dependencies import Config
from ..fastapi_classes import DiracxRouter
from .access_policies import (
    ActionType,
    CheckPilotManagementPolicyCallable,
)

router = DiracxRouter()


@router.post("/")
async def register_pilot(
    config: Config,
    pilot_db: PilotAgentsDB,
    registration: PilotRegistrationParams,
    check_permissions: CheckPilotManagementPolicyCallable,
):
    """Register a pilot with its reference.

    If the stamp already exists, the registration is rejected with a 409.
    Registering into a VO that is not in the registry is rejected with a 400.
    """
    # Legacy (X.509 / GENERIC_PILOT) pilot identities may self-register:
    # pilots started in the vacuum have no SiteDirector to register them.
    # This mirrors dirac-admin-add-pilot in legacy DIRAC. The route takes a
    # single stamp per call, which bounds what a stolen credential can do.
    await check_permissions(
        action=ActionType.MANAGE_PILOTS,
        target_vo=registration.vo,
        allow_legacy_pilots=True,
    )

    try:
        await register_new_pilot(
            config=config,
            pilot_db=pilot_db,
            registration=registration,
        )
    except ValueError as e:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=str(e)) from e
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
    # Legacy pilot identities may self-update (dirac-admin-add-pilot
    # --status); the policy caps them to a single pilot stamp per call.
    await check_permissions(
        action=ActionType.MANAGE_PILOTS,
        pilot_db=pilot_db,
        pilot_stamps=list(updates),
        allow_legacy_pilots=True,
    )

    try:
        await update_pilots_metadata(
            pilot_db=pilot_db,
            updates=updates,
        )
    except PilotNotFoundError as e:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(e)) from e
