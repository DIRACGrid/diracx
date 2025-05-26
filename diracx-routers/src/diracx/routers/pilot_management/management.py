from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Annotated

from fastapi import Body, Depends, HTTPException, Query, status

from diracx.core.exceptions import (
    PilotAlreadyExistsError,
    PilotNotFoundError,
)
from diracx.core.models import PilotCredentialsInfo, PilotStampInfo
from diracx.logic.pilots.auth import (
    register_new_pilots,
)
from diracx.logic.pilots.management import (
    clear_pilots_bulk,
    delete_pilots_by_stamps_bulk,
)

from ..dependencies import AuthSettings, PilotAgentsDB
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import (
    ActionType,
    CheckDiracServicesPolicyCallable,
    CheckPilotManagementPolicyCallable,
)

router = DiracxRouter()

logger = logging.getLogger(__name__)


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
    settings: AuthSettings,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckPilotManagementPolicyCallable,
    grid_type: Annotated[str, Body(description="Grid type of the pilots.")] = "Dirac",
    pilot_references: Annotated[
        dict | None,
        Body(description="Association of a pilot reference with a pilot stamp."),
    ] = None,
    generate_secrets: Annotated[
        bool, Body(description="Boolean to allow secret creation or not.")
    ] = True,
    pilot_secret_use_count_max: Annotated[
        int, Body(description="Number of times we can use a secret.")
    ] = 1,
) -> list[PilotStampInfo] | list[PilotCredentialsInfo]:
    """Endpoint where a you can create pilots with their references.
    It will return the pilot secrets as well as an expiration date.

    If a pilot stamp already exists, it will block the insertion.
    """
    await check_permissions(action=ActionType.CREATE_PILOT_OR_SECRET, vo=vo)

    try:
        pilot_response = await register_new_pilots(
            pilot_db=pilot_db,
            pilot_stamps=pilot_stamps,
            vo=vo,
            pilot_secret_use_count_max=pilot_secret_use_count_max,
            settings=settings,
            generate_secrets=generate_secrets,
            grid_type=grid_type,
            pilot_job_references=pilot_references,
        )

        # Logs credentials creation
        logger.debug(
            f"{user_info.preferred_username} added {len(pilot_stamps)} pilots."
        )

        return pilot_response
    except PilotAlreadyExistsError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e)) from e


@router.delete("/", status_code=HTTPStatus.NO_CONTENT)
async def delete_pilots(
    pilot_stamps: Annotated[
        list[str], Query(description="Stamps of the pilots we want to delete.")
    ],
    pilot_agents_db: PilotAgentsDB,
    check_permissions: CheckPilotManagementPolicyCallable,
):
    """Endpoint to delete a pilot.

    If at least one pilot is not found, it WILL rollback.
    """
    await check_permissions(
        action=ActionType.CHANGE_PILOT_FIELD,
        pilot_stamps=pilot_stamps,
        pilot_db=pilot_agents_db,
    )

    try:
        await delete_pilots_by_stamps_bulk(
            pilot_db=pilot_agents_db, pilot_stamps=pilot_stamps
        )
    except PilotNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one pilot has not been found.",
        ) from e


@router.delete("/interval", status_code=HTTPStatus.NO_CONTENT)
async def clear_pilots(
    pilot_agents_db: PilotAgentsDB,
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
        pilot_db=pilot_agents_db,
        age_in_days=age_in_days,
        delete_only_aborted=delete_only_aborted,
    )
