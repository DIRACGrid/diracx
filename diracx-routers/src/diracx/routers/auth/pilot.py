from __future__ import annotations

import logging
from typing import Annotated

from fastapi import Body, Depends, HTTPException, status

from diracx.core.exceptions import (
    PilotAlreadyExistsError,
)
from diracx.core.models import PilotCredentialsResponse
from diracx.logic.auth.pilot import (
    add_pilot_credentials,
    create_pilot_credentials_response,
    register_new_pilots,
)

from ..dependencies import (
    AuthSettings,
    PilotAgentsDB,
)
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token

router = DiracxRouter()

logger = logging.getLogger(__name__)


@router.post("/register-new-pilots")
async def register_new_pilots_to_db(
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
    grid_type: Annotated[str, Body(description="Grid type of the pilots.")] = "Dirac",
    pilot_references: Annotated[
        dict | None,
        Body(description="Association of a pilot reference with a pilot stamp."),
    ] = None,
) -> PilotCredentialsResponse:
    """Endpoint where a you can create pilots with their references.
    It will return the pilot secrets as well as an expiration date.

    If a pilot stamp already exists, it will block the insertion.
    """
    try:
        await register_new_pilots(
            pilot_db=pilot_db,
            pilot_stamps=pilot_stamps,
            vo=vo,
            grid_type=grid_type,
            pilot_job_references=pilot_references,
        )
    except PilotAlreadyExistsError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e)) from e

    credentials, expiration_dates = await add_pilot_credentials(
        pilot_stamps=pilot_stamps, pilot_db=pilot_db, settings=settings
    )

    # Logs credentials creation
    logger.debug(f"{user_info.preferred_username} added {len(pilot_stamps)} pilots.")

    return create_pilot_credentials_response(
        pilot_stamps=pilot_stamps,
        pilot_secrets=credentials,
        pilot_expiration_dates=expiration_dates,
    )
