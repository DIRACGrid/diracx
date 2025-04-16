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
from diracx.logic.pilots.utils import get_pilot_ids_from_references

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
    pilot_references: Annotated[
        list[str],
        Body(description="List of the pilot references we want to add to the db."),
    ],
    vo: Annotated[
        str,
        Body(description="Virtual Organisation associated with the inserted pilots."),
    ],
    settings: AuthSettings,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    grid_type: Annotated[str, Body(description="Grid type of the pilots.")] = "Dirac",
    pilot_stamps: Annotated[
        dict | None,
        Body(description="Association of a pilot reference with a pilot stamp."),
    ] = None,
) -> PilotCredentialsResponse:
    """Endpoint where a you can create pilots with their references.
    It will return the pilot secrets as well as an expiration date.

    If a pilot reference already exists, it will block the insertion.
    """
    try:
        await register_new_pilots(
            pilot_db=pilot_db,
            pilot_job_references=pilot_references,
            vo=vo,
            grid_type=grid_type,
            pilot_stamps=pilot_stamps,
        )

        pilot_ids = await get_pilot_ids_from_references(
            pilot_db=pilot_db, pilot_references=pilot_references
        )
    except PilotAlreadyExistsError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e)) from e

    credentials, expiration_dates = await add_pilot_credentials(
        pilot_ids=pilot_ids, pilot_db=pilot_db, settings=settings
    )

    # Logs credentials creation
    logger.debug(
        f"{user_info.preferred_username} added {len(pilot_references)} pilots."
    )

    return create_pilot_credentials_response(
        pilot_references=pilot_references,
        pilot_secrets=credentials,
        pilot_expiration_dates=expiration_dates,
    )
