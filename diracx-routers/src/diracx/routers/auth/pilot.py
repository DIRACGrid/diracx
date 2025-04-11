from __future__ import annotations

from typing import Annotated

from fastapi import (
    Depends,
    HTTPException,
    status,
)

from diracx.core.exceptions import (
    PilotAlreadyExistsError,
)
from diracx.logic.auth.pilot import (
    add_pilot_credentials,
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


@router.post("/register-new-pilots")
async def register_new_pilots_to_db(
    pilot_db: PilotAgentsDB,
    pilot_references: list[str],
    vo: str,
    settings: AuthSettings,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    grid_type: str = "Dirac",
    pilot_stamps: dict | None = None,
):
    """Endpoint where a you can create pilots with their credentials."""
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

    credentials = await add_pilot_credentials(
        pilot_ids=pilot_ids, pilot_db=pilot_db, settings=settings
    )

    # Logs
    print(f"{user_info.preferred_username} added {len(pilot_references)} pilots.")

    return {"credentials": credentials}
