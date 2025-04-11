from __future__ import annotations

from typing import Annotated

from fastapi import (
    Depends,
    HTTPException,
    status,
)

from diracx.core.exceptions import (
    AuthorizationError,
    InvalidCredentialsError,
    PilotAlreadyExistsError,
    PilotNotFoundError,
)
from diracx.core.models import TokenResponse
from diracx.logic.auth.pilot import (
    add_pilot_credentials,
    register_new_pilots,
    try_login,
)
from diracx.logic.auth.token import create_token, generate_pilot_tokens
from diracx.logic.pilots.utils import get_pilot_ids_from_references
from diracx.routers.pilots.access_policies import RegisteredPilotAccessPolicyCallable

from ..dependencies import (
    AuthDB,
    AuthSettings,
    AvailableSecurityProperties,
    Config,
    PilotAgentsDB,
)
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token

router = DiracxRouter(require_auth=False)


@router.post("/pilot-login")
async def pilot_login(
    pilot_db: PilotAgentsDB,
    auth_db: AuthDB,
    pilot_job_reference: str,
    pilot_secret: str,
    config: Config,
    settings: AuthSettings,
    available_properties: AvailableSecurityProperties,
) -> TokenResponse:
    """Endpoint without policy, the pilot uses only its secret."""
    try:
        await try_login(
            pilot_reference=pilot_job_reference,
            pilot_db=pilot_db,
            pilot_secret=pilot_secret,
        )
    except AuthorizationError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=e.detail
        ) from e
    except PilotNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="bad pilot_id / pilot_secret",
        ) from e

    try:
        access_token, refresh_token = await generate_pilot_tokens(
            pilot_db=pilot_db,
            auth_db=auth_db,
            pilot_job_reference=pilot_job_reference,
            config=config,
            settings=settings,
            available_properties=available_properties,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)
        ) from e

    serialized_access_token = create_token(access_token, settings=settings)

    serialized_refresh_token = create_token(refresh_token, settings=settings)

    return TokenResponse(
        access_token=serialized_access_token,
        expires_in=settings.access_token_expire_minutes * 60,
        refresh_token=serialized_refresh_token,
    )


@router.post("/pilot-refresh-token")
async def refresh_pilot_tokens(
    pilot_db: PilotAgentsDB,
    auth_db: AuthDB,
    config: Config,
    settings: AuthSettings,
    available_properties: AvailableSecurityProperties,
    check_permissions: RegisteredPilotAccessPolicyCallable,
    refresh_token: str,
    pilot_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
) -> TokenResponse:
    """Endpoint where a pilot can exchange a refresh token against a token."""
    await check_permissions()

    try:
        new_access_token, new_refresh_token = await generate_pilot_tokens(
            pilot_db=pilot_db,
            auth_db=auth_db,
            pilot_job_reference=pilot_info.preferred_username,
            config=config,
            settings=settings,
            available_properties=available_properties,
            refresh_token=refresh_token,
        )
    except InvalidCredentialsError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e)
        ) from e
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)
        ) from e

    serialized_access_token = create_token(new_access_token, settings=settings)

    serialized_refresh_token = create_token(new_refresh_token, settings=settings)

    return TokenResponse(
        access_token=serialized_access_token,
        expires_in=settings.access_token_expire_minutes * 60,
        refresh_token=serialized_refresh_token,
    )


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
