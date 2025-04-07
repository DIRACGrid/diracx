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
    PilotNotFoundError,
)
from diracx.logic.auth.pilot import try_login
from diracx.logic.auth.token import create_token, generate_pilot_tokens
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
):
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

    return {
        "access_token": create_token(access_token, settings),
        "refresh_token": create_token(refresh_token, settings),
    }


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
):
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

    return {
        "access_token": create_token(new_access_token, settings),
        "refresh_token": create_token(new_refresh_token, settings),
    }
