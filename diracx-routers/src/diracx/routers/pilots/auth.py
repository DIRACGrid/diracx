"""Token endpoint."""

from __future__ import annotations

from typing import Annotated

from fastapi import Body, Depends, HTTPException, status

from diracx.core.exceptions import (
    BadPilotCredentialsError,
    CredentialsNotFoundError,
    InvalidCredentialsError,
    PilotNotFoundError,
    SecretHasExpiredError,
    SecretNotFoundError,
)
from diracx.core.models import (
    TokenResponse,
)
from diracx.core.properties import GENERIC_PILOT, LIMITED_DELEGATION
from diracx.logic.pilots.auth import refresh_pilot_token, verify_pilot_credentials
from diracx.routers.access_policies import BaseAccessPolicy

from ..auth.token import mint_token
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


@router.post("/token")
async def pilot_login(
    pilot_db: PilotAgentsDB,
    auth_db: AuthDB,
    pilot_stamp: Annotated[str, Body(description="Stamp used by a pilot to login.")],
    pilot_secret: Annotated[
        str, Body(description="Pilot secret given by Dirac/DiracX.")
    ],
    config: Config,
    settings: AuthSettings,
    available_properties: AvailableSecurityProperties,
    all_access_policies: Annotated[
        dict[str, BaseAccessPolicy], Depends(BaseAccessPolicy.all_used_access_policies)
    ],
) -> TokenResponse:
    """This endpoint is used by the pilot to exchange a secret for a token."""
    try:
        access_payload, refresh_payload = await verify_pilot_credentials(
            pilot_db=pilot_db,
            auth_db=auth_db,
            pilot_stamp=pilot_stamp,
            pilot_secret=pilot_secret,
            config=config,
            settings=settings,
            available_properties=available_properties,
        )
    except (BadPilotCredentialsError, CredentialsNotFoundError) as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="bad credentials"
        ) from e
    except PilotNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="bad pilot_stamp",
        ) from e
    except SecretNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="bad pilot_secret",
        ) from e
    except SecretHasExpiredError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="secret expired",
        ) from e
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)
        ) from e

    return await mint_token(
        access_payload=access_payload,
        refresh_payload=refresh_payload,
        existing_refresh_token=None,
        all_access_policies=all_access_policies,
        settings=settings,
    )


@router.post("/refresh-token")
async def refresh_pilot_tokens(
    auth_db: AuthDB,
    config: Config,
    settings: AuthSettings,
    available_properties: AvailableSecurityProperties,
    refresh_token: Annotated[
        str, Body(description="Refresh Token given at login by DiracX.", embed=True)
    ],
    pilot_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    all_access_policies: Annotated[
        dict[str, BaseAccessPolicy], Depends(BaseAccessPolicy.all_used_access_policies)
    ],
) -> TokenResponse:
    """Endpoint where a pilot can exchange a refresh token for a token."""
    if not {GENERIC_PILOT, LIMITED_DELEGATION} & set(pilot_info.properties):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="you are not a pilot"
        )

    try:
        new_access_token, new_refresh_token = await refresh_pilot_token(
            vo=pilot_info.vo,
            auth_db=auth_db,
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

    return await mint_token(
        access_payload=new_access_token,
        refresh_payload=new_refresh_token,
        existing_refresh_token=refresh_token,
        all_access_policies=all_access_policies,
        settings=settings,
    )
