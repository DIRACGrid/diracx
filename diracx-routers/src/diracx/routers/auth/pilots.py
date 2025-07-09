"""Token endpoint."""

from __future__ import annotations

from typing import Annotated

from fastapi import Body, HTTPException, status

from diracx.core.exceptions import (
    BadPilotCredentialsError,
    BadTokenError,
    InvalidCredentialsError,
    PilotNotFoundError,
    SecretHasExpiredError,
    SecretNotFoundError,
)
from diracx.core.models import (
    PilotAuthCredentials,
    TokenResponse,
    VacuumPilotAuth,
)
from diracx.logic.pilots.auth import refresh_pilot_token, verify_pilot_credentials

from ..dependencies import (
    AuthDB,
    AuthSettings,
    PilotAgentsDB,
)
from ..fastapi_classes import DiracxRouter

router = DiracxRouter(require_auth=False)


@router.post("/secret-exchange")
async def perform_secret_exchange(
    pilot_db: PilotAgentsDB,
    auth_db: AuthDB,
    pilot_credentials: Annotated[
        PilotAuthCredentials | VacuumPilotAuth,
        Body(description="Pilot credentials (stamp and secret)"),
    ],
    settings: AuthSettings,
) -> TokenResponse:
    """This endpoint is used by the pilot to exchange a secret for a token.

    This endpoint also acts as DIRAC's `dirac-admin-add-pilot`.
    """
    try:
        return await verify_pilot_credentials(
            pilot_db=pilot_db,
            auth_db=auth_db,
            credentials=pilot_credentials,
            settings=settings,
        )
    except BadPilotCredentialsError as e:
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


@router.post("/pilot-token")
async def refresh_pilot_tokens(
    auth_db: AuthDB,
    settings: AuthSettings,
    pilot_db: PilotAgentsDB,
    refresh_token: Annotated[
        str, Body(description="Refresh Token given at login by DiracX.")
    ],
    pilot_stamp: Annotated[str, Body(description="Pilot stamp")],
) -> TokenResponse:
    """Endpoint where *only* pilots can exchange a refresh token for a token."""
    # Refresh it
    try:
        return await refresh_pilot_token(
            pilot_stamp=pilot_stamp,
            auth_db=auth_db,
            settings=settings,
            pilot_db=pilot_db,
            refresh_token=refresh_token,
        )
    except (InvalidCredentialsError, PilotNotFoundError, BadTokenError) as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e)
        ) from e
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)
        ) from e
