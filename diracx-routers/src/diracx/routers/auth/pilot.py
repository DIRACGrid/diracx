from __future__ import annotations

from fastapi import HTTPException, status

from diracx.core.exceptions import AuthorizationError
from diracx.core.models import TokenResponse
from diracx.logic.auth.token import create_token, exchange_token

from ..dependencies import (
    AuthDB,
    AuthSettings,
    AvailableSecurityProperties,
    Config,
    PilotAgentsDB,
)
from ..fastapi_classes import DiracxRouter

router = DiracxRouter(require_auth=False)


@router.post("/pilot-login")
async def pilot_login(
    pilot_db: PilotAgentsDB,
    auth_db: AuthDB,
    pilot_id: int,
    pilot_secret: str,
    config: Config,
    settings: AuthSettings,
    available_properties: AvailableSecurityProperties,
) -> TokenResponse:
    """Endpoint without policy, the pilot uses only its secret."""
    try:
        await pilot_db.verify_pilot_secret(pilot_id=pilot_id, pilot_secret=pilot_secret)
    except AuthorizationError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=e.detail
        ) from e

    pilot = await pilot_db.get_pilot_by_id(pilot_id=pilot_id)

    pilot_info = {
        "pilot_reference": pilot["PilotJobReference"],
        "sub": pilot["PilotJobReference"],
    }

    try:
        access_token, refresh_token = await exchange_token(
            auth_db=auth_db,
            scope=generate_pilot_scope(pilot),
            oidc_token_info=pilot_info,
            config=config,
            settings=settings,
            available_properties=available_properties,
            pilot_exchange=True,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)
        ) from e

    encoded_access_token = create_token(access_token, settings=settings)
    encoded_refresh_token = create_token(refresh_token, settings=settings)

    return TokenResponse(
        access_token=encoded_access_token,
        expires_in=settings.access_token_expire_minutes * 60,
        refresh_token=encoded_refresh_token,
    )


def generate_pilot_scope(pilot: dict) -> str:
    return f"vo:{pilot['VO']}"
