from __future__ import annotations

import re
from typing import Annotated

from fastapi import Header, HTTPException, status
from joserfc.errors import JoseError
from joserfc.jwt import JWTClaimsRegistry
from pydantic import BaseModel

from diracx.core.models import UUID, PilotInfo
from diracx.logic.auth.utils import read_token
from diracx.routers.dependencies import AuthSettings


class AuthInfo(BaseModel):
    # raw token for propagation
    bearer_token: str

    # token ID in the DB for Component
    # unique jwt identifier for user
    token_id: UUID


class AuthorizedPilotInfo(AuthInfo, PilotInfo):
    pass


async def verify_dirac_pilot_access_token(
    settings: AuthSettings,
    authorization: Annotated[str | None, Header()] = None,
) -> AuthorizedPilotInfo:
    """Verify dirac user token and return a UserInfo class
    Used for each API endpoint.
    """
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header is missing",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if match := re.fullmatch(r"Bearer (.+)", authorization):
        raw_token = match.group(1)
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid authorization header",
        )

    try:
        claims = read_token(
            raw_token,
            settings.token_keystore.jwks,
            settings.token_allowed_algorithms,
            claims_requests=JWTClaimsRegistry(
                iss={"essential": True, "value": settings.token_issuer},
            ),
        )

        return AuthorizedPilotInfo(
            bearer_token=raw_token,
            token_id=claims["jti"],
            sub=claims["sub"],
            pilot_stamp=claims["pilot_stamp"],
            vo=claims["vo"],
        )
    # We catch KeyError if a user tries with its token to access this resource:
    # -> claims["pilot_stamp"] will lead to a KeyError
    except (JoseError, KeyError) as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid JWT",
        ) from e
