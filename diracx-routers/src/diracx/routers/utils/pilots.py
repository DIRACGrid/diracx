from __future__ import annotations

import re
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import OpenIdConnect
from joserfc.errors import DecodeError, ExpiredTokenError
from joserfc.jwt import JWTClaimsRegistry

from diracx.core.models import AuthInfo, PilotInfo
from diracx.logic.auth.utils import read_token
from diracx.routers.dependencies import AuthSettings

# auto_error=False is used to avoid raising the wrong exception when the token is missing
# The error is handled in the verify_dirac_access_token function
# More info:
# - https://github.com/tiangolo/fastapi/issues/10177
# - https://datatracker.ietf.org/doc/html/rfc6750#section-3.1
oidc_scheme = OpenIdConnect(
    openIdConnectUrl="/.well-known/openid-configuration", auto_error=False
)


class AuthorizedPilotInfo(AuthInfo, PilotInfo):
    pass


async def verify_dirac_pilot_access_token(
    authorization: Annotated[str, Depends(oidc_scheme)],
    settings: AuthSettings,
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
    except (DecodeError, ExpiredTokenError, KeyError) as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid JWT",
        ) from e
