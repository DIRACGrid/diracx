from __future__ import annotations

import re
from typing import Annotated, Any

from fastapi import Depends, HTTPException, status
from fastapi.security import OpenIdConnect
from joserfc.errors import JoseError
from joserfc.jwt import JWTClaimsRegistry
from pydantic import BaseModel

from diracx.core.models import UUID, UserInfo
from diracx.core.properties import SecurityProperty
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


class AuthInfo(BaseModel):
    # raw token for propagation
    bearer_token: str

    # token ID in the DB for Component
    # unique jwt identifier for user
    token_id: UUID

    # list of DIRAC properties
    properties: list[SecurityProperty]

    policies: dict[str, Any] = {}


class AuthorizedUserInfo(AuthInfo, UserInfo):
    pass


async def verify_dirac_access_token(
    authorization: Annotated[str, Depends(oidc_scheme)],
    settings: AuthSettings,
) -> AuthorizedUserInfo:
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

        return AuthorizedUserInfo(
            bearer_token=raw_token,
            token_id=claims["jti"],
            properties=claims["dirac_properties"],
            sub=claims["sub"],
            preferred_username=claims["preferred_username"],
            dirac_group=claims["dirac_group"],
            vo=claims["vo"],
            policies=claims.get("dirac_policies", {}),
        )
    except (JoseError, KeyError) as e:
        # We catch KeyError to prevent pilots accessing user resources
        # -> If a pilot tries, because he has not dirac_properties, KeyError will be raised
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid JWT",
        ) from e
