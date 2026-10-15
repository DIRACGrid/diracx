"""Define user authentication models and access-token verification helpers.

The module provides Pydantic support for the project's UUID type and a FastAPI
dependency that validates DiracX access tokens and extracts user claims.
"""

from __future__ import annotations

import logging
import re
import uuid as std_uuid
from http import HTTPStatus
from typing import Annotated, Any

from fastapi import Depends, HTTPException
from fastapi.security import OpenIdConnect
from joserfc.errors import JoseError
from joserfc.jwt import JWTClaimsRegistry
from opentelemetry import trace
from pydantic import BaseModel, GetCoreSchemaHandler, GetJsonSchemaHandler
from pydantic_core import CoreSchema, core_schema
from uuid_utils import UUID as _UUID

from diracx.core.logs import set_log_context
from diracx.core.models import UserInfo
from diracx.core.properties import SecurityProperty
from diracx.core.settings import AuthSettings
from diracx.logic.auth import read_token
from diracx.routers.dependencies import auto_inject

logger = logging.getLogger(__name__)

# auto_error=False is used to avoid raising the wrong exception when the token is missing
# The error is handled in the verify_dirac_access_token function
# More info:
# - https://github.com/tiangolo/fastapi/issues/10177
# - https://datatracker.ietf.org/doc/html/rfc6750#section-3.1
oidc_scheme = OpenIdConnect(
    openIdConnectUrl="/.well-known/openid-configuration", auto_error=False
)


class UUID(_UUID):
    """Add Pydantic validation and serialization support to ``uuid_utils.UUID``."""

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Build a core schema using the standard-library UUID type.

        Args:
            source_type: Type for which Pydantic is generating a schema.
            handler: Pydantic handler used to generate the standard UUID schema.

        Returns:
            A core schema that validates standard UUIDs and converts them to
            this ``uuid_utils.UUID`` subclass.
        """
        std_schema = handler(std_uuid.UUID)

        def to_uuid_utils(u: std_uuid.UUID) -> UUID:
            return cls(str(u))

        return core_schema.no_info_after_validator_function(to_uuid_utils, std_schema)

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> dict[str, Any]:
        """Return the JSON schema generated for the standard UUID type.

        Args:
            core_schema: Core schema for this UUID type.
            handler: Pydantic handler used to generate the JSON schema.

        Returns:
            JSON schema compatible with the standard-library UUID type.
        """
        return handler(core_schema)


class AuthInfo(BaseModel):
    """Authentication claims and token metadata for an authorised user.

    Attributes:
        bearer_token: Raw access token, retained for forwarding.
        token_id: Unique token identifier used to reference the token in the database.
        properties: DIRAC security properties granted to the user.
        policies: Additional policy data carried by the token.
    """

    # raw token for propagation
    bearer_token: str

    # token ID in the DB for Component
    # unique jwt identifier for user
    token_id: UUID

    # list of DIRAC properties
    properties: list[SecurityProperty]

    policies: dict[str, Any] = {}


class AuthorizedUserInfo(AuthInfo, UserInfo):
    """Combined identity and authentication information for an authorised user."""

    pass


@auto_inject
async def verify_dirac_access_token(
    authorization: Annotated[str, Depends(oidc_scheme)],
    settings: AuthSettings,
) -> AuthorizedUserInfo:
    """Validate a DiracX bearer token and extract its user information.

    The token's signature and issuer are checked against the configured key
    store and allowed algorithms. User identity is also added to the current
    trace and logging context.

    Args:
        authorization: Optional OpenID Connect authorization header value.
        settings: Authentication settings containing issuer and token-validation keys.

    Returns:
        Authenticated user identity, properties, policy data, and token metadata.

    Raises:
        HTTPException: If the authorization header is missing (401), malformed
            (400), or contains an invalid token (401).
    """
    if not authorization:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail="Authorization header is missing",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if match := re.fullmatch(r"Bearer (.+)", authorization):
        raw_token = match.group(1)
    else:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
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
    except JoseError as e:
        logger.warning("Token validation failed: %s", e)
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail="Invalid JWT",
        ) from e

    # Allows to find the requests of a given user/community in the traces
    # and in the logs of the request
    user_attributes = {
        "enduser.id": claims["sub"],
        "diracx.vo": claims["vo"],
        "diracx.group": claims["dirac_group"],
    }
    trace.get_current_span().set_attributes(user_attributes)
    set_log_context(**user_attributes)

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
