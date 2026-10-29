"""FastAPI endpoints for managing authentication tokens and user identity.

The endpoints list and revoke refresh tokens and return identity information
from the authenticated user's access token.
"""

from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Annotated, Any

from fastapi import Depends, Form, HTTPException
from joserfc.errors import DecodeError
from pydantic import ConfigDict, with_config
from typing_extensions import TypedDict
from uuid_utils import UUID

from diracx.core.exceptions import InvalidCredentialsError, TokenNotFoundError
from diracx.core.properties import PROXY_MANAGEMENT, SecurityProperty
from diracx.core.settings import AuthSettings
from diracx.db.sql import AuthDB
from diracx.logic.auth import (
    get_refresh_tokens as get_refresh_tokens_bl,
)
from diracx.logic.auth import (
    revoke_refresh_token_by_jti as revoke_refresh_token_by_jti_bl,
)
from diracx.logic.auth import (
    revoke_refresh_token_by_refresh_token as revoke_refresh_token_by_refresh_token_bl,
)

from ..fastapi_classes import DiracxRouter
from ..utils import AuthorizedUserInfo, verify_dirac_access_token

router = DiracxRouter(require_auth=False)


logger = logging.getLogger(__name__)


@with_config(ConfigDict(use_attribute_docstrings=True))
class UserInfoResponse(TypedDict):
    """Identity information returned by the userinfo endpoint."""

    sub: str
    """Subject identifier from the access token."""
    vo: str
    """Virtual organisation associated with the user."""
    dirac_group: str
    """DIRAC group associated with the user."""
    policies: dict[str, Any]
    """Policies included in the access token."""
    properties: list[SecurityProperty]
    """DIRAC security properties included in the access token."""
    preferred_username: str
    """Preferred username for display."""


@router.get("/refresh-tokens")
async def get_refresh_tokens(
    auth_db: AuthDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
) -> list:
    """List refresh tokens available to the authenticated user.

    Users with the ``proxy_management`` property can list tokens belonging to
    any subject; other users see only tokens bound to their own subject.

    \f

    Args:
        auth_db: Database used to retrieve refresh tokens.
        user_info: Authenticated user and security properties from the access token.

    Returns:
        Refresh-token records visible to the user.
    """
    subject: str | None = user_info.sub
    if PROXY_MANAGEMENT in user_info.properties:
        subject = None

    return await get_refresh_tokens_bl(auth_db, subject)


@router.post("/revoke")
async def revoke_refresh_token_by_refresh_token(
    auth_db: AuthDB,
    settings: AuthSettings,
    token: Annotated[str, Form(description="The refresh token to revoke")],
    token_type_hint: Annotated[
        str | None,
        Form(description="Hint for the type of token being revoked"),
    ] = None,
    client_id: Annotated[
        str,
        Form(description="The client ID of the application requesting the revocation"),
    ] = "myDIRACClientID",
) -> str:
    """Revoke a refresh token using the RFC 7009 token-revocation flow.

    Invalid or undecodable tokens are treated as successfully revoked, as
    required by the token-revocation response semantics.

    \f

    Args:
        auth_db: Database containing the refresh token.
        settings: Authentication settings used to validate the client and token.
        token: Refresh token to revoke.
        token_type_hint: Optional hint identifying the token type.
        client_id: Identifier of the client requesting revocation.

    Returns:
        A confirmation string.

    Raises:
        HTTPException: If the token type is unsupported (400) or client
            credentials are invalid (401).
    """
    try:
        await revoke_refresh_token_by_refresh_token_bl(
            auth_db, None, token, token_type_hint, client_id, settings
        )
    except (DecodeError, KeyError):
        logger.warning(
            "Token revocation failed: invalid token format (client_id=%s)",
            client_id,
            exc_info=True,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e
    except InvalidCredentialsError as e:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        ) from e

    return "Refresh token revoked"


@router.delete("/refresh-tokens/{jti}")
async def revoke_refresh_token_by_jti(
    auth_db: AuthDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    jti: str,
) -> str:
    """Revoke a refresh token by its unique identifier.

    Users with the ``proxy_management`` property can revoke tokens belonging
    to any subject; other users can revoke only their own tokens.

    \f

    Args:
        auth_db: Database containing the refresh token.
        user_info: Authenticated user and security properties from the access token.
        jti: Unique identifier of the refresh token to revoke.

    Returns:
        A confirmation string.

    Raises:
        HTTPException: If the identifier is invalid (400), the token belongs
            to another user (403), or the token does not exist (404).
    """
    subject: str | None = user_info.sub
    if PROXY_MANAGEMENT in user_info.properties:
        subject = None

    try:
        await revoke_refresh_token_by_jti_bl(auth_db, subject, UUID(jti))
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e
    except PermissionError as e:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail=str(e),
        ) from e
    except TokenNotFoundError as e:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=str(e),
        ) from e
    return "Refresh token revoked"


@router.get("/userinfo")
async def userinfo(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
) -> UserInfoResponse:
    """Return identity and authorisation details from the authenticated access token.

    \f

    Args:
        user_info: Authenticated user information extracted from the access token.

    Returns:
        The user's subject, VO, DIRAC group, policies, properties, and preferred username.
    """
    return {
        "sub": user_info.sub,
        "vo": user_info.vo,
        "dirac_group": user_info.dirac_group,
        "properties": user_info.properties,
        "policies": user_info.policies,
        "preferred_username": user_info.preferred_username,
    }
