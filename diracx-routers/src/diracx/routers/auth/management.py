"""This module contains the auth management endpoints.

These endpoints are used to manage the user's authentication tokens and
to get information about the user's identity.
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

from fastapi import Depends, HTTPException, status
from joserfc.errors import DecodeError
from typing_extensions import TypedDict
from uuid_utils import UUID

from diracx.core.exceptions import TokenNotFoundError
from diracx.core.properties import PROXY_MANAGEMENT, SecurityProperty
from diracx.logic.auth.management import (
    get_refresh_tokens as get_refresh_tokens_bl,
)
from diracx.logic.auth.management import (
    revoke_refresh_token_by_jti as revoke_refresh_token_by_jti_bl,
)
from diracx.logic.auth.management import (
    revoke_refresh_token_by_refresh_token as revoke_refresh_token_by_refresh_token_bl,
)

from ..dependencies import AuthDB, AuthSettings
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token

router = DiracxRouter(require_auth=False)


logger = logging.getLogger(__name__)


class UserInfoResponse(TypedDict):
    """Response for the userinfo endpoint."""

    sub: str
    vo: str
    dirac_group: str
    policies: dict[str, Any]
    properties: list[SecurityProperty]
    preferred_username: str


@router.get("/refresh-tokens")
async def get_refresh_tokens(
    auth_db: AuthDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
) -> list:
    """Get all refresh tokens for the user. If the user has the `proxy_management` property, then
    the subject is not used to filter the refresh tokens.
    """
    subject: str | None = user_info.sub
    if PROXY_MANAGEMENT in user_info.properties:
        subject = None

    return await get_refresh_tokens_bl(auth_db, subject)


@router.post("/revoke")
async def revoke_refresh_token_by_refresh_token(
    auth_db: AuthDB,
    settings: AuthSettings,
    refresh_token: str,
    client_id: str,
) -> str:
    """Revoke a refresh token."""
    # Test the client_id
    if settings.dirac_client_id != client_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Unrecognised client_id"
        )

    try:
        await revoke_refresh_token_by_refresh_token_bl(
            auth_db, None, refresh_token, settings
        )
    except DecodeError:
        logger.warning("Someone tried to revoke its token but failed.")

    return "Refresh token revoked"


@router.delete("/refresh-tokens/{jti}")
async def revoke_refresh_token_by_jti(
    auth_db: AuthDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    jti: str,
) -> str:
    """Revoke a refresh token. If the user has the `proxy_management` property, then
    the subject is not used to filter the refresh tokens.
    """
    subject: str | None = user_info.sub
    if PROXY_MANAGEMENT in user_info.properties:
        subject = None

    try:
        await revoke_refresh_token_by_jti_bl(auth_db, subject, UUID(jti))
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except PermissionError as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(e),
        ) from e
    except TokenNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        ) from e
    return "Refresh token revoked"


@router.get("/userinfo")
async def userinfo(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
) -> UserInfoResponse:
    """Get information about the user's identity."""
    return {
        "sub": user_info.sub,
        "vo": user_info.vo,
        "dirac_group": user_info.dirac_group,
        "properties": user_info.properties,
        "policies": user_info.policies,
        "preferred_username": user_info.preferred_username,
    }
