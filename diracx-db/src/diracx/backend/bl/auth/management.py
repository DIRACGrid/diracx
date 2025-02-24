"""This module contains the auth management endpoints.

These endpoints are used to manage the user's authentication tokens and
to get information about the user's identity.
"""

from __future__ import annotations

from uuid import UUID

from diracx.backend.dal.sql import AuthDB
from diracx.core.models import AuthorizedUserInfo
from diracx.core.properties import PROXY_MANAGEMENT


async def get_refresh_tokens(
    auth_db: AuthDB,
    user_info: AuthorizedUserInfo,
) -> list:
    """Get all refresh tokens for the user. If the user has the `proxy_management` property, then
    the subject is not used to filter the refresh tokens.
    """
    subject: str | None = user_info.sub
    if PROXY_MANAGEMENT in user_info.properties:
        subject = None

    return await auth_db.get_user_refresh_tokens(subject)


async def revoke_refresh_token(
    auth_db: AuthDB,
    user_info: AuthorizedUserInfo,
    jti: UUID,
) -> str:
    """Revoke a refresh token. If the user has the `proxy_management` property, then
    the subject is not used to filter the refresh tokens.
    """
    res = await auth_db.get_refresh_token(jti)

    if PROXY_MANAGEMENT not in user_info.properties and user_info.sub != res["Sub"]:
        raise PermissionError("Cannot revoke a refresh token owned by someone else")

    await auth_db.revoke_refresh_token(jti)
    return f"Refresh token {jti} revoked"
