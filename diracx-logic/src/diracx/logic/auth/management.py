"""This module contains the auth management functions."""

from __future__ import annotations

from uuid_utils import UUID

from diracx.core.settings import AuthSettings
from diracx.db.sql import AuthDB
from diracx.logic.auth.utils import verify_dirac_refresh_token


async def get_refresh_tokens(
    auth_db: AuthDB,
    subject: str | None,
) -> list:
    """Get all refresh tokens bound to a given subject. If there is no subject, then
    all the refresh tokens are retrieved.
    """
    return await auth_db.get_user_refresh_tokens(subject)


async def revoke_refresh_token_by_jti(
    auth_db: AuthDB,
    subject: str | None,
    jti: UUID,
) -> str:
    """Revoke a refresh token. If a subject is provided, then the refresh token must be owned by that subject."""
    res = await auth_db.get_refresh_token(jti)

    if subject and subject != res["Sub"]:
        raise PermissionError("Cannot revoke a refresh token owned by someone else")

    await auth_db.revoke_refresh_token(jti)
    return f"Refresh token {jti} revoked"


async def revoke_refresh_token_by_refresh_token(
    auth_db: AuthDB, subject: str | None, refresh_token: str, settings: AuthSettings
) -> str:
    """Revoke a refresh token. If a subject is provided, then the refresh token must be owned by that subject."""
    jti, _, _ = await verify_dirac_refresh_token(refresh_token, settings)

    return await revoke_refresh_token_by_jti(auth_db=auth_db, subject=subject, jti=jti)
