"""Auth management helpers for refresh token lifecycle and cleanup.

This module contains business logic for refresh token administration,
including listing refresh tokens, revoking tokens by JTI or token value,
and cleanup of expired auth state.
"""

from __future__ import annotations

import logging

from uuid_utils import UUID

from diracx.core.exceptions import InvalidCredentialsError
from diracx.core.models import TokenTypeHint
from diracx.core.settings import AuthSettings
from diracx.db.sql import AuthDB

from .utils import verify_dirac_refresh_token

logger = logging.getLogger(__name__)


async def get_refresh_tokens(
    auth_db: AuthDB,
    subject: str | None,
) -> list:
    """Return refresh tokens bound to a subject.

    If no subject is provided, all refresh tokens are retrieved.

    Args:
        auth_db (AuthDB): Database accessor for refresh token state.
        subject (str | None): Subject identifier to filter by, or ``None`` to
            return all refresh tokens.

    Returns:
        list: Refresh token records matching the query.
    """
    return await auth_db.get_user_refresh_tokens(subject)


async def revoke_refresh_token_by_jti(
    auth_db: AuthDB,
    subject: str | None,
    jti: UUID,
) -> str:
    """Revoke a refresh token by its JWT ID.

    If a subject is provided, the token must belong to that subject.

    Args:
        auth_db (AuthDB): Database accessor for refresh token state.
        subject (str | None): Subject identifier for ownership checks.
        jti (UUID): JWT ID of the refresh token to revoke.

    Returns:
        str: Confirmation message that the refresh token was revoked.
    """
    res = await auth_db.get_refresh_token(jti)

    if subject and subject != res["Sub"]:
        raise PermissionError("Cannot revoke a refresh token owned by someone else")

    await auth_db.revoke_refresh_token(jti)
    return f"Refresh token {jti} revoked"


async def revoke_refresh_token_by_refresh_token(
    auth_db: AuthDB,
    subject: str | None,
    token: str,
    token_type_hint: str | None,
    client_id: str,
    settings: AuthSettings,
) -> str:
    """Revoke a refresh token using RFC 7009 semantics.

    This validates the optional token type hint, verifies the client ID, and
    decodes the provided refresh token before revoking it by JWT ID.

    Args:
        auth_db (AuthDB): Database accessor for refresh token state.
        subject (str | None): Subject identifier for ownership checks.
        token (str): Refresh token to revoke.
        token_type_hint (str | None): Optional token type hint.
        client_id (str): Client identifier making the revocation request.
        settings (AuthSettings): Authentication-related settings.

    Returns:
        str: Confirmation message that the refresh token was revoked.
    """
    # Test the token type hint
    if token_type_hint and token_type_hint == TokenTypeHint.access_token:
        raise ValueError("unsupported_token_type")

    # Test the client_id
    if settings.dirac_client_id != client_id:
        raise InvalidCredentialsError("Unrecognised client_id")

    # Decode and verify the refresh token
    jti, _, _ = await verify_dirac_refresh_token(token, settings)
    return await revoke_refresh_token_by_jti(auth_db=auth_db, subject=subject, jti=jti)


async def cleanup_expired_data(auth_db: AuthDB, settings: AuthSettings) -> None:
    """Remove expired refresh tokens and auth flow data from the database.

    The refresh token partitions are rotated to delete old tokens, while
    expired authorization and device flows are deleted from their respective
    tables.

    Args:
        auth_db (AuthDB): Database accessor for auth state maintenance.
        settings (AuthSettings): Authentication-related settings.
    """
    await auth_db.maintain_refresh_token_partitions(
        retention_months=settings.refresh_token_retention_months,
    )

    auth = await auth_db.clean_expired_authorization_flows(
        max_retention=settings.completed_flow_retention_minutes,
    )
    logger.info("Deleted %d expired authorization flows", auth)

    device = await auth_db.clean_expired_device_flows(
        max_retention=settings.completed_flow_retention_minutes,
    )
    logger.info("Deleted %d expired device flows", device)
