"""This module contains the auth management endpoints.

These endpoints are used to manage the user's authentication tokens and
to get information about the user's identity.
"""

from typing import Annotated, Any

from fastapi import (
    Depends,
    HTTPException,
    status,
)
from typing_extensions import TypedDict

from diracx.core.properties import PROXY_MANAGEMENT, SecurityProperty

from ..dependencies import (
    AuthDB,
)
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token

router = DiracxRouter(require_auth=False)


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

    return await auth_db.get_user_refresh_tokens(subject)


@router.delete("/refresh-tokens/{jti}")
async def revoke_refresh_token(
    auth_db: AuthDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    jti: str,
) -> str:
    """Revoke a refresh token. If the user has the `proxy_management` property, then
    the subject is not used to filter the refresh tokens.
    """
    res = await auth_db.get_refresh_token(jti)
    if not res:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="JTI provided does not exist",
        )

    if PROXY_MANAGEMENT not in user_info.properties and user_info.sub != res["sub"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Cannot revoke a refresh token owned by someone else",
        )
    await auth_db.revoke_refresh_token(jti)
    return f"Refresh token {jti} revoked"


@router.get("/userinfo")
async def userinfo(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)]
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
