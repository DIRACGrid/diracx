"""Module for auth management endpoints.

These endpoints are used to manage the user's authentication tokens and
to get information about the user's identity.
"""

from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Annotated, Any

from fastapi import Depends, Form, HTTPException
from joserfc.errors import DecodeError
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


class UserInfoResponse(TypedDict):
    """Typed response describing an authenticated user's identity.

    Attributes:
        sub (str): Subject identifier for the user.
        vo (str): Virtual organization (VO) the user belongs to.
        dirac_group (str): DIRAC group of the user.
        policies (dict[str, Any]): Effective policies attached to the user.
        properties (list[SecurityProperty]): Security properties available to the user.
        preferred_username (str): Display username for the user.
    """

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
    """Retrieve refresh tokens visible to the caller.

    Returns all refresh-token records associated with the authenticated
    subject. If the caller has the ``proxy_management`` property the
    subject filter is omitted and tokens for all subjects are returned.

    Args:
        auth_db (AuthDB): Database accessor for refresh/token records.
        user_info (AuthorizedUserInfo): Authenticated user information from
            the validated DIRAC access token.

    Returns:
        list: A list of refresh-token records (implementation-specific
            dict-like objects) visible to the caller.
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
    """Revoke a refresh token (RFC 7009-style revocation endpoint).

    Attempts to revoke the provided refresh token. The endpoint attempts
    to follow RFC 7009 semantics: it accepts a refresh token, an optional
    ``token_type_hint``, and the ``client_id`` of the caller. Token format
    errors are logged but do not expose details to the caller.

    Args:
        auth_db (AuthDB): Database accessor for token/authorization state.
        settings (AuthSettings): Authentication-related settings.
        token (str): The refresh token to revoke (submitted as form data).
        token_type_hint (Optional[str]): Optional hint for the token type.
        client_id (str): Client identifier of the application requesting
            revocation (submitted as form data).

    Returns:
        str: Success message "Refresh token revoked" on completion.

    Raises:
        HTTPException: On invalid input (HTTP 400) or when credentials are
            invalid (HTTP 401). Token decode/format errors are logged but
            do not raise an exception to the caller.
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
    """Revoke a refresh token identified by its JTI (JWT ID).

    Revokes the refresh token whose JTI (UUID) is provided in the path.
    If the caller has the ``proxy_management`` property the subject filter
    is omitted and the operation may affect tokens across subjects.

    Args:
        auth_db (AuthDB): Database accessor for token/authorization state.
        user_info (AuthorizedUserInfo): Authenticated user information from
            the validated DIRAC access token.
        jti (str): UUID string (JTI) of the refresh token to revoke.

    Returns:
        str: Success message "Refresh token revoked" on completion.

    Raises:
        HTTPException: On invalid input (HTTP 400), insufficient permissions
            (HTTP 403), or when the token cannot be found (HTTP 404).
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
    """Return identity information for the authenticated user.

    The response contains the subject identifier, VO, DIRAC group,
    effective policies, security properties, and preferred username. This
    mirrors the standard OpenID Connect ``userinfo`` semantics adapted to
    DIRAC-specific attributes.

    Args:
        user_info (AuthorizedUserInfo): Authenticated user information
            resolved from the validated access token.

    Returns:
        UserInfoResponse: Dictionary-like response containing identity
            attributes for the user.
    """
    return {
        "sub": user_info.sub,
        "vo": user_info.vo,
        "dirac_group": user_info.dirac_group,
        "properties": user_info.properties,
        "policies": user_info.policies,
        "preferred_username": user_info.preferred_username,
    }
