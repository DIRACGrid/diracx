"""FastAPI routes for the OAuth 2.0 authorization code flow.

The endpoints initiate authentication with a VO identity provider and complete
the flow when the provider redirects the user back to DiracX. Both steps use
PKCE.

See Also:
    docs/admin/explanations/auth-with-diracx.md: End-to-end authentication flow.
"""

from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Literal

from fastapi import (
    HTTPException,
    Request,
)
from fastapi.responses import RedirectResponse

from diracx.core.exceptions import AuthorizationError, IAMClientError, IAMServerError
from diracx.core.settings import AuthSettings
from diracx.db.sql import AuthDB
from diracx.logic.auth import (
    complete_authorization_flow as complete_authorization_flow_bl,
)
from diracx.logic.auth import (
    initiate_authorization_flow as initiate_authorization_flow_bl,
)
from diracx.routers.dependencies import Config

from ..dependencies import AvailableSecurityProperties
from ..fastapi_classes import DiracxRouter

logger = logging.getLogger(__name__)

router = DiracxRouter(require_auth=False)


@router.get("/authorize")
async def initiate_authorization_flow(
    request: Request,
    response_type: Literal["code"],
    code_challenge: str,
    code_challenge_method: Literal["S256"],
    client_id: str,
    redirect_uri: str,
    scope: str,
    state: str,
    auth_db: AuthDB,
    config: Config,
    available_properties: AvailableSecurityProperties,
    settings: AuthSettings,
) -> RedirectResponse:
    """Start an authorization flow with the configured identity provider.

    The request details are stored before the user is redirected to the VO's
    OpenID provider (such as IAM or CheckIn). Scope may specify a VO, an
    optional group, and optional security properties. If no group is supplied,
    the VO's default group is used. If properties are supplied without a group,
    they are combined with the default group's properties; if both are
    supplied, they are combined with the selected group's properties.

    The user authorization details are stored in a cookie so the flow can be
    associated with the corresponding user authorization request.

    Args:
        request: Incoming request used to construct the callback URL.
        response_type: OAuth response type. Only ``"code"`` is supported.
        code_challenge: PKCE challenge supplied by the client.
        code_challenge_method: PKCE challenge method. Only ``"S256"`` is supported.
        client_id: Identifier of the client requesting authorization.
        redirect_uri: Client URI to redirect to after authorization completes.
        scope: Requested VO, group, and security properties.
        state: Opaque client state to return after authorization completes.
        auth_db: Database used to persist the authorization flow.
        config: DiracX configuration used to select the identity provider.
        available_properties: Security properties available for this request.
        settings: Authentication settings used to validate and configure the flow.

    Returns:
        A redirect response to the VO identity provider.

    Raises:
        HTTPException: If the client ID, redirect URI, or scope is invalid.
    """
    try:
        redirect_uri = await initiate_authorization_flow_bl(
            request_url=f"{request.url.replace(query='')}",
            code_challenge=code_challenge,
            code_challenge_method=code_challenge_method,
            client_id=client_id,
            redirect_uri=redirect_uri,
            scope=scope,
            state=state,
            auth_db=auth_db,
            config=config,
            settings=settings,
            available_properties=available_properties,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e

    return RedirectResponse(redirect_uri)


@router.get("/authorize/complete")
async def complete_authorization_flow(
    code: str,
    state: str,
    request: Request,
    auth_db: AuthDB,
    config: Config,
    settings: AuthSettings,
) -> RedirectResponse:
    """Complete the flow after the identity provider redirects back to DiracX.

    The endpoint retrieves the original flow details from the encrypted state,
    exchanges the provider's authorization code for an ID token, stores that
    token, and redirects the user to the client's redirect URI. The original
    client state is included in the redirect.

    Args:
        code: Authorization code returned by the identity provider.
        state: Encrypted state returned by the identity provider.
        request: Incoming request used to construct the token-exchange callback URL.
        auth_db: Database containing the pending authorization flow.
        config: DiracX configuration used to contact the identity provider.
        settings: Authentication settings used to decrypt state and expire the flow.

    Returns:
        A redirect response to the client with the authorization code and
        original state.

    Raises:
        HTTPException: If the state is invalid (400), the identity provider
            returns a server error (502), or the authorization code is invalid
            (401).
    """
    try:
        redirect_uri = await complete_authorization_flow_bl(
            code=code,
            state=state,
            request_url=str(request.url.replace(query="")),
            auth_db=auth_db,
            config=config,
            settings=settings,
        )
    except AuthorizationError as e:
        logger.warning("Authorization flow failed with invalid state: %s", e)
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail="Invalid state"
        ) from e
    except IAMServerError as e:
        logger.warning("IAM server error during authorization flow: %s", e)
        raise HTTPException(
            status_code=HTTPStatus.BAD_GATEWAY,
            detail="Failed to contact IAM server",
        ) from e
    except IAMClientError as e:
        logger.warning("IAM client error during authorization flow: %s", e)
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED, detail="Invalid code"
        ) from e
    return RedirectResponse(redirect_uri)
