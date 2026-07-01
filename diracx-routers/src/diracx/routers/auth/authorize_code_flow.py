"""Authorization code flow.

See docs/admin/explanations/authentication.md
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
    """Initiate an OAuth2 authorization-code flow by redirecting to the IAM.

    Builds the authorization request and returns a `RedirectResponse` that
    routes the user agent to the configured identity provider (IAM/CheckIn)
    to authenticate and authorize the client.

    Scope resolution behavior:
    - If only VO is provided: uses the VO's default group and its properties.
    - If VO and group are provided: uses the specified group and its properties.
    - If VO and properties are provided: uses the default group and merges its
      properties with the provided properties.
    - If VO, group, and properties are provided: uses the specified group and
      merges its properties with the provided properties.

    The implementation stores transient flow details in a cookie so the
    authorization response (returned to ``/authorize/complete``) can be
    correlated back to the initiating request.

    Args:
        request (Request): Incoming FastAPI request; used to compute the
            ``request_url`` passed to the business logic.
        response_type (Literal["code"]): OAuth2 response type (must be
            ``"code"`` for the authorization-code flow).
        code_challenge (str): PKCE code challenge (base64url-encoded).
        code_challenge_method (Literal["S256"]): PKCE method (must be
            ``"S256"``).
        client_id (str): Client identifier registered in the IAM.
        redirect_uri (str): Client redirect URI to return the user to after
            successful authentication.
        scope (str): OAuth2 scope string; may contain VO/group/property
            information as described above.
        state (str): Opaque state value used to correlate requests and
            mitigate CSRF attacks.
        auth_db (AuthDB): Database accessor for temporary authorization state.
        config (Config): Application configuration object.
        available_properties (AvailableSecurityProperties): Available
            security properties used to resolve requested scope.
        settings (AuthSettings): Authentication-related settings.

    Returns:
        RedirectResponse: A redirect to the identity provider's authorization
            endpoint.

    Raises:
        HTTPException: If input validation or business-logic validation fails
            (returns HTTP 400 with details).
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
    """Complete the OAuth2 authorization-code flow and persist the ID token.

    This endpoint is the redirect target for the identity provider. It
    restores the original authorization flow context from the encrypted
    ``state``, exchanges the authorization ``code`` for tokens (ID token /
    access token) via the business logic layer, persists the retrieved ID
    token and related state, and finally redirects the user agent to the
    client's configured redirect URI.

    Args:
        code (str): Authorization code issued by the identity provider.
        state (str): Opaque encrypted state previously created by
            ``initiate_authorization_flow`` to recover the original context.
        request (Request): Incoming FastAPI request; used to compute the
            ``request_url`` passed to the business logic.
        auth_db (AuthDB): Database accessor for authorization state and
            tokens.
        config (Config): Application configuration object.
        settings (AuthSettings): Authentication-related settings.

    Returns:
        RedirectResponse: A redirect to the client's redirect URI on success.

    Raises:
        HTTPException: Raised with different status codes depending on the
            failure mode:
            - HTTP 400: Invalid or tampered ``state`` (mapped from
              ``AuthorizationError``).
            - HTTP 502: IAM server-side error while exchanging the code
              (mapped from ``IAMServerError``).
            - HTTP 401: Invalid authorization ``code`` or IAM client error
              (mapped from ``IAMClientError``).
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
