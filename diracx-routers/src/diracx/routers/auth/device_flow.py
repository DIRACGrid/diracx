"""FastAPI routes for the OAuth 2.0 device authorization flow.

The routes let a client start authorization on a device with limited input,
then let the user authenticate in a browser with the configured VO identity
provider. DiracX associates the browser callback with the pending device flow
and stores the resulting ID token for the client to exchange.

See Also:
    docs/admin/explanations/auth-with-diracx.md: Overview of DiracX authentication flows.
"""

from __future__ import annotations

import logging
from http import HTTPStatus

from fastapi import (
    HTTPException,
    Request,
    Response,
)
from fastapi.responses import RedirectResponse
from sqlalchemy.exc import NoResultFound

from diracx.core.exceptions import IAMClientError, IAMServerError
from diracx.core.models import InitiateDeviceFlowResponse
from diracx.core.settings import AuthSettings
from diracx.db.sql import AuthDB
from diracx.logic.auth import (
    do_device_flow as do_device_flow_bl,
)
from diracx.logic.auth import (
    finish_device_flow as finish_device_flow_bl,
)
from diracx.logic.auth import (
    initiate_device_flow as initiate_device_flow_bl,
)
from diracx.routers.dependencies import Config

from ..dependencies import AvailableSecurityProperties
from ..fastapi_classes import DiracxRouter

logger = logging.getLogger(__name__)

router = DiracxRouter(require_auth=False)


@router.post("/device")
async def initiate_device_flow(
    client_id: str,
    scope: str,
    request: Request,
    auth_db: AuthDB,
    config: Config,
    available_properties: AvailableSecurityProperties,
    settings: AuthSettings,
) -> InitiateDeviceFlowResponse:
    """Start a device authorization flow for a client.

    The scope may request a VO, an optional group, and optional security
    properties. Without a group, the VO's default group is used; requested
    properties are combined with the selected group's properties.

    Args:
        client_id: Identifier of the client requesting authorization.
        scope: Requested VO, group, and security properties.
        request: Incoming request used to construct the verification URI.
        auth_db: Database used to store the pending device flow.
        config: DiracX configuration used to validate the requested scope.
        available_properties: Security properties available for this request.
        settings: Authentication settings used to validate the client and set flow expiry.

    Returns:
        Device and user codes, the browser verification URIs, and the expiry time.

    Raises:
        HTTPException: If the client ID or requested scope is invalid (400).
    """
    try:
        device_flow_response = await initiate_device_flow_bl(
            client_id=client_id,
            scope=scope,
            verification_uri=str(request.url.replace(query={})),
            auth_db=auth_db,
            config=config,
            available_properties=available_properties,
            settings=settings,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e

    return device_flow_response


@router.get("/device")
async def do_device_flow(
    request: Request,
    auth_db: AuthDB,
    user_code: str,
    config: Config,
    available_properties: AvailableSecurityProperties,
    settings: AuthSettings,
) -> RedirectResponse:
    """Serve as the verification URI for the device flow.

    The user opens this URI in a browser and enters the user code. DiracX then
    redirects the browser to the VO identity provider (such as IAM or CheckIn)
    to complete an authorization code flow.

    Args:
        request: Incoming request used to construct the identity-provider callback URI.
        auth_db: Database containing the pending device flow.
        user_code: User code displayed by the client and entered in the browser.
        config: DiracX configuration used to select the identity provider.
        available_properties: Security properties available for this request.
        settings: Authentication settings used to validate and expire the flow.

    Returns:
        A redirect response to the VO identity provider.

    Raises:
        HTTPException: If the user code is invalid, expired, or its scope is invalid (400),
            or if the identity provider is unavailable (502).
    """
    try:
        authorization_flow_url = await do_device_flow_bl(
            request_url=str(request.url.replace(query="")),
            auth_db=auth_db,
            user_code=user_code,
            config=config,
            available_properties=available_properties,
            settings=settings,
        )
    except NoResultFound as e:
        logger.warning("Invalid or expired user_code: %s", e)
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e
    except ValueError as e:
        logger.warning("Invalid scope during device flow: %s", e)
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e
    except IAMServerError as e:
        logger.warning("IAM server error during device flow: %s", e)
        raise HTTPException(
            status_code=HTTPStatus.BAD_GATEWAY,
            detail=str(e),
        ) from e
    return RedirectResponse(authorization_flow_url)


@router.get("/device/complete")
async def finish_device_flow(
    request: Request,
    code: str,
    state: str,
    auth_db: AuthDB,
    config: Config,
    settings: AuthSettings,
) -> RedirectResponse:
    """Complete the browser callback from the VO identity provider.

    DiracX exchanges the authorization code for an ID token, associates it
    with the pending device flow, and redirects the browser to the finished
    page. The original device client can then exchange its device code for
    DiracX tokens.

    Args:
        request: Incoming request used to construct the token-exchange callback URI.
        code: Authorization code returned by the identity provider.
        state: Encrypted state used to identify the pending device flow.
        auth_db: Database containing the pending device flow.
        config: DiracX configuration used to contact the identity provider.
        settings: Authentication settings used to decrypt state and expire the flow.

    Returns:
        A redirect response to the device-flow finished page.

    Raises:
        HTTPException: If the identity provider returns a server error (502) or
            the authorization code is invalid (401).
    """
    request_url = str(request.url.replace(query={}))

    try:
        await finish_device_flow_bl(
            request_url,
            code,
            state,
            auth_db,
            config,
            settings,
        )
    except IAMServerError as e:
        logger.warning("IAM server error during device flow completion: %s", e)
        raise HTTPException(
            status_code=HTTPStatus.BAD_GATEWAY,
            detail=str(e),
        ) from e
    except IAMClientError as e:
        logger.warning("IAM client error during device flow completion: %s", e)
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail=str(e),
        ) from e

    return RedirectResponse(f"{request_url}/finished")


@router.get("/device/complete/finished")
def finished(response: Response):
    """Display the completion page after the device flow.

    Args:
        response: Response object populated with the completion page.

    Returns:
        An HTML response asking the user to close the browser window.
    """
    response.body = b"<h1>Please close the window</h1>"
    response.status_code = 200
    response.media_type = "text/html"
    return response
