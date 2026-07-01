"""Device flow.

See docs/admin/explanations/authentication.md
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
    """Initiate the OAuth2 device authorization flow.

    Starts the device flow for a public client by contacting the identity
    provider and returning the verification information needed by the
    device (user code, verification URI, and expiry). The returned
    ``InitiateDeviceFlowResponse`` is intended for consumption by a
    headless device which will instruct the end user to visit the
    verification URI and enter the displayed user code.

    Scope resolution behavior:
    - If only VO is provided: uses the VO's default group and its properties.
    - If VO and group are provided: uses the specified group and its properties.
    - If VO and properties are provided: uses the default group and merges its
        properties with the provided properties.
    - If VO, group, and properties are provided: uses the specified group and
        merges its properties with the provided properties.

    The verification URI typically points to the ``/device`` endpoint which
    renders a form for the user to enter the provided ``user_code``.

    Args:
        client_id (str): OAuth2 client identifier initiating the device flow.
        scope (str): Requested scope string; may include VO/group/property
            qualifiers as described above.
        request (Request): FastAPI request used to compute the ``verification_uri``.
        auth_db (AuthDB): Database accessor for temporary device flow state.
        config (Config): Application configuration object.
        available_properties (AvailableSecurityProperties): Available
            security properties used to resolve requested scope.
        settings (AuthSettings): Authentication-related settings.

    Returns:
        InitiateDeviceFlowResponse: Contains user code, verification URI, and
            other parameters the device needs to complete the flow.

    Raises:
        HTTPException: On invalid input (returns HTTP 400 with validation details).
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
            detail=e.args[0],
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
    """Serve the device verification UI and redirect to the IAM.

    This endpoint is the verification URI that the end user visits in a
    browser. It establishes a short-lived browser-side session (cookie)
    that stores the device ``user_code`` and then redirects the user to the
    identity provider to perform an interactive authorization (authorization
    code flow). The server uses the stored ``user_code`` to correlate the
    interactive authorization with the waiting device.

    Args:
        request (Request): FastAPI request; used to build redirect URLs.
        auth_db (AuthDB): Database accessor for device flow state and lookups.
        user_code (str): The user code issued to the device that the user
            must enter; stored server-side (cookie) for correlation.
        config (Config): Application configuration object.
        available_properties (AvailableSecurityProperties): Available
            security properties used to resolve requested scope.
        settings (AuthSettings): Authentication-related settings.

    Returns:
        RedirectResponse: Redirects the user agent to the identity provider's
            authorization endpoint.

    Raises:
        HTTPException: On invalid or expired ``user_code`` (HTTP 400), invalid
            scope (HTTP 400), or IAM server errors (HTTP 502).
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
            detail=e.args[0],
        ) from e
    except ValueError as e:
        logger.warning("Invalid scope during device flow: %s", e)
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=e.args[0],
        ) from e
    except IAMServerError as e:
        logger.warning("IAM server error during device flow: %s", e)
        raise HTTPException(
            status_code=HTTPStatus.BAD_GATEWAY,
            detail=e.args[0],
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
    """Complete the interactive step of the device flow and finalize tokens.

    The identity provider redirects the user's browser to this callback
    after the interactive authorization. This handler exchanges the provided
    authorization ``code`` for tokens (via the business logic), maps the
    tokens to the waiting device (using the server-side ``user_code`` state),
    and persists any required tokens/state.

    Args:
        request (Request): FastAPI request used to compute the base request URL.
        code (str): Authorization code returned by the identity provider.
        state (str): State value used to validate and restore context.
        auth_db (AuthDB): Database accessor for device flow state and tokens.
        config (Config): Application configuration object.
        settings (AuthSettings): Authentication-related settings.

    Returns:
        RedirectResponse: Redirects the user agent to a local "finished"
            page on success.

    Raises:
        HTTPException: On IAM server errors (HTTP 502) or IAM client errors
            such as invalid code (HTTP 401).
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
            detail=e.args[0],
        ) from e
    except IAMClientError as e:
        logger.warning("IAM client error during device flow completion: %s", e)
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail=e.args[0],
        ) from e

    return RedirectResponse(f"{request_url}/finished")


@router.get("/device/complete/finished")
def finished(response: Response):
    """Render a simple final page instructing the user to close the window.

    This synchronous endpoint returns a simple HTML response indicating the
    interactive portion of the device flow has finished and the user may
    close their browser window.

    Args:
        response (Response): FastAPI response object to populate.

    Returns:
        Response: HTML response with a short message.
    """
    response.body = b"<h1>Please close the window</h1>"
    response.status_code = 200
    response.media_type = "text/html"
    return response
