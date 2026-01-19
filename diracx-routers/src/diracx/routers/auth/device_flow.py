"""Device flow.

See docs/admin/explanations/authentication.md
"""

from __future__ import annotations

import logging

from fastapi import (
    HTTPException,
    Request,
    Response,
    responses,
    status,
)
from fastapi.responses import RedirectResponse

from diracx.core.exceptions import IAMClientError, IAMServerError
from diracx.core.models import InitiateDeviceFlowResponse
from diracx.logic.auth.device_flow import do_device_flow as do_device_flow_bl
from diracx.logic.auth.device_flow import (
    finish_device_flow as finish_device_flow_bl,
)
from diracx.logic.auth.device_flow import (
    initiate_device_flow as initiate_device_flow_bl,
)

from ..dependencies import (
    AuthDB,
    AuthSettings,
    AvailableSecurityProperties,
    Config,
)
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
    """Initiate the device flow against DIRAC authorization Server.

    Scope details:
    - If only VO is provided: Uses the default group and its properties for the VO.

    - If VO and group are provided: Uses the specified group and its properties for the VO.

    - If VO and properties are provided: Uses the default group and combines its properties with the
      provided properties.

    - If VO, group, and properties are provided: Uses the specified group and combines its properties with the
      provided properties.

    Offers the user to go with the browser to
    `auth/<vo>/device?user_code=XYZ`
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
            status_code=status.HTTP_400_BAD_REQUEST,
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
    """This is called as the verification URI for the device flow.
    It will redirect to the actual OpenID server (IAM, CheckIn) to
    perform a authorization code flow.

    We set the user_code obtained from the device flow in a cookie
    to be able to map the authorization flow with the corresponding
    device flow.
    (note: it can't be put as parameter or in the URL)
    """
    authorization_flow_url = await do_device_flow_bl(
        request_url=str(request.url.replace(query="")),
        auth_db=auth_db,
        user_code=user_code,
        config=config,
        available_properties=available_properties,
        settings=settings,
    )
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
    """This the url callbacked by IAM/CheckIn after the authorization
    flow was granted.
    It gets us the code we need for the authorization flow, and we
    can map it to the corresponding device flow using the user_code
    in the cookie/session.
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
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=e.args[0],
        ) from e
    except IAMClientError as e:
        logger.warning("IAM client error during device flow completion: %s", e)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=e.args[0],
        ) from e

    return responses.RedirectResponse(f"{request_url}/finished")


@router.get("/device/complete/finished")
def finished(response: Response):
    """This is the final step of the device flow."""
    response.body = b"<h1>Please close the window</h1>"
    response.status_code = 200
    response.media_type = "text/html"
    return response
