"""Device flow.

Client Device                      DIRAC Auth Service                  IAM (Identity Access Management)
-------------                      ------------------                  --------------------------------
     |                                     |                                         |
     |---(A)--- Device Authorization ----->|                                         |
     |                                     |                                         |
     |<--(B)--- User Code, Device Code ----|                                         |
     |                                     |                                         |
User |                                     |                                         |
-----|-------------------------------------|-----------------------------------------|
     |                                     |                                         |
     |   (C) Enter User Code in Browser    |                                         |
     |------------------------------------>|                                         |
     |                                     |---(D)--- Authorization Request -------->|
     |                                     |                                         |
     |                                     |<--(E)--- Authorization Grant -----------|
     |                                     |                                         |
     |                                     |---(F)--- ID Token Request ------------->|
     |                                     |                                         |
     |                                     |<--(G)--- ID Token ----------------------|
     |                                     |                                         |
     |<--(H)--- Device Grant --------------|                                         |
     |                                     |                                         |
-----|-------------------------------------|-----------------------------------------|
     |                                     |                                         |
     |---(I)--- Access Token Request ----->|                                         |
     |                                     |                                         |
     |<--(J)--- Access Token --------------|                                         |
     |                                     |                                         |


* (A) The device flow is initiated by the client, which is typically a device with
limited input capabilities (POST /device).
* (B) The DIRAC auth service responds with a user code and a device code.
* The client then displays the user code to the user and instructs the user to go to
a verification URI on another device
(e.g. a smartphone or computer) and enter the user code.
* (C) The user then authenticates on the other device (GET /device).
In the meantime, the client polls the DIRAC authorization server to check if the user has authorized the client.
* (D) The DIRAC auth service stores the request details and redirects the user to the IAM's authorization flow
by performing an authorization request on behalf of the user.
* (E) Once done, the IAM redirects the user back to the DIRAC auth service (GET /device/complete).
* (F) The DIRAC auth service exchanges the code for an ID token by making a
token request to the IAM.
* (G) The IAM responds with an access, a refresh and an ID tokens.
* (H) The DIRAC auth service stores the ID token and redirects the user to
the redirect URI (GET /device/complete/finished).
* (I) The client requests an access token from the DIRAC auth service by making a request to
the token endpoint (POST /token).
* (J) The DIRAC auth service responds with a DIRAC access token, based on the ID token.
* The client can then use the access token to access the DIRAC services.
"""

from __future__ import annotations

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
    """This the url callbacked by IAM/Checkin after the authorization
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
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=e.args[0],
        ) from e
    except IAMClientError as e:
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
