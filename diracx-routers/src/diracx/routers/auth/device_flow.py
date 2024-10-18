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

from fastapi import (
    HTTPException,
    Request,
    Response,
    responses,
    status,
)
from fastapi.responses import RedirectResponse
from typing_extensions import TypedDict

from ..dependencies import (
    AuthDB,
    AvailableSecurityProperties,
    Config,
)
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthSettings
from .utils import (
    GrantType,
    decrypt_state,
    get_token_from_iam,
    initiate_authorization_flow_with_iam,
    parse_and_validate_scope,
)

router = DiracxRouter(require_auth=False)


class InitiateDeviceFlowResponse(TypedDict):
    """Response for the device flow initiation."""

    user_code: str
    device_code: str
    verification_uri_complete: str
    verification_uri: str
    expires_in: int


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
    if settings.dirac_client_id != client_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Unrecognised client ID"
        )

    try:
        parse_and_validate_scope(scope, config, available_properties)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=e.args[0],
        ) from e
    except PermissionError as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=e.args[0],
        ) from e

    user_code, device_code = await auth_db.insert_device_flow(client_id, scope)

    verification_uri = str(request.url.replace(query={}))

    return {
        "user_code": user_code,
        "device_code": device_code,
        "verification_uri_complete": f"{verification_uri}?user_code={user_code}",
        "verification_uri": str(request.url.replace(query={})),
        "expires_in": settings.device_flow_expiration_seconds,
    }


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
    # Here we make sure the user_code actually exists
    scope = await auth_db.device_flow_validate_user_code(
        user_code, settings.device_flow_expiration_seconds
    )
    parsed_scope = parse_and_validate_scope(scope, config, available_properties)

    redirect_uri = f"{request.url.replace(query='')}/complete"

    state_for_iam = {
        "grant_type": GrantType.device_code.value,
        "user_code": user_code,
    }

    authorization_flow_url = await initiate_authorization_flow_with_iam(
        config,
        parsed_scope["vo"],
        redirect_uri,
        state_for_iam,
        settings.state_key.fernet,
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
):
    """This the url callbacked by IAM/Checkin after the authorization
    flow was granted.
    It gets us the code we need for the authorization flow, and we
    can map it to the corresponding device flow using the user_code
    in the cookie/session.
    """
    decrypted_state = decrypt_state(state, settings.state_key.fernet)
    assert decrypted_state["grant_type"] == GrantType.device_code

    id_token = await get_token_from_iam(
        config,
        decrypted_state["vo"],
        code,
        decrypted_state,
        str(request.url.replace(query="")),
    )
    await auth_db.device_flow_insert_id_token(
        decrypted_state["user_code"], id_token, settings.device_flow_expiration_seconds
    )

    return responses.RedirectResponse(f"{request.url.replace(query='')}/finished")


@router.get("/device/complete/finished")
def finished(response: Response):
    """This is the final step of the device flow."""
    response.body = b"<h1>Please close the window</h1>"
    response.status_code = 200
    response.media_type = "text/html"
    return response
