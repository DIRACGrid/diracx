"""Authorization code flow.

Client Application                   DIRAC Auth Service                IAM (Identity Access Management)
------------------                   ------------------                --------------------------------
       |                                     |                                         |
       |---(A)--- Authorization Request ---->|                                         |
       |                                     |---(B)--- Authorization Request -------->|
       |                                     |                                         |
       |                                     |<--(C)--- Authorization Grant -----------|
       |                                     |                                         |
       |                                     |---(D)--- ID Token Request ------------->|
       |                                     |                                         |
       |                                     |<--(E)--- ID Token ----------------------|
       |<--(F)--- Authorization Grant -------|                                         |
       |                                     |                                         |
       |---(G)--- Access Token Request ----->|                                         |
       |                                     |                                         |
       |<--(H)--- Access Token --------------|                                         |
       |                                     |                                         |


* (A) The flow is initiated by the client making a request to the DIRAC auth service (GET /authorize) with PKCE.
* (B) The DIRAC auth service stores the request details and redirects the user to the IAM's authorization flow
by performing an authorization request on behalf of the user.
* (C) Once done, the IAM redirects the user back to the DIRAC auth service (GET /authorize/complete).
* (D) The DIRAC auth service exchanges the code for an ID token by making a token request to the IAM.
* (E) The IAM responds with an access, a refresh and an ID tokens.
* (F) The DIRAC auth service only stores the ID token and redirects the user to the client's redirect URI.
* (G) The client requests an access token from the DIRAC auth service by making a request to
the token endpoint (POST /token).
* (H) The DIRAC auth service responds with a DIRAC access token, based on the ID token.
* The client can then use the access token to access the DIRAC services.
"""

from __future__ import annotations

from typing import Literal

from fastapi import (
    HTTPException,
    Request,
    responses,
    status,
)

from diracx.core.exceptions import AuthorizationError, IAMClientError, IAMServerError
from diracx.logic.auth.authorize_code_flow import (
    complete_authorization_flow as complete_authorization_flow_bl,
)
from diracx.logic.auth.authorize_code_flow import (
    initiate_authorization_flow as initiate_authorization_flow_bl,
)

from ..dependencies import (
    AuthDB,
    AuthSettings,
    AvailableSecurityProperties,
    Config,
)
from ..fastapi_classes import DiracxRouter

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
) -> responses.RedirectResponse:
    """Initiate the authorization flow.
    It will redirect to the actual OpenID server (IAM, CheckIn) to
    perform a authorization code flow.

    Scope details:
    - If only VO is provided: Uses the default group and its properties for the VO.

    - If VO and group are provided: Uses the specified group and its properties for the VO.

    - If VO and properties are provided: Uses the default group and combines its properties with the
      provided properties.

    - If VO, group, and properties are provided: Uses the specified group and combines its properties with the
      provided properties.

    We set the user details obtained from the user authorize flow in a cookie
    to be able to map the authorization flow with the corresponding
    user authorize flow.
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
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e

    return responses.RedirectResponse(redirect_uri)


@router.get("/authorize/complete")
async def complete_authorization_flow(
    code: str,
    state: str,
    request: Request,
    auth_db: AuthDB,
    config: Config,
    settings: AuthSettings,
) -> responses.RedirectResponse:
    """Complete the authorization flow.

    The user is redirected back to the DIRAC auth service after completing the IAM's authorization flow.
    We retrieve the original flow details from the decrypted state and store the ID token requested from the IAM.
    The user is then redirected to the client's redirect URI.
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid state"
        ) from e
    except IAMServerError as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to contact IAM server",
        ) from e
    except IAMClientError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid code"
        ) from e
    return responses.RedirectResponse(redirect_uri)
