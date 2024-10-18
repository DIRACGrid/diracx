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

from typing import Literal

from fastapi import (
    HTTPException,
    Request,
    responses,
    status,
)

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


@router.get("/authorize")
async def authorization_flow(
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
):
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
    if settings.dirac_client_id != client_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Unrecognised client ID"
        )
    if redirect_uri not in settings.allowed_redirects:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Unrecognised redirect_uri"
        )

    try:
        parsed_scope = parse_and_validate_scope(scope, config, available_properties)
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

    # Store the authorization flow details
    uuid = await auth_db.insert_authorization_flow(
        client_id,
        scope,
        code_challenge,
        code_challenge_method,
        redirect_uri,
    )

    # Initiate the authorization flow with the IAM
    state_for_iam = {
        "external_state": state,
        "uuid": uuid,
        "grant_type": GrantType.authorization_code.value,
    }

    authorization_flow_url = await initiate_authorization_flow_with_iam(
        config,
        parsed_scope["vo"],
        f"{request.url.replace(query='')}/complete",
        state_for_iam,
        settings.state_key.fernet,
    )

    return responses.RedirectResponse(authorization_flow_url)


@router.get("/authorize/complete")
async def authorization_flow_complete(
    code: str,
    state: str,
    request: Request,
    auth_db: AuthDB,
    config: Config,
    settings: AuthSettings,
):
    """Complete the authorization flow.

    The user is redirected back to the DIRAC auth service after completing the IAM's authorization flow.
    We retrieve the original flow details from the decrypted state and store the ID token requested from the IAM.
    The user is then redirected to the client's redirect URI.
    """
    # Decrypt the state to access user details
    decrypted_state = decrypt_state(state, settings.state_key.fernet)
    assert decrypted_state["grant_type"] == GrantType.authorization_code

    # Get the ID token from the IAM
    id_token = await get_token_from_iam(
        config,
        decrypted_state["vo"],
        code,
        decrypted_state,
        str(request.url.replace(query="")),
    )

    # Store the ID token and redirect the user to the client's redirect URI
    code, redirect_uri = await auth_db.authorization_flow_insert_id_token(
        decrypted_state["uuid"],
        id_token,
        settings.authorization_flow_expiration_seconds,
    )

    return responses.RedirectResponse(
        f"{redirect_uri}?code={code}&state={decrypted_state['external_state']}"
    )
