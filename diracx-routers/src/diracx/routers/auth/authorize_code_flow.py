"""Authorization code flow.

See docs/admin/explanations/authentication.md
"""

from __future__ import annotations

import logging
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
        logger.warning("Authorization flow failed with invalid state: %s", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid state"
        ) from e
    except IAMServerError as e:
        logger.warning("IAM server error during authorization flow: %s", e)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to contact IAM server",
        ) from e
    except IAMClientError as e:
        logger.warning("IAM client error during authorization flow: %s", e)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid code"
        ) from e
    return responses.RedirectResponse(redirect_uri)
