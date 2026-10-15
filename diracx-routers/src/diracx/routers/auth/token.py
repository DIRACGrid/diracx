"""FastAPI endpoints for issuing DiracX access and refresh tokens.

The standard token route exchanges authorization-code, device-code, or refresh
token grants. A separate legacy route supports token exchange from legacy
DIRAC installations when that integration is enabled.
"""

from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Annotated, Literal

from fastapi import Depends, Form, Header, HTTPException
from fastapi.responses import JSONResponse
from joserfc.errors import JoseError

from diracx.core.exceptions import (
    InvalidCredentialsError,
    PendingAuthorizationError,
)
from diracx.core.models import (
    AccessTokenPayload,
    GrantType,
    RefreshTokenPayload,
    TokenResponse,
)
from diracx.core.settings import AuthSettings, FactorySettings
from diracx.db.sql import AuthDB
from diracx.logic.auth import create_token
from diracx.logic.auth import get_oidc_token as get_oidc_token_bl
from diracx.logic.auth import (
    perform_legacy_exchange as perform_legacy_exchange_bl,
)
from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.dependencies import Config

from ..dependencies import AvailableSecurityProperties
from ..fastapi_classes import DiracxRouter

router = DiracxRouter(require_auth=False)
logger = logging.getLogger(__name__)


async def mint_token(
    access_payload: AccessTokenPayload,
    refresh_payload: RefreshTokenPayload | None,
    existing_refresh_token: str | None,
    all_access_policies: dict[str, BaseAccessPolicy],
    settings: AuthSettings,
) -> TokenResponse:
    """Apply access-policy data and create the token response.

    Each configured access policy may add claims to the access or refresh
    payload before the tokens are signed.

    Args:
        access_payload: Claims to include in the access token.
        refresh_payload: Claims to include in a newly created refresh token, if applicable.
        existing_refresh_token: Existing refresh token to return when no new payload is created.
        all_access_policies: Policies that can enrich token payloads.
        settings: Authentication settings used to sign tokens and set access-token expiry.

    Returns:
        Access token, expiry duration, and refresh token.

    Raises:
        HTTPException: If neither a refresh payload nor an existing refresh token is provided (500).
    """
    if not refresh_payload and not existing_refresh_token:
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail="Refresh token is not set and no refresh token was provided",
        )

    # Enrich the token with policy specific content
    dirac_access_policies = {}
    dirac_refresh_policies = {}
    for policy_name, policy in all_access_policies.items():
        access_extra, refresh_extra = policy.enrich_tokens(
            access_payload, refresh_payload
        )
        if access_extra:
            dirac_access_policies[policy_name] = access_extra
        if refresh_extra:
            dirac_refresh_policies[policy_name] = refresh_extra

    # Create the access token
    access_payload.dirac_policies = dirac_access_policies
    access_token = create_token(access_payload, settings)

    # Create the refresh token
    if refresh_payload:
        refresh_payload.dirac_policies = dirac_refresh_policies
        refresh_token = create_token(refresh_payload, settings)
    elif existing_refresh_token:
        refresh_token = existing_refresh_token

    return TokenResponse(
        access_token=access_token,
        expires_in=settings.access_token_expire_minutes * 60,
        refresh_token=refresh_token,
    )


@router.post("/token")
async def get_oidc_token(
    # Autorest does not support the GrantType annotation
    # We need to specify each option with Literal[]
    grant_type: Annotated[
        Literal[GrantType.authorization_code]
        | Literal[GrantType.device_code]
        | Literal[GrantType.refresh_token],
        Form(description="OAuth2 Grant type"),
    ],
    client_id: Annotated[str, Form(description="OAuth2 client id")],
    auth_db: AuthDB,
    config: Config,
    settings: AuthSettings,
    available_properties: AvailableSecurityProperties,
    all_access_policies: Annotated[
        dict[str, BaseAccessPolicy], Depends(BaseAccessPolicy.all_used_access_policies)
    ],
    device_code: Annotated[
        str | None, Form(description="device code for OAuth2 device flow")
    ] = None,
    code: Annotated[
        str | None, Form(description="Code for OAuth2 authorization code flow")
    ] = None,
    redirect_uri: Annotated[
        str | None,
        Form(description="redirect_uri used with OAuth2 authorization code flow"),
    ] = None,
    code_verifier: Annotated[
        str | None,
        Form(
            description="Verifier for the code challenge for the OAuth2 authorization flow with PKCE"
        ),
    ] = None,
    refresh_token: Annotated[
        str | None,
        Form(description="Refresh token used with OAuth2 refresh token flow"),
    ] = None,
) -> TokenResponse:
    """Exchange an OAuth grant for DiracX tokens.

    Supports authorization-code, device-code, and refresh-token grants. A
    device-code request may return ``authorization_pending`` until the user
    completes browser authorization.

    Args:
        grant_type: OAuth grant type to exchange.
        client_id: Identifier of the requesting client.
        auth_db: Database used to retrieve or update authorization and token records.
        config: DiracX configuration used to validate users, groups, and scopes.
        settings: Authentication settings used to validate grants and create tokens.
        available_properties: Security properties available for the request.
        all_access_policies: Policies that can enrich the issued token payloads.
        device_code: Device code for a device-code grant.
        code: Authorization code for an authorization-code grant.
        redirect_uri: Redirect URI associated with the authorization-code grant.
        code_verifier: PKCE verifier for the authorization-code grant.
        refresh_token: Refresh token for a refresh-token grant.

    Returns:
        DiracX access and refresh tokens with the access-token expiry duration.

    Raises:
        HTTPException: If the grant is invalid (400), credentials are invalid
            (401), the user lacks the requested permissions (403), or token
            creation fails (500).
    """
    try:
        access_payload, refresh_payload = await get_oidc_token_bl(
            grant_type,
            client_id,
            auth_db,
            config,
            settings,
            available_properties,
            device_code=device_code,
            code=code,
            redirect_uri=redirect_uri,
            code_verifier=code_verifier,
            refresh_token=refresh_token,
        )
    except PendingAuthorizationError:
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content={"error": "authorization_pending"},
        )
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e
    except (
        InvalidCredentialsError,
        JoseError,
    ) as e:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        ) from e
    except PermissionError as e:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail=str(e),
        ) from e
    return await mint_token(
        access_payload, refresh_payload, refresh_token, all_access_policies, settings
    )


BASE_64_URL_SAFE_PATTERN = (
    r"(?:[A-Za-z0-9\-_]{4})*(?:[A-Za-z0-9\-_]{2}==|[A-Za-z0-9\-_]{3}=)?"
)
LEGACY_EXCHANGE_PATTERN = rf"Bearer diracx:legacy:({BASE_64_URL_SAFE_PATTERN})"


@router.get("/legacy-exchange", include_in_schema=False)
async def perform_legacy_exchange(
    preferred_username: str,
    scope: str,
    authorization: Annotated[str, Header()],
    auth_db: AuthDB,
    available_properties: AvailableSecurityProperties,
    settings: AuthSettings,
    factory_settings: FactorySettings,
    config: Config,
    all_access_policies: Annotated[
        dict[str, BaseAccessPolicy], Depends(BaseAccessPolicy.all_used_access_policies)
    ],
    expires_minutes: int | None = None,
) -> TokenResponse:
    """Exchange a legacy DIRAC proxy credential for DiracX tokens.

    The route is hidden from the OpenAPI schema and is disabled unless a
    hashed legacy-exchange API key is configured.

    Args:
        preferred_username: Username of the user represented by the legacy proxy.
        scope: Requested VO, group, and security properties.
        authorization: Encoded legacy credential supplied in the Authorization header.
        auth_db: Database used to store the issued refresh token.
        available_properties: Security properties available for the request.
        settings: Authentication settings used to create tokens.
        factory_settings: Factory settings containing the optional hashed API key.
        config: DiracX configuration used to resolve the user and validate the scope.
        all_access_policies: Policies that can enrich the issued token payloads.
        expires_minutes: Optional refresh-token lifetime override.

    Returns:
        DiracX access and refresh tokens with the access-token expiry duration.

    Raises:
        HTTPException: If the route is disabled (503), the scope or username is
            invalid (400), credentials are invalid (401), or the user lacks the
            requested permissions (403).
    """
    if not (expected_api_key := factory_settings.legacy_exchange_hashed_api_key):
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,
            detail="Legacy exchange is not enabled",
        )

    try:
        access_payload, refresh_payload = await perform_legacy_exchange_bl(
            expected_api_key=expected_api_key,
            preferred_username=preferred_username,
            scope=scope,
            authorization=authorization,
            auth_db=auth_db,
            available_properties=available_properties,
            settings=settings,
            config=config,
            expires_minutes=expires_minutes,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e
    except InvalidCredentialsError as e:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        ) from e
    except PermissionError as e:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail=str(e),
        ) from e
    return await mint_token(
        access_payload, refresh_payload, None, all_access_policies, settings
    )
