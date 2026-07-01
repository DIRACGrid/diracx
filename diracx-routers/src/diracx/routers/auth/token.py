"""Token endpoint."""

from __future__ import annotations

import logging
import os
from http import HTTPStatus
from typing import Annotated, Literal

from fastapi import Depends, Form, Header, HTTPException
from joserfc.errors import JoseError

from diracx.core.exceptions import (
    DiracHttpResponseError,
    InvalidCredentialsError,
    PendingAuthorizationError,
)
from diracx.core.models import (
    AccessTokenPayload,
    GrantType,
    RefreshTokenPayload,
    TokenResponse,
)
from diracx.core.settings import AuthSettings
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
    """Enrich token payloads with policy-specific content and mint tokens.

    This utility applies all configured `BaseAccessPolicy` implementations to
    the access and refresh payloads, embeds any policy-specific claims, and
    creates signed token strings using the application's settings.

    Args:
        access_payload (AccessTokenPayload): Payload used to create the
            access token.
        refresh_payload (RefreshTokenPayload | None): Optional payload used
            to create the refresh token. If omitted, ``existing_refresh_token``
            must be provided.
        existing_refresh_token (str | None): An existing refresh token string
            to reuse when ``refresh_payload`` is not supplied.
        all_access_policies (dict[str, BaseAccessPolicy]): Mapping of policy
            name to policy instance; each policy may enrich token payloads.
        settings (AuthSettings): Authentication settings used during token
            creation (signing keys, expiry, etc.).

    Returns:
        TokenResponse: Object containing the minted ``access_token``,
            ``expires_in`` and ``refresh_token`` values.

    Raises:
        HTTPException: If neither ``refresh_payload`` nor
            ``existing_refresh_token`` is provided (HTTP 500).
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
    """Token endpoint to exchange grant artifacts for tokens.

    Implements the OAuth2 `/token` endpoint supporting a limited set of
    grant types used by DIRACX: authorization code, device code, and
    refresh token. The endpoint delegates grant validation to the business
    logic layer and then calls ``mint_token`` to produce the final
    ``TokenResponse``.

    Args:
        grant_type (GrantType): The OAuth2 grant type (authorization_code,
            device_code, or refresh_token).
        client_id (str): Registered OAuth2 client identifier.
        auth_db (AuthDB): Database accessor used by business logic.
        config (Config): Application configuration object.
        settings (AuthSettings): Authentication-related settings.
        available_properties (AvailableSecurityProperties): Security
            properties available to the client/user.
        all_access_policies (dict[str, BaseAccessPolicy]): Access policies to
            apply when minting tokens.
        device_code (str | None): Device flow code when using the device grant.
        code (str | None): Authorization code when using the authorization
            code grant.
        redirect_uri (str | None): Redirect URI used with the authorization
            code grant.
        code_verifier (str | None): PKCE code verifier for authorization-code
            exchanges from public clients.
        refresh_token (str | None): Refresh token for the refresh-token grant.

    Returns:
        TokenResponse: Contains the newly minted access token (and refresh
            token when applicable) and expiry information.

    Raises:
        DiracHttpResponseError: When the grant is pending (maps to a
            400 with ``error=authorization_pending`` for device flows).
        HTTPException: For invalid requests (HTTP 400), invalid credentials
            (HTTP 401), or insufficient permissions (HTTP 403).
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
    except PendingAuthorizationError as e:
        raise DiracHttpResponseError(
            status_code=HTTPStatus.BAD_REQUEST,
            data={"error": "authorization_pending"},
        ) from e
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
    config: Config,
    all_access_policies: Annotated[
        dict[str, BaseAccessPolicy], Depends(BaseAccessPolicy.all_used_access_policies)
    ],
    expires_minutes: int | None = None,
) -> TokenResponse:
    """Legacy proxy-to-token exchange endpoint used by older DIRAC clients.

    This helper allows legacy proxies to exchange a bearer-like header
    (matching ``LEGACY_EXCHANGE_PATTERN``) for modern access/refresh tokens.
    The route is gated by the environment variable
    ``DIRACX_LEGACY_EXCHANGE_HASHED_API_KEY``; if unset the endpoint returns
    HTTP 503 (service unavailable).

    Args:
        preferred_username (str): Username asserted by the legacy proxy.
        scope (str): Requested scope string.
        authorization (str): Authorization header value provided by the
            legacy proxy (must match the expected hashed API key format).
        auth_db (AuthDB): Database accessor used by business logic.
        available_properties (AvailableSecurityProperties): Security
            properties used to resolve the requested scope.
        settings (AuthSettings): Authentication-related settings.
        config (Config): Application configuration object.
        all_access_policies (dict[str, BaseAccessPolicy]): Access policies to
            apply when minting tokens.
        expires_minutes (int | None): Optional override for refresh token
            expiry in minutes.

    Returns:
        TokenResponse: Contains minted access and (optionally) refresh tokens.

    Raises:
        HTTPException: When the legacy exchange is disabled (HTTP 503), on
            invalid input (HTTP 400), invalid credentials (HTTP 401), or
            insufficient permissions (HTTP 403).
    """
    if not (
        expected_api_key := os.environ.get("DIRACX_LEGACY_EXCHANGE_HASHED_API_KEY")
    ):
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
