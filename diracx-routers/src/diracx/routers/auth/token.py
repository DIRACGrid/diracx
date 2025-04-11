"""Token endpoint."""

from __future__ import annotations

import os
from typing import Annotated, Literal

from authlib.jose import JoseError
from fastapi import Depends, Form, Header, HTTPException, status

from diracx.core.exceptions import (
    DiracHttpResponseError,
    ExpiredFlowError,
    InvalidCredentialsError,
    PendingAuthorizationError,
)
from diracx.core.models import (
    AccessTokenPayload,
    GrantType,
    RefreshTokenPayload,
    TokenResponse,
)
from diracx.logic.auth.token import create_token
from diracx.logic.auth.token import get_oidc_token as get_oidc_token_bl
from diracx.logic.auth.token import (
    perform_legacy_exchange as perform_legacy_exchange_bl,
)
from diracx.routers.access_policies import BaseAccessPolicy

from ..dependencies import AuthDB, AuthSettings, AvailableSecurityProperties, Config
from ..fastapi_classes import DiracxRouter

router = DiracxRouter(require_auth=False)


async def mint_token(
    access_payload: AccessTokenPayload,
    refresh_payload: RefreshTokenPayload | None,
    existing_refresh_token: str | None,
    all_access_policies: dict[str, BaseAccessPolicy],
    settings: AuthSettings,
) -> TokenResponse:
    """Enrich the token with policy specific content and mint it."""
    if not refresh_payload and not existing_refresh_token:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
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
    access_payload["dirac_policies"] = dirac_access_policies
    access_token = create_token(access_payload, settings)

    # Create the refresh token
    if refresh_payload:
        refresh_payload["dirac_policies"] = dirac_refresh_policies
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
    """Token endpoint to retrieve the token at the end of a flow.
    This is the endpoint being pulled by dirac-login when doing the device flow.
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
            status_code=status.HTTP_400_BAD_REQUEST,
            data={"error": "authorization_pending"},
        ) from e
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except ExpiredFlowError as e:
        raise DiracHttpResponseError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            data={"error": "expired_token"},
        ) from e
    except InvalidCredentialsError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        ) from e
    except JoseError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid JWT: {e}",
        ) from e
    except PermissionError as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
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
    """Endpoint used by legacy DIRAC to mint tokens for proxy -> token exchange.

    This route is disabled if DIRACX_LEGACY_EXCHANGE_HASHED_API_KEY is not set
    in the environment.

    If legacy token exchange is required, an API key must be included in the
    request. This can be generated with the following python code::

        import secrets
        import base64
        import hashlib
        token = secrets.token_bytes()

        # This is the secret to include in the request by setting the
        # /DiracX/LegacyExchangeApiKey CS option in your legacy DIRAC installation
        print(f"API key is diracx:legacy:{base64.urlsafe_b64encode(token).decode()}")

        # This is the environment variable to set on the DiracX server
        print(f"DIRACX_LEGACY_EXCHANGE_HASHED_API_KEY={hashlib.sha256(token).hexdigest()}")

    """
    if not (
        expected_api_key := os.environ.get("DIRACX_LEGACY_EXCHANGE_HASHED_API_KEY")
    ):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
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
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    except InvalidCredentialsError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        ) from e
    except PermissionError as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(e),
        ) from e
    return await mint_token(
        access_payload, refresh_payload, None, all_access_policies, settings
    )
