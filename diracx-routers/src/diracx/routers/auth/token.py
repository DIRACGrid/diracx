"""Token endpoint implementation."""

import base64
import hashlib
import os
import re
from datetime import timedelta
from typing import Annotated, Literal
from uuid import uuid4

from authlib.jose import JsonWebToken
from fastapi import Depends, Form, Header, HTTPException, status

from diracx.core.exceptions import (
    DiracHttpResponse,
    ExpiredFlowError,
    PendingAuthorizationError,
)
from diracx.core.models import TokenResponse
from diracx.db.sql.auth.schema import FlowStatus, RefreshTokenStatus
from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.auth.utils import GrantType

from ..dependencies import AuthDB, AvailableSecurityProperties, Config
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthSettings, get_allowed_user_properties
from .utils import (
    parse_and_validate_scope,
    verify_dirac_refresh_token,
)

router = DiracxRouter(require_auth=False)


@router.post("/token")
async def token(
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
    legacy_exchange = False

    if grant_type == GrantType.device_code:
        oidc_token_info, scope = await get_oidc_token_info_from_device_flow(
            device_code, client_id, auth_db, settings
        )

    elif grant_type == GrantType.authorization_code:
        oidc_token_info, scope = await get_oidc_token_info_from_authorization_flow(
            code, client_id, redirect_uri, code_verifier, auth_db, settings
        )

    elif grant_type == GrantType.refresh_token:
        (
            oidc_token_info,
            scope,
            legacy_exchange,
        ) = await get_oidc_token_info_from_refresh_flow(
            refresh_token, auth_db, settings
        )
    else:
        raise NotImplementedError(f"Grant type not implemented {grant_type}")

    # Get a TokenResponse to return to the user
    return await exchange_token(
        auth_db,
        scope,
        oidc_token_info,
        config,
        settings,
        available_properties,
        all_access_policies=all_access_policies,
        legacy_exchange=legacy_exchange,
    )


async def get_oidc_token_info_from_device_flow(
    device_code: str | None, client_id: str, auth_db: AuthDB, settings: AuthSettings
):
    """Get OIDC token information from the device flow DB and check few parameters before returning it."""
    assert device_code is not None
    try:
        info = await auth_db.get_device_flow(
            device_code, settings.device_flow_expiration_seconds
        )
    except PendingAuthorizationError as e:
        raise DiracHttpResponse(
            status.HTTP_400_BAD_REQUEST, {"error": "authorization_pending"}
        ) from e
    except ExpiredFlowError as e:
        raise DiracHttpResponse(
            status.HTTP_401_UNAUTHORIZED, {"error": "expired_token"}
        ) from e
    # raise DiracHttpResponse(status.HTTP_400_BAD_REQUEST, {"error": "slow_down"})
    # raise DiracHttpResponse(status.HTTP_400_BAD_REQUEST, {"error": "expired_token"})

    if info["client_id"] != client_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Bad client_id",
        )
    oidc_token_info = info["id_token"]
    scope = info["scope"]

    # TODO: use HTTPException while still respecting the standard format
    # required by the RFC
    if info["status"] != FlowStatus.READY:
        # That should never ever happen
        raise NotImplementedError(f"Unexpected flow status {info['status']!r}")
    return (oidc_token_info, scope)


async def get_oidc_token_info_from_authorization_flow(
    code: str | None,
    client_id: str | None,
    redirect_uri: str | None,
    code_verifier: str | None,
    auth_db: AuthDB,
    settings: AuthSettings,
):
    """Get OIDC token information from the authorization flow DB and check few parameters before returning it."""
    assert code is not None
    info = await auth_db.get_authorization_flow(
        code, settings.authorization_flow_expiration_seconds
    )
    if redirect_uri != info["redirect_uri"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid redirect_uri",
        )
    if client_id != info["client_id"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Bad client_id",
        )

    # Check the code_verifier
    try:
        assert code_verifier is not None
        code_challenge = (
            base64.urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest())
            .decode()
            .strip("=")
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Malformed code_verifier",
        ) from e

    if code_challenge != info["code_challenge"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid code_challenge",
        )

    oidc_token_info = info["id_token"]
    scope = info["scope"]

    # TODO: use HTTPException while still respecting the standard format
    # required by the RFC
    if info["status"] != FlowStatus.READY:
        # That should never ever happen
        raise NotImplementedError(f"Unexpected flow status {info['status']!r}")

    return (oidc_token_info, scope)


async def get_oidc_token_info_from_refresh_flow(
    refresh_token: str | None, auth_db: AuthDB, settings: AuthSettings
):
    """Get OIDC token information from the refresh token DB and check few parameters before returning it."""
    assert refresh_token is not None

    # Decode the refresh token to get the JWT ID
    jti, _, legacy_exchange = await verify_dirac_refresh_token(refresh_token, settings)

    # Get some useful user information from the refresh token entry in the DB
    refresh_token_attributes = await auth_db.get_refresh_token(jti)

    sub = refresh_token_attributes["sub"]

    # Check if the refresh token was obtained from the legacy_exchange endpoint
    # If it is the case, we bypass the refresh token rotation mechanism
    if not legacy_exchange:
        # Refresh token rotation: https://datatracker.ietf.org/doc/html/rfc6749#section-10.4
        # Check that the refresh token has not been already revoked
        # This might indicate that a potential attacker try to impersonate someone
        # In such case, all the refresh tokens bound to a given user (subject) should be revoked
        # Forcing the user to reauthenticate interactively through an authorization/device flow (recommended practice)
        if refresh_token_attributes["status"] == RefreshTokenStatus.REVOKED:
            # Revoke all the user tokens from the subject
            await auth_db.revoke_user_refresh_tokens(sub)

            # Commit here, otherwise the revokation operation will not be taken into account
            # as we return an error to the user
            await auth_db.conn.commit()

            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Revoked refresh token reused: potential attack detected. You must authenticate again",
            )

        # Part of the refresh token rotation mechanism:
        # Revoke the refresh token provided, a new one needs to be generated
        await auth_db.revoke_refresh_token(jti)

    # Build an ID token and get scope from the refresh token attributes received
    oidc_token_info = {
        # The sub attribute coming from the DB contains the VO name
        # We need to remove it as if it were coming from an ID token from an external IdP
        "sub": sub.split(":", 1)[1],
        "preferred_username": refresh_token_attributes["preferred_username"],
    }
    scope = refresh_token_attributes["scope"]
    return (oidc_token_info, scope, legacy_exchange)


BASE_64_URL_SAFE_PATTERN = (
    r"(?:[A-Za-z0-9\-_]{4})*(?:[A-Za-z0-9\-_]{2}==|[A-Za-z0-9\-_]{3}=)?"
)
LEGACY_EXCHANGE_PATTERN = rf"Bearer diracx:legacy:({BASE_64_URL_SAFE_PATTERN})"


@router.get("/legacy-exchange", include_in_schema=False)
async def legacy_exchange(
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
):
    """Endpoint used by legacy DIRAC to mint tokens for proxy -> token exchange.

    This route is disabled if DIRACX_LEGACY_EXCHANGE_HASHED_API_KEY is not set
    in the environment.

    If legacy token exchange is required, an API key must be included in the
    request. This can be generated with the following python code::

        import secrets
        import base64
        import hashlib
        token = secrets.token_bytes()

        # This is the secret to include in the request
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

    if match := re.fullmatch(LEGACY_EXCHANGE_PATTERN, authorization):
        raw_token = base64.urlsafe_b64decode(match.group(1))
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid authorization header",
        )

    if hashlib.sha256(raw_token).hexdigest() != expected_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        parsed_scope = parse_and_validate_scope(scope, config, available_properties)
        vo_users = config.Registry[parsed_scope["vo"]]
        sub = vo_users.sub_from_preferred_username(preferred_username)
    except (KeyError, ValueError) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid scope or preferred_username",
        ) from e
    except PermissionError as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=e.args[0],
        ) from e

    return await exchange_token(
        auth_db,
        scope,
        {"sub": sub, "preferred_username": preferred_username},
        config,
        settings,
        available_properties,
        all_access_policies=all_access_policies,
        refresh_token_expire_minutes=expires_minutes,
        legacy_exchange=True,
    )


async def exchange_token(
    auth_db: AuthDB,
    scope: str,
    oidc_token_info: dict,
    config: Config,
    settings: AuthSettings,
    available_properties: AvailableSecurityProperties,
    all_access_policies: Annotated[
        dict[str, BaseAccessPolicy], Depends(BaseAccessPolicy.all_used_access_policies)
    ],
    *,
    refresh_token_expire_minutes: int | None = None,
    legacy_exchange: bool = False,
) -> TokenResponse:
    """Method called to exchange the OIDC token for a DIRAC generated access token."""
    # Extract dirac attributes from the OIDC scope
    try:
        parsed_scope = parse_and_validate_scope(scope, config, available_properties)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=e.args[0],
        ) from e
    vo = parsed_scope["vo"]
    dirac_group = parsed_scope["group"]
    properties = parsed_scope["properties"]

    # Extract attributes from the OIDC token details
    sub = oidc_token_info["sub"]
    if user_info := config.Registry[vo].Users.get(sub):
        preferred_username = user_info.PreferedUsername
    else:
        preferred_username = oidc_token_info.get("preferred_username", sub)
        raise NotImplementedError(
            "Dynamic registration of users is not yet implemented"
        )

    # Extract attributes from the settings and configuration
    issuer = settings.token_issuer

    # Check that the subject is part of the dirac users
    if sub not in config.Registry[vo].Groups[dirac_group].Users:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"User is not a member of the requested group ({preferred_username}, {dirac_group})",
        )

    # Check that the user properties are valid
    allowed_user_properties = get_allowed_user_properties(config, sub, vo)
    if not properties.issubset(allowed_user_properties):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"{' '.join(properties - allowed_user_properties)} are not valid properties "
            f"for user {preferred_username}, available values: {' '.join(allowed_user_properties)}",
        )

    # Merge the VO with the subject to get a unique DIRAC sub
    sub = f"{vo}:{sub}"

    # Insert the refresh token with user details into the RefreshTokens table
    # User details are needed to regenerate access tokens later
    jti, creation_time = await auth_db.insert_refresh_token(
        subject=sub,
        preferred_username=preferred_username,
        scope=scope,
    )

    # Generate refresh token payload
    if refresh_token_expire_minutes is None:
        refresh_token_expire_minutes = settings.refresh_token_expire_minutes
    refresh_payload = {
        "jti": jti,
        "exp": creation_time + timedelta(minutes=refresh_token_expire_minutes),
        # legacy_exchange is used to indicate that the original refresh token
        # was obtained from the legacy_exchange endpoint
        "legacy_exchange": legacy_exchange,
    }

    # Generate access token payload
    # For now, the access token is only used to access DIRAC services,
    # therefore, the audience is not set and checked
    access_payload = {
        "sub": sub,
        "vo": vo,
        "iss": issuer,
        "dirac_properties": list(properties),
        "jti": str(uuid4()),
        "preferred_username": preferred_username,
        "dirac_group": dirac_group,
        "exp": creation_time + timedelta(minutes=settings.access_token_expire_minutes),
    }

    # Enrich the token payload with policy specific content
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

    access_payload["dirac_policies"] = dirac_access_policies
    refresh_payload["dirac_policies"] = dirac_refresh_policies

    # Generate the token: encode the payloads
    access_token = create_token(access_payload, settings)
    refresh_token = create_token(refresh_payload, settings)

    return TokenResponse(
        access_token=access_token,
        expires_in=settings.access_token_expire_minutes * 60,
        refresh_token=refresh_token,
    )


def create_token(payload: dict, settings: AuthSettings) -> str:
    jwt = JsonWebToken(settings.token_algorithm)
    encoded_jwt = jwt.encode(
        {"alg": settings.token_algorithm}, payload, settings.token_key.jwk
    )
    return encoded_jwt.decode("ascii")
