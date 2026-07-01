"""Token endpoint logic used by DIRACX OAuth2 flows.

This module implements the business logic behind the token endpoint and
related grant validation functions. It supports device flow, authorization
code flow, refresh token exchange, legacy proxy exchange, and token minting.
"""

from __future__ import annotations

import base64
import hashlib
import re
from datetime import datetime, timedelta, timezone
from typing import cast

from joserfc import jwt
from joserfc.jwt import Claims
from uuid_utils import UUID, uuid7

from diracx.core.config import Config
from diracx.core.exceptions import (
    AuthorizationError,
    InvalidCredentialsError,
    PendingAuthorizationError,
)
from diracx.core.models import (
    AccessTokenPayload,
    GrantType,
    RefreshTokenPayload,
    TokenPayload,
)
from diracx.core.properties import SecurityProperty
from diracx.core.settings import AuthSettings
from diracx.db.sql import AuthDB
from diracx.db.sql.auth.schema import FlowStatus, RefreshTokenStatus
from diracx.db.sql.utils import uuid7_to_datetime
from diracx.db.sql.utils.functions import substract_date

from .utils import (
    get_allowed_user_properties,
    parse_and_validate_scope,
    verify_dirac_refresh_token,
)


async def get_oidc_token(
    grant_type: GrantType,
    client_id: str,
    auth_db: AuthDB,
    config: Config,
    settings: AuthSettings,
    available_properties: set[SecurityProperty],
    device_code: str | None = None,
    code: str | None = None,
    redirect_uri: str | None = None,
    code_verifier: str | None = None,
    refresh_token: str | None = None,
) -> tuple[AccessTokenPayload, RefreshTokenPayload | None]:
    """Create OIDC-compatible access and refresh token payloads.

    Args:
        grant_type (GrantType): The grant type being exchanged.
        client_id (str): Client identifier making the token request.
        auth_db (AuthDB): Database accessor for auth flow and refresh token state.
        config (Config): Application configuration registry.
        settings (AuthSettings): Authentication settings.
        available_properties (set[SecurityProperty]): Security properties available to the client/user.
        device_code (str | None): Device flow code when using the device grant.
        code (str | None): Authorization code for the authorization code grant.
        redirect_uri (str | None): Redirect URI used with the authorization code grant.
        code_verifier (str | None): PKCE code verifier for the authorization code grant.
        refresh_token (str | None): Refresh token for the refresh token grant.

    Returns:
        tuple[AccessTokenPayload, RefreshTokenPayload | None]: Access token payload and optional refresh token payload.

    Raises:
        NotImplementedError: If the grant type is not supported.
    """
    legacy_exchange = False
    include_refresh_token = True
    refresh_token_expire_minutes = None

    if grant_type == GrantType.device_code:
        assert device_code is not None
        oidc_token_info, scope = await get_oidc_token_info_from_device_flow(
            device_code, client_id, auth_db, settings
        )

    elif grant_type == GrantType.authorization_code:
        assert code is not None
        assert code_verifier is not None
        oidc_token_info, scope = await get_oidc_token_info_from_authorization_flow(
            code, client_id, redirect_uri, code_verifier, auth_db, settings
        )

    elif grant_type == GrantType.refresh_token:
        assert refresh_token is not None
        (
            oidc_token_info,
            scope,
            legacy_exchange,
            refresh_token_expire_minutes,
            include_refresh_token,
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
        legacy_exchange=legacy_exchange,
        refresh_token_expire_minutes=refresh_token_expire_minutes,
        include_refresh_token=include_refresh_token,
    )


async def get_oidc_token_info_from_device_flow(
    device_code: str, client_id: str, auth_db: AuthDB, settings: AuthSettings
) -> tuple[dict, str]:
    """Validate a device flow code and return OIDC token information.

    Args:
        device_code (str): Device flow code to resolve.
        client_id (str): Expected client identifier.
        auth_db (AuthDB): Database accessor for device flow state.
        settings (AuthSettings): Authentication settings.

    Returns:
        tuple[dict, str]: OIDC token info and the resolved scope string.

    Raises:
        ValueError: If the client ID does not match the stored flow.
        NotImplementedError: If the flow status is not READY.
    """
    info = await get_device_flow(
        auth_db, device_code, settings.device_flow_expiration_seconds
    )

    if info["ClientID"] != client_id:
        raise ValueError("Bad client_id")

    oidc_token_info = info["IDToken"]
    scope = info["Scope"]

    # TODO: use HTTPException while still respecting the standard format
    # required by the RFC
    if info["Status"] != FlowStatus.READY:
        # That should never ever happen
        raise NotImplementedError(f"Unexpected flow status {info['status']!r}")
    return (oidc_token_info, scope)


async def get_oidc_token_info_from_authorization_flow(
    code: str,
    client_id: str | None,
    redirect_uri: str | None,
    code_verifier: str,
    auth_db: AuthDB,
    settings: AuthSettings,
) -> tuple[dict, str]:
    """Validate an authorization code and return OIDC token information.

    Args:
        code (str): Authorization code returned by the identity provider.
        client_id (str | None): Expected client identifier.
        redirect_uri (str | None): Expected redirect URI.
        code_verifier (str): PKCE verifier used to validate the code challenge.
        auth_db (AuthDB): Database accessor for authorization flow state.
        settings (AuthSettings): Authentication settings.

    Returns:
        tuple[dict, str]: OIDC token info and the resolved scope string.

    Raises:
        ValueError: If the client ID, redirect URI, or code verifier are invalid.
        NotImplementedError: If the flow status is not READY.
    """
    info = await get_authorization_flow(
        auth_db, code, settings.authorization_flow_expiration_seconds
    )
    if redirect_uri != info["RedirectURI"]:
        raise ValueError("Invalid redirect_uri")
    if client_id != info["ClientID"]:
        raise ValueError("Bad client_id")

    # Check the code_verifier
    try:
        code_challenge = (
            base64.urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest())
            .decode()
            .strip("=")
        )
    except Exception as e:
        raise ValueError("Malformed code_verifier") from e

    if code_challenge != info["CodeChallenge"]:
        raise ValueError("Invalid code_challenge")

    oidc_token_info = info["IDToken"]
    scope = info["Scope"]

    # TODO: use HTTPException while still respecting the standard format
    # required by the RFC
    if info["Status"] != FlowStatus.READY:
        # That should never ever happen
        raise NotImplementedError(f"Unexpected flow status {info['status']!r}")

    return (oidc_token_info, scope)


async def get_oidc_token_info_from_refresh_flow(
    refresh_token: str, auth_db: AuthDB, settings: AuthSettings
) -> tuple[dict, str, bool, float, bool]:
    """Validate a refresh token and return OIDC token information.

    This verifies the refresh token, checks its status in the DB, handles
    refresh token rotation for non-legacy exchanges, and returns the token
    payload needed to mint a new access token.

    Args:
        refresh_token (str): Refresh token string to validate.
        auth_db (AuthDB): Database accessor for refresh token state.
        settings (AuthSettings): Authentication settings.

    Returns:
        tuple[dict, str, bool, float, bool]: OIDC token info, scope, legacy
            exchange flag, remaining expiry minutes, and include-refresh-token flag.

    Raises:
        InvalidCredentialsError: If the refresh token has been revoked or is invalid.
    """
    # Decode the refresh token to get the JWT ID
    jti, exp, legacy_exchange = await verify_dirac_refresh_token(
        refresh_token, settings
    )

    # Get some useful user information from the refresh token entry in the DB
    refresh_token_attributes = await auth_db.get_refresh_token(jti)

    sub = refresh_token_attributes["Sub"]

    # Get the remaining time in minutes before the token expires
    remaining_minutes = (
        datetime.fromtimestamp(exp, timezone.utc) - datetime.now(timezone.utc)
    ).total_seconds() / 60

    # Check if the refresh token was obtained from the legacy_exchange endpoint
    if not legacy_exchange:
        include_refresh_token = True

        # Refresh token rotation: https://datatracker.ietf.org/doc/html/rfc6749#section-10.4
        # Check that the refresh token has not been already revoked
        # This might indicate that a potential attacker try to impersonate someone
        # In such case, all the refresh tokens bound to a given user (subject) should be revoked
        # Forcing the user to reauthenticate interactively through an authorization/device flow (recommended practice)
        if refresh_token_attributes["Status"] == RefreshTokenStatus.REVOKED:
            # Revoke all the user tokens from the subject
            await auth_db.revoke_user_refresh_tokens(sub)

            # Commit here, otherwise the revocation operation will not be taken into account
            # as we return an error to the user
            await auth_db.conn.commit()

            raise InvalidCredentialsError(
                "Revoked refresh token reused: potential attack detected. You must authenticate again"
            )

        # Part of the refresh token rotation mechanism:
        # Revoke the refresh token provided, a new one needs to be generated
        await auth_db.revoke_refresh_token(jti)
    else:
        # We bypass the refresh token rotation mechanism
        # and we don't want to generate a new refresh token
        include_refresh_token = False

    # Build an ID token and get scope from the refresh token attributes received
    oidc_token_info = {
        # The sub attribute coming from the DB contains the VO name
        # We need to remove it as if it were coming from an ID token from an external IdP
        "sub": sub.split(":", 1)[1],
    }
    scope = refresh_token_attributes["Scope"]
    return (
        oidc_token_info,
        scope,
        legacy_exchange,
        remaining_minutes,
        include_refresh_token,
    )


BASE_64_URL_SAFE_PATTERN = (
    r"(?:[A-Za-z0-9\-_]{4})*(?:[A-Za-z0-9\-_]{2}==|[A-Za-z0-9\-_]{3}=)?"
)
LEGACY_EXCHANGE_PATTERN = rf"Bearer diracx:legacy:({BASE_64_URL_SAFE_PATTERN})"


async def perform_legacy_exchange(
    expected_api_key: str,
    preferred_username: str,
    scope: str,
    authorization: str,
    auth_db: AuthDB,
    available_properties: set[SecurityProperty],
    settings: AuthSettings,
    config: Config,
    expires_minutes: float | None = None,
) -> tuple[AccessTokenPayload, RefreshTokenPayload | None]:
    """Perform a legacy proxy-to-token exchange for older DIRAC clients.

    Args:
        expected_api_key (str): Expected hashed API key used to authenticate the proxy.
        preferred_username (str): Username asserted by the legacy proxy.
        scope (str): Requested scope string.
        authorization (str): Authorization header value from the legacy proxy.
        auth_db (AuthDB): Database accessor used for token and flow state.
        available_properties (set[SecurityProperty]): Allowed properties for the user.
        settings (AuthSettings): Authentication settings.
        config (Config): Application configuration registry.
        expires_minutes (float | None): Optional refresh token expiry override.

    Returns:
        tuple[AccessTokenPayload, RefreshTokenPayload | None]: Generated access/refresh payloads.

    Raises:
        ValueError: If the authorization header is invalid or the scope/preferred username is invalid.
        InvalidCredentialsError: If the provided proxy credentials are incorrect.
    """
    if match := re.fullmatch(LEGACY_EXCHANGE_PATTERN, authorization):
        raw_token = base64.urlsafe_b64decode(match.group(1))
    else:
        raise ValueError("Invalid authorization header")

    if hashlib.sha256(raw_token).hexdigest() != expected_api_key:
        raise InvalidCredentialsError("Invalid credentials")

    try:
        parsed_scope = parse_and_validate_scope(scope, config, available_properties)
        vo_users = config.registry[parsed_scope["vo"]]
        sub = vo_users.sub_from_preferred_username(preferred_username)
    except (KeyError, ValueError) as e:
        raise ValueError("Invalid scope or preferred_username") from e

    return await exchange_token(
        auth_db,
        scope,
        {"sub": sub, "preferred_username": preferred_username},
        config,
        settings,
        available_properties,
        refresh_token_expire_minutes=expires_minutes,
        legacy_exchange=True,
    )


async def exchange_token(
    auth_db: AuthDB,
    scope: str,
    oidc_token_info: dict,
    config: Config,
    settings: AuthSettings,
    available_properties: set[SecurityProperty],
    *,
    refresh_token_expire_minutes: float | None = None,
    legacy_exchange: bool = False,
    include_refresh_token: bool = True,
) -> tuple[AccessTokenPayload, RefreshTokenPayload | None]:
    """Create DIRAC access and refresh token payloads from OIDC token info.

    Args:
        auth_db (AuthDB): Database accessor for refresh token state.
        scope (str): The OIDC scope string.
        oidc_token_info (dict): Claims extracted from the OIDC token.
        config (Config): Application configuration registry.
        settings (AuthSettings): Authentication settings.
        available_properties (set[SecurityProperty]): Allowed security properties for the user.
        refresh_token_expire_minutes (float | None): Optional refresh token expiry override.
        legacy_exchange (bool): Whether the request is a legacy proxy exchange.
        include_refresh_token (bool): Whether to mint a new refresh token.

    Returns:
        tuple[AccessTokenPayload, RefreshTokenPayload | None]: Newly generated token payloads.
    """
    # Extract dirac attributes from the OIDC scope
    parsed_scope = parse_and_validate_scope(scope, config, available_properties)
    vo = parsed_scope["vo"]
    dirac_group = parsed_scope["group"]
    properties = parsed_scope["properties"]

    # Extract attributes from the OIDC token details
    sub = oidc_token_info["sub"]
    if user_info := config.registry[vo].users.get(sub):
        preferred_username = user_info.prefered_username
    else:
        preferred_username = oidc_token_info.get("preferred_username", sub)
        raise NotImplementedError(
            "Dynamic registration of users is not yet implemented"
        )

    # Check that the subject is part of the dirac users
    if sub not in config.registry[vo].groups[dirac_group].users:
        raise PermissionError(
            f"User is not a member of the requested group ({preferred_username}, {dirac_group})"
        )

    # Check that the user properties are valid
    allowed_user_properties = get_allowed_user_properties(config, sub, vo)
    if not properties.issubset(allowed_user_properties):
        raise PermissionError(
            f"{' '.join(properties - allowed_user_properties)} are not valid properties "
            f"for user {preferred_username}, available values: {' '.join(allowed_user_properties)}"
        )

    # Merge the VO with the subject to get a unique DIRAC sub
    sub = f"{vo}:{sub}"

    refresh_payload: RefreshTokenPayload | None = None
    if include_refresh_token:
        # Insert the refresh token with user details into the RefreshTokens table
        # User details are needed to regenerate access tokens later
        refresh_jti = await insert_refresh_token(
            auth_db=auth_db,
            subject=sub,
            scope=scope,
        )

        # Generate refresh token payload
        if refresh_token_expire_minutes is None:
            refresh_token_expire_minutes = settings.refresh_token_expire_minutes
        refresh_exp = uuid7_to_datetime(refresh_jti) + timedelta(
            minutes=refresh_token_expire_minutes
        )
        refresh_payload = RefreshTokenPayload(
            jti=str(refresh_jti),
            exp=refresh_exp,
            # legacy_exchange is used to indicate that the original refresh token
            # was obtained from the legacy_exchange endpoint
            legacy_exchange=legacy_exchange,
            dirac_policies={},
        )

    # Generate access token payload
    # For now, the access token is only used to access DIRAC services,
    # therefore, the audience is not set and checked
    access_jti = uuid7()
    access_exp = uuid7_to_datetime(access_jti) + timedelta(
        minutes=settings.access_token_expire_minutes
    )
    access_payload = AccessTokenPayload(
        sub=sub,
        vo=vo,
        iss=settings.token_issuer,
        dirac_properties=list(properties),
        jti=str(access_jti),
        preferred_username=preferred_username,
        dirac_group=dirac_group,
        exp=access_exp,
        dirac_policies={},
    )

    return access_payload, refresh_payload


def create_token(payload: TokenPayload, settings: AuthSettings) -> str:
    """Create a signed JWT token from the provided payload.

    Args:
        payload (TokenPayload): Typed token payload to sign.
        settings (AuthSettings): Authentication settings containing signing keys.

    Returns:
        str: Signed JWT token string.
    """
    return _sign_token_payload(payload.model_dump(), settings)


def _sign_token_payload(claims: dict, settings: AuthSettings) -> str:
    """Sign a raw claims dictionary as a JWT string.

    Args:
        claims (dict): JWT claims to sign.
        settings (AuthSettings): Authentication settings containing signing keys.

    Returns:
        str: Signed JWT token.
    """
    signing_key = None
    for key in settings.token_keystore.jwks.keys:
        key_ops = key.get("key_ops")
        if key_ops and not isinstance(key_ops, list):
            key_ops = [key_ops]
        if key_ops and "sign" in key_ops:
            signing_key = key
            break

    if not signing_key:
        raise ValueError("No signing key found in JWKS")

    return jwt.encode(
        header={"alg": signing_key.get("alg"), "kid": signing_key.get("kid")},
        claims=cast(Claims, claims),
        key=settings.token_keystore.jwks,
        algorithms=settings.token_allowed_algorithms,
    )


async def insert_refresh_token(
    auth_db: AuthDB,
    subject: str,
    scope: str,
) -> UUID:
    """Insert a refresh token record and return its generated JWT ID.

    Args:
        auth_db (AuthDB): Database accessor for refresh token state.
        subject (str): Subject identifier for the refresh token.
        scope (str): Scope string associated with the refresh token.

    Returns:
        UUID: Generated JWT ID for the refresh token.
    """
    # Generate a JWT ID
    jti = uuid7()

    # Insert the refresh token into the DB
    await auth_db.insert_refresh_token(
        jti=jti,
        subject=subject,
        scope=scope,
    )
    return jti


async def get_device_flow(auth_db: AuthDB, device_code: str, max_validity: int):
    """Load and validate a device flow record from the database.

    Args:
        auth_db (AuthDB): Database accessor for device flow state.
        device_code (str): Device code issued to the client.
        max_validity (int): Maximum validity window in seconds for the device code.

    Returns:
        dict: Device flow record from the database.

    Raises:
        InvalidCredentialsError: If the device code has expired.
        AuthorizationError: If the code has already been used or is in a bad state.
        PendingAuthorizationError: If the device flow is still pending user approval.
    """
    res = await auth_db.get_device_flow(device_code)

    if res["CreationTime"].replace(tzinfo=timezone.utc) < substract_date(
        seconds=max_validity
    ):
        raise InvalidCredentialsError("Device code expired")

    if res["Status"] == FlowStatus.READY:
        await auth_db.update_device_flow_status(device_code, FlowStatus.DONE)
        return res

    if res["Status"] == FlowStatus.DONE:
        raise AuthorizationError("Code was already used")

    if res["Status"] == FlowStatus.PENDING:
        raise PendingAuthorizationError()

    raise AuthorizationError("Bad state in device flow")


async def get_authorization_flow(auth_db: AuthDB, code: str, max_validity: int):
    """Load and validate an authorization flow record from the database.

    Args:
        auth_db (AuthDB): Database accessor for authorization flow state.
        code (str): Authorization code issued to the client.
        max_validity (int): Maximum validity window in seconds for the authorization code.

    Returns:
        dict: Authorization flow record from the database.

    Raises:
        AuthorizationError: If the code has already been used or is in a bad state.
    """
    res = await auth_db.get_authorization_flow(code, max_validity)

    if res["Status"] == FlowStatus.READY:
        await auth_db.update_authorization_flow_status(code, FlowStatus.DONE)
        return res

    if res["Status"] == FlowStatus.DONE:
        raise AuthorizationError("Code was already used")

    raise AuthorizationError("Bad state in authorization flow")
