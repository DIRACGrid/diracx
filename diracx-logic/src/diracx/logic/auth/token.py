"""Token endpoint implementation."""

from __future__ import annotations

import base64
import hashlib
import re
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from authlib.jose import JsonWebToken

from diracx.core.config import Config
from diracx.core.exceptions import (
    AuthorizationError,
    ExpiredFlowError,
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
    """Token endpoint to retrieve the token at the end of a flow."""
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
    """Get OIDC token information from the device flow DB and check few parameters before returning it."""
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
    """Get OIDC token information from the authorization flow DB and check few parameters before returning it."""
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
    """Get OIDC token information from the refresh token DB and check few parameters before returning it."""
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

            # Commit here, otherwise the revokation operation will not be taken into account
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
        "preferred_username": refresh_token_attributes["PreferredUsername"],
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
    """Endpoint used by legacy DIRAC to mint tokens for proxy -> token exchange."""
    if match := re.fullmatch(LEGACY_EXCHANGE_PATTERN, authorization):
        raw_token = base64.urlsafe_b64decode(match.group(1))
    else:
        raise ValueError("Invalid authorization header")

    if hashlib.sha256(raw_token).hexdigest() != expected_api_key:
        raise InvalidCredentialsError("Invalid credentials")

    try:
        parsed_scope = parse_and_validate_scope(scope, config, available_properties)
        vo_users = config.Registry[parsed_scope["vo"]]
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
    """Method called to exchange the OIDC token for a DIRAC generated access token."""
    # Extract dirac attributes from the OIDC scope
    parsed_scope = parse_and_validate_scope(scope, config, available_properties)
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

    # Check that the subject is part of the dirac users
    if sub not in config.Registry[vo].Groups[dirac_group].Users:
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

    creation_time = datetime.now(timezone.utc)
    refresh_payload: RefreshTokenPayload | None = None
    if include_refresh_token:
        # Insert the refresh token with user details into the RefreshTokens table
        # User details are needed to regenerate access tokens later
        jti, creation_time = await insert_refresh_token(
            auth_db=auth_db,
            subject=sub,
            preferred_username=preferred_username,
            scope=scope,
        )

        # Generate refresh token payload
        if refresh_token_expire_minutes is None:
            refresh_token_expire_minutes = settings.refresh_token_expire_minutes
        refresh_payload = {
            "jti": str(jti),
            "exp": creation_time + timedelta(minutes=refresh_token_expire_minutes),
            # legacy_exchange is used to indicate that the original refresh token
            # was obtained from the legacy_exchange endpoint
            "legacy_exchange": legacy_exchange,
            "dirac_policies": {},
        }

    # Generate access token payload
    # For now, the access token is only used to access DIRAC services,
    # therefore, the audience is not set and checked
    access_payload: AccessTokenPayload = {
        "sub": sub,
        "vo": vo,
        "iss": settings.token_issuer,
        "dirac_properties": list(properties),
        "jti": str(uuid4()),
        "preferred_username": preferred_username,
        "dirac_group": dirac_group,
        "exp": creation_time + timedelta(minutes=settings.access_token_expire_minutes),
        "dirac_policies": {},
    }

    return access_payload, refresh_payload


def create_token(payload: TokenPayload, settings: AuthSettings) -> str:
    jwt = JsonWebToken(settings.token_algorithm)
    encoded_jwt = jwt.encode(
        {"alg": settings.token_algorithm}, payload, settings.token_key.jwk
    )
    return encoded_jwt.decode("ascii")


async def insert_refresh_token(
    auth_db: AuthDB,
    subject: str,
    preferred_username: str,
    scope: str,
) -> tuple[UUID, datetime]:
    """Insert a refresh token into the database and return the JWT ID and creation time."""
    # Generate a JWT ID
    jti = uuid4()

    # Insert the refresh token into the DB
    await auth_db.insert_refresh_token(
        jti=jti,
        subject=subject,
        preferred_username=preferred_username,
        scope=scope,
    )

    # Get the creation time of the refresh token
    refresh_token = await auth_db.get_refresh_token(jti)
    return jti, refresh_token["CreationTime"]


async def get_device_flow(auth_db: AuthDB, device_code: str, max_validity: int):
    """Get the device flow from the DB and check few parameters before returning it."""
    res = await auth_db.get_device_flow(device_code)

    if res["CreationTime"].replace(tzinfo=timezone.utc) < substract_date(
        seconds=max_validity
    ):
        raise ExpiredFlowError()

    if res["Status"] == FlowStatus.READY:
        await auth_db.update_device_flow_status(device_code, FlowStatus.DONE)
        return res

    if res["Status"] == FlowStatus.DONE:
        raise AuthorizationError("Code was already used")

    if res["Status"] == FlowStatus.PENDING:
        raise PendingAuthorizationError()

    raise AuthorizationError("Bad state in device flow")


async def get_authorization_flow(auth_db: AuthDB, code: str, max_validity: int):
    """Get the authorization flow from the DB and check few parameters before returning it."""
    res = await auth_db.get_authorization_flow(code, max_validity)

    if res["Status"] == FlowStatus.READY:
        await auth_db.update_authorization_flow_status(code, FlowStatus.DONE)
        return res

    if res["Status"] == FlowStatus.DONE:
        raise AuthorizationError("Code was already used")

    raise AuthorizationError("Bad state in authorization flow")
