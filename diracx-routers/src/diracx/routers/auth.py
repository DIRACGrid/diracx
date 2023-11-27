from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import secrets
from datetime import timedelta
from enum import StrEnum
from typing import Annotated, Literal, TypedDict
from uuid import UUID, uuid4

import httpx
from authlib.integrations.starlette_client import OAuthError
from authlib.jose import JoseError, JsonWebKey, JsonWebToken
from authlib.oidc.core import IDToken
from cachetools import TTLCache
from fastapi import (
    Depends,
    Form,
    Header,
    HTTPException,
    Request,
    Response,
    responses,
    status,
)
from fastapi.responses import RedirectResponse
from fastapi.security import OpenIdConnect
from pydantic import BaseModel, Field

from diracx.core.exceptions import (
    DiracHttpResponse,
    ExpiredFlowError,
    PendingAuthorizationError,
)
from diracx.core.models import TokenResponse, UserInfo
from diracx.core.properties import (
    PROXY_MANAGEMENT,
    SecurityProperty,
    UnevaluatedProperty,
)
from diracx.core.settings import ServiceSettingsBase, TokenSigningKey
from diracx.db.sql.auth.schema import FlowStatus, RefreshTokenStatus

from .dependencies import (
    AuthDB,
    AvailableSecurityProperties,
    Config,
    add_settings_annotation,
)
from .fastapi_classes import DiracxRouter

oidc_scheme = OpenIdConnect(openIdConnectUrl="/.well-known/openid-configuration")


@add_settings_annotation
class AuthSettings(ServiceSettingsBase, env_prefix="DIRACX_SERVICE_AUTH_"):
    dirac_client_id: str = "myDIRACClientID"
    # TODO: This should be taken dynamically
    # ["http://pclhcb211:8000/docs/oauth2-redirect"]
    allowed_redirects: list[str] = []
    device_flow_expiration_seconds: int = 600
    authorization_flow_expiration_seconds: int = 300

    token_issuer: str = "http://lhcbdirac.cern.ch/"
    token_audience: str = "dirac"
    token_key: TokenSigningKey
    token_algorithm: str = "RS256"
    access_token_expire_minutes: int = 20
    refresh_token_expire_minutes: int = 60

    available_properties: set[SecurityProperty] = Field(
        default_factory=SecurityProperty.available_properties
    )


def has_properties(expression: UnevaluatedProperty | SecurityProperty):
    evaluator = (
        expression
        if isinstance(expression, UnevaluatedProperty)
        else UnevaluatedProperty(expression)
    )

    async def require_property(
        user: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)]
    ):
        if not evaluator(user.properties):
            raise HTTPException(status.HTTP_403_FORBIDDEN)

    return Depends(require_property)


class GrantType(StrEnum):
    authorization_code = "authorization_code"
    device_code = "urn:ietf:params:oauth:grant-type:device_code"
    refresh_token = "refresh_token"


router = DiracxRouter(require_auth=False)

_server_metadata_cache: TTLCache = TTLCache(maxsize=1024, ttl=3600)


async def get_server_metadata(url: str):
    server_metadata = _server_metadata_cache.get(url)
    if server_metadata is None:
        async with httpx.AsyncClient() as c:
            res = await c.get(url)
            if res.status_code != 200:
                # TODO: Better error handling
                raise NotImplementedError(res)
            server_metadata = res.json()
            _server_metadata_cache[url] = server_metadata
    return server_metadata


async def fetch_jwk_set(url: str):
    server_metadata = await get_server_metadata(url)

    jwks_uri = server_metadata.get("jwks_uri")
    if not jwks_uri:
        raise RuntimeError('Missing "jwks_uri" in metadata')

    async with httpx.AsyncClient() as c:
        res = await c.get(jwks_uri)
        if res.status_code != 200:
            # TODO: Better error handling
            raise NotImplementedError(res)
        jwk_set = res.json()

    # self.server_metadata['jwks'] = jwk_set
    return JsonWebKey.import_key_set(jwk_set)


async def parse_id_token(config, vo, raw_id_token: str, audience: str):
    server_metadata = await get_server_metadata(
        config.Registry[vo].IdP.server_metadata_url
    )
    alg_values = server_metadata.get("id_token_signing_alg_values_supported", ["RS256"])
    jwk_set = await fetch_jwk_set(config.Registry[vo].IdP.server_metadata_url)

    token = JsonWebToken(alg_values).decode(
        raw_id_token,
        key=jwk_set,
        claims_cls=IDToken,
        claims_options={
            "iss": {"values": [server_metadata["issuer"]]},
            "aud": {"values": [audience]},
        },
    )
    token.validate()
    return token


class AuthInfo(BaseModel):
    # raw token for propagation
    bearer_token: str

    # token ID in the DB for Component
    # unique jwt identifier for user
    token_id: UUID

    # list of DIRAC properties
    properties: list[SecurityProperty]


class AuthorizedUserInfo(AuthInfo, UserInfo):
    pass


async def verify_dirac_access_token(
    authorization: Annotated[str, Depends(oidc_scheme)],
    settings: AuthSettings,
) -> AuthorizedUserInfo:
    """Verify dirac user token and return a UserInfo class
    Used for each API endpoint
    """
    if match := re.fullmatch(r"Bearer (.+)", authorization):
        raw_token = match.group(1)
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid authorization header",
        )

    try:
        jwt = JsonWebToken(settings.token_algorithm)
        token = jwt.decode(
            raw_token,
            key=settings.token_key.jwk,
            claims_options={
                "iss": {"values": [settings.token_issuer]},
                "aud": {"values": [settings.token_audience]},
            },
        )
        token.validate()
    except JoseError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid JWT",
        ) from None

    return AuthorizedUserInfo(
        bearer_token=raw_token,
        token_id=token["jti"],
        properties=token["dirac_properties"],
        sub=token["sub"],
        preferred_username=token["preferred_username"],
        dirac_group=token["dirac_group"],
        vo=token["vo"],
    )


async def verify_dirac_refresh_token(
    refresh_token: str,
    settings: AuthSettings,
) -> tuple[str, float, bool]:
    """Verify dirac user token and return a UserInfo class
    Used for each API endpoint
    """
    try:
        jwt = JsonWebToken(settings.token_algorithm)
        token = jwt.decode(
            refresh_token,
            key=settings.token_key.jwk,
        )
        token.validate()
    # Handle problematic tokens such as:
    # - tokens signed with an invalid JWK
    # - expired tokens
    except JoseError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid JWT: {e.args[0]}",
        ) from e

    return (token["jti"], float(token["exp"]), token["legacy_exchange"])


def create_token(payload: dict, settings: AuthSettings) -> str:
    jwt = JsonWebToken(settings.token_algorithm)
    encoded_jwt = jwt.encode(
        {"alg": settings.token_algorithm}, payload, settings.token_key.jwk
    )
    return encoded_jwt.decode("ascii")


async def exchange_token(
    auth_db: AuthDB,
    scope: str,
    oidc_token_info: dict,
    config: Config,
    settings: AuthSettings,
    available_properties: AvailableSecurityProperties,
    *,
    refresh_token_expire_minutes: int | None = None,
    legacy_exchange: bool = False,
) -> TokenResponse:
    """Method called to exchange the OIDC token for a DIRAC generated access token"""
    # Extract dirac attributes from the OIDC scope
    parsed_scope = parse_and_validate_scope(scope, config, available_properties)
    vo = parsed_scope["vo"]
    dirac_group = parsed_scope["group"]

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
    # dirac_properties needs to be a list in the token as to be json serializable
    dirac_properties = sorted(config.Registry[vo].Groups[dirac_group].Properties)

    # Check that the subject is part of the dirac users
    if sub not in config.Registry[vo].Groups[dirac_group].Users:
        raise ValueError(
            f"User is not a member of the requested group ({preferred_username}, {dirac_group})"
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
    access_payload = {
        "sub": sub,
        "vo": vo,
        "aud": settings.token_audience,
        "iss": issuer,
        "dirac_properties": dirac_properties,
        "jti": str(uuid4()),
        "preferred_username": preferred_username,
        "dirac_group": dirac_group,
        "exp": creation_time + timedelta(minutes=settings.access_token_expire_minutes),
    }

    # Generate the token: encode the payloads
    access_token = create_token(access_payload, settings)
    refresh_token = create_token(refresh_payload, settings)

    return TokenResponse(
        access_token=access_token,
        expires_in=settings.access_token_expire_minutes * 60,
        refresh_token=refresh_token,
    )


class InitiateDeviceFlowResponse(TypedDict):
    user_code: str
    device_code: str
    verification_uri_complete: str
    verification_uri: str
    expires_in: int


@router.post("/device")
async def initiate_device_flow(
    client_id: str,
    scope: str,
    audience: str,
    request: Request,
    auth_db: AuthDB,
    config: Config,
    available_properties: AvailableSecurityProperties,
    settings: AuthSettings,
) -> InitiateDeviceFlowResponse:
    """Initiate the device flow against DIRAC authorization Server.
    Scope must have exactly up to one `group` (otherwise default) and
    one or more `property` scope.
    If no property, then get default one

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

    user_code, device_code = await auth_db.insert_device_flow(
        client_id, scope, audience
    )

    verification_uri = str(request.url.replace(query={}))

    return {
        "user_code": user_code,
        "device_code": device_code,
        "verification_uri_complete": f"{verification_uri}?user_code={user_code}",
        "verification_uri": str(request.url.replace(query={})),
        "expires_in": settings.device_flow_expiration_seconds,
    }


async def initiate_authorization_flow_with_iam(
    config, vo: str, redirect_uri: str, state: dict[str, str]
):
    # code_verifier: https://www.rfc-editor.org/rfc/rfc7636#section-4.1
    code_verifier = secrets.token_hex()

    # code_challenge: https://www.rfc-editor.org/rfc/rfc7636#section-4.2
    code_challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest())
        .decode()
        .replace("=", "")
    )

    server_metadata = await get_server_metadata(
        config.Registry[vo].IdP.server_metadata_url
    )

    # Take these two from CS/.well-known
    authorization_endpoint = server_metadata["authorization_endpoint"]

    # TODO : encrypt it for good
    encrypted_state = base64.urlsafe_b64encode(
        json.dumps(state | {"vo": vo, "code_verifier": code_verifier}).encode()
    ).decode()

    urlParams = [
        "response_type=code",
        f"code_challenge={code_challenge}",
        "code_challenge_method=S256",
        f"client_id={config.Registry[vo].IdP.ClientID}",
        f"redirect_uri={redirect_uri}",
        "scope=openid%20profile",
        f"state={encrypted_state}",
    ]
    authorization_flow_url = f"{authorization_endpoint}?{'&'.join(urlParams)}"
    return authorization_flow_url


async def get_token_from_iam(
    config, vo: str, code: str, state: dict[str, str], redirect_uri: str
) -> dict[str, str]:
    server_metadata = await get_server_metadata(
        config.Registry[vo].IdP.server_metadata_url
    )

    # Take these two from CS/.well-known
    token_endpoint = server_metadata["token_endpoint"]

    data = {
        "grant_type": GrantType.authorization_code.value,
        "client_id": config.Registry[vo].IdP.ClientID,
        "code": code,
        "code_verifier": state["code_verifier"],
        "redirect_uri": redirect_uri,
    }

    async with httpx.AsyncClient() as c:
        res = await c.post(
            token_endpoint,
            data=data,
        )
        if res.status_code >= 500:
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY, "Failed to contact token endpoint"
            )
        elif res.status_code >= 400:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid code")

    raw_id_token = res.json()["id_token"]
    # Extract the payload and verify it
    try:
        id_token = await parse_id_token(
            config=config,
            vo=vo,
            raw_id_token=raw_id_token,
            audience=config.Registry[vo].IdP.ClientID,
        )
    except OAuthError:
        raise

    return id_token


@router.get("/device")
async def do_device_flow(
    request: Request,
    auth_db: AuthDB,
    user_code: str,
    config: Config,
    available_properties: AvailableSecurityProperties,
    settings: AuthSettings,
) -> RedirectResponse:
    """
    This is called as the verification URI for the device flow.
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
        config, parsed_scope["vo"], redirect_uri, state_for_iam
    )
    return RedirectResponse(authorization_flow_url)


def decrypt_state(state):
    try:
        # TODO: There have been better schemes like rot13
        return json.loads(base64.urlsafe_b64decode(state).decode())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid state"
        ) from e


@router.get("/device/complete")
async def finish_device_flow(
    request: Request,
    code: str,
    state: str,
    auth_db: AuthDB,
    config: Config,
    settings: AuthSettings,
):
    """
    This the url callbacked by IAM/Checkin after the authorization
    flow was granted.
    It gets us the code we need for the authorization flow, and we
    can map it to the corresponding device flow using the user_code
    in the cookie/session
    """
    decrypted_state = decrypt_state(state)
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
    response.body = b"<h1>Please close the window</h1>"
    response.status_code = 200
    response.media_type = "text/html"
    return response


class DeviceCodeTokenForm(BaseModel):
    grant_type: Literal[GrantType.device_code]
    device_code: str
    client_id: str


class ScopeInfoDict(TypedDict):
    group: str
    properties: list[str]
    vo: str


def parse_and_validate_scope(
    scope: str, config: Config, available_properties: set[SecurityProperty]
) -> ScopeInfoDict:
    """
    Check:
        * At most one VO
        * At most one group
        * group belongs to VO
        * properties are known
    return dict with group and properties

    :raises:
        * ValueError in case the scope isn't valide
    """
    scopes = set(scope.split(" "))

    groups = []
    properties = []
    vos = []
    unrecognised = []
    for scope in scopes:
        if scope.startswith("group:"):
            groups.append(scope.split(":", 1)[1])
        elif scope.startswith("property:"):
            properties.append(scope.split(":", 1)[1])
        elif scope.startswith("vo:"):
            vos.append(scope.split(":", 1)[1])
        else:
            unrecognised.append(scope)
    if unrecognised:
        raise ValueError(f"Unrecognised scopes: {unrecognised}")

    if not vos:
        available_vo_scopes = [repr(f"vo:{vo}") for vo in config.Registry]
        raise ValueError(
            "No vo scope requested, available values: "
            f"{' '.join(available_vo_scopes)}"
        )
    elif len(vos) > 1:
        raise ValueError(f"Only one vo is allowed but got {vos}")
    else:
        vo = vos[0]
        if vo not in config.Registry:
            raise ValueError(f"VO {vo} is not known to this installation")

    if not groups:
        # TODO: Handle multiple groups correctly
        group = config.Registry[vo].DefaultGroup
    elif len(groups) > 1:
        raise ValueError(f"Only one DIRAC group allowed but got {groups}")
    else:
        group = groups[0]
        if group not in config.Registry[vo].Groups:
            raise ValueError(f"{group} not in {vo} groups")

    if not properties:
        # If there are no properties set get the defaults from the CS
        properties = [str(p) for p in config.Registry[vo].Groups[group].Properties]

    if not set(properties).issubset(available_properties):
        raise ValueError(
            f"{set(properties)-set(available_properties)} are not valid properties"
        )

    return {
        "group": group,
        "properties": properties,
        "vo": vo,
    }


# @overload
# @router.post("/{vo}/token")
# async def token(
#     vo: str,
#     grant_type: Annotated[
#         Literal["urn:ietf:params:oauth:grant-type:device_code"],
#         Form(description="OAuth2 Grant type"),
#     ],
#     client_id: Annotated[str, Form(description="OAuth2 client id")],
#     auth_db: AuthDB,
#     config: Config,
#     device_code: Annotated[str, Form(description="device code for OAuth2 device flow")],
#     code: None,
#     redirect_uri: None,
#     code_verifier: None,
# ) -> TokenResponse:
#     ...


# @overload
# @router.post("/{vo}/token")
# async def token(
#     vo: str,
#     grant_type: Annotated[
#         Literal["authorization_code"],
#         Form(description="OAuth2 Grant type"),
#     ],
#     client_id: Annotated[str, Form(description="OAuth2 client id")],
#     auth_db: AuthDB,
#     config: Config,
#     device_code: None,
#     code: Annotated[str, Form(description="Code for OAuth2 authorization code flow")],
#     redirect_uri: Annotated[
#         str, Form(description="redirect_uri used with OAuth2 authorization code flow")
#     ],
#     code_verifier: Annotated[
#         str,
#         Form(
#             description="Verifier for the code challenge for the OAuth2 authorization flow with PKCE"
#         ),
#     ],
# ) -> TokenResponse:
#     ...


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
    This is the endpoint being pulled by dirac-login when doing the device flow
    """
    legacy_exchange = False

    if grant_type == GrantType.device_code:
        oidc_token_info, scope = await get_oidc_token_info_from_device_flow(
            device_code, client_id, auth_db, settings
        )

    elif grant_type == GrantType.authorization_code:
        oidc_token_info, scope = await get_oidc_token_info_from_authorization_flow(
            code, redirect_uri, code_verifier, auth_db, settings
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
        legacy_exchange=legacy_exchange,
    )


async def get_oidc_token_info_from_device_flow(
    device_code: str | None, client_id: str, auth_db: AuthDB, settings: AuthSettings
):
    """Get OIDC token information from the device flow DB and check few parameters before returning it"""
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
            status.HTTP_400_BAD_REQUEST, {"error": "expired_token"}
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
    redirect_uri: str | None,
    code_verifier: str | None,
    auth_db: AuthDB,
    settings: AuthSettings,
):
    """Get OIDC token information from the authorization flow DB and check few parameters before returning it"""
    assert code is not None
    info = await auth_db.get_authorization_flow(
        code, settings.authorization_flow_expiration_seconds
    )
    if redirect_uri != info["redirect_uri"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid redirect_uri",
        )

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
    """Get OIDC token information from the refresh token DB and check few parameters before returning it"""
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
                status_code=status.HTTP_400_BAD_REQUEST,
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


@router.get("/refresh-tokens")
async def get_refresh_tokens(
    auth_db: AuthDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
) -> list:
    subject: str | None = user_info.sub
    if PROXY_MANAGEMENT in user_info.properties:
        subject = None

    res = await auth_db.get_user_refresh_tokens(subject)
    return res


@router.delete("/refresh-tokens/{jti}")
async def revoke_refresh_token(
    auth_db: AuthDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    jti: str,
) -> str:
    res = await auth_db.get_refresh_token(jti)
    if not res:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="JTI provided does not exist",
        )

    if PROXY_MANAGEMENT not in user_info.properties and user_info.sub != res["sub"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Cannot revoke a refresh token owned by someone else",
        )
    await auth_db.revoke_refresh_token(jti)
    return f"Refresh token {jti} revoked"


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

    uuid = await auth_db.insert_authorization_flow(
        client_id,
        scope,
        "audience",
        code_challenge,
        code_challenge_method,
        redirect_uri,
    )

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
    decrypted_state = decrypt_state(state)
    assert decrypted_state["grant_type"] == GrantType.authorization_code

    id_token = await get_token_from_iam(
        config,
        decrypted_state["vo"],
        code,
        decrypted_state,
        str(request.url.replace(query="")),
    )

    code, redirect_uri = await auth_db.authorization_flow_insert_id_token(
        decrypted_state["uuid"],
        id_token,
        settings.authorization_flow_expiration_seconds,
    )

    return responses.RedirectResponse(
        f"{redirect_uri}?code={code}&state={decrypted_state['external_state']}"
    )


class UserInfoResponse(TypedDict):
    sub: str
    vo: str
    dirac_group: str
    properties: list[SecurityProperty]
    preferred_username: str


@router.get("/userinfo")
async def userinfo(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)]
) -> UserInfoResponse:
    return {
        "sub": user_info.sub,
        "vo": user_info.vo,
        "dirac_group": user_info.dirac_group,
        "properties": user_info.properties,
        "preferred_username": user_info.preferred_username,
    }


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
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid credentials",
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
