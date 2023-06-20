from __future__ import annotations

import base64
import hashlib
import json
import re
import secrets
from datetime import datetime, timedelta
from typing import Annotated, Literal, TypedDict
from uuid import UUID, uuid4

import httpx
from authlib.integrations.starlette_client import OAuthError
from authlib.jose import JoseError, JsonWebKey, JsonWebToken
from authlib.oidc.core import IDToken
from cachetools import TTLCache
from fastapi import (
    APIRouter,
    Depends,
    Form,
    HTTPException,
    Request,
    Response,
    responses,
    status,
)
from fastapi.responses import RedirectResponse
from fastapi.security import OpenIdConnect
from pydantic import BaseModel

from diracx.core.config import Config, get_config
from diracx.core.exceptions import (
    DiracHttpResponse,
    ExpiredFlowError,
    PendingAuthorizationError,
)
from diracx.core.properties import SecurityProperty
from diracx.core.secrets import get_secrets
from diracx.db.auth.db import AuthDB, get_auth_db
from diracx.db.auth.schema import FlowStatus

oidc_scheme = OpenIdConnect(
    openIdConnectUrl="http://localhost:8000/.well-known/openid-configuration"
)


class TokenResponse(BaseModel):
    # Base on RFC 6749
    access_token: str
    # refresh_token: str
    expires_in: int
    state: str


router = APIRouter(tags=["auth"])

ISSUER = "http://lhcbdirac.cern.ch/"
AUDIENCE = "dirac"
ACCESS_TOKEN_EXPIRE_MINUTES = 3000
DIRAC_CLIENT_ID = "myDIRACClientID"
# This should be taken dynamically
KNOWN_CLIENTS = {
    DIRAC_CLIENT_ID: {
        "allowed_redirects": ["http://localhost:8000/docs/oauth2-redirect"]
    }
}
# Duration for which the flows against DIRAC AS are valid
DEVICE_FLOW_EXPIRATION_SECONDS = 600
AUTHORIZATION_FLOW_EXPIRATION_SECONDS = 300

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


class UserInfo(AuthInfo):
    # dirac generated vo:sub
    sub: str
    preferred_username: str
    dirac_group: str
    vo: str


async def verify_dirac_token(
    authorization: Annotated[str, Depends(oidc_scheme)]
) -> UserInfo:
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

    secrets = get_secrets()

    try:
        jwt = JsonWebToken(secrets.token_algorithm)
        token = jwt.decode(
            raw_token,
            key=secrets.token_key.jwk,
            claims_options={
                "iss": {"values": [ISSUER]},
                "aud": {"values": [AUDIENCE]},
            },
        )
        token.validate()
    except JoseError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid JWT",
        ) from None

    return UserInfo(
        bearer_token=raw_token,
        token_id=token["jti"],
        properties=token["dirac_properties"],
        sub=token["sub"],
        preferred_username=token["preferred_username"],
        dirac_group=token["dirac_group"],
        vo=token["vo"],
    )


def create_access_token(payload: dict, expires_delta: timedelta | None = None) -> str:
    to_encode = payload.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})

    secrets = get_secrets()
    jwt = JsonWebToken(secrets.token_algorithm)
    encoded_jwt = jwt.encode(
        {"alg": secrets.token_algorithm}, payload, secrets.token_key.jwk
    )
    return encoded_jwt.decode("ascii")


async def exchange_token(
    vo: str, dirac_group: str, id_token: dict[str, str], config: Config
) -> TokenResponse:
    """Method called to exchange the OIDC token for a DIRAC generated access token"""
    sub = id_token["sub"]
    if sub not in config.Registry[vo].Groups[dirac_group].Users:
        raise ValueError(
            f"User is not a member of the requested group ({id_token['preferred_username']}, {dirac_group})"
        )

    payload = {
        "sub": f"{vo}:{sub}",
        "vo": vo,
        "aud": AUDIENCE,
        "iss": ISSUER,
        "dirac_properties": config.Registry[vo].Groups[dirac_group].Properties,
        "jti": str(uuid4()),
        "preferred_username": id_token["preferred_username"],
        "dirac_group": dirac_group,
    }

    return TokenResponse(
        access_token=create_access_token(payload),
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        state="None",
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
    auth_db: Annotated[AuthDB, Depends(get_auth_db)],
    config: Annotated[Config, Depends(get_config)],
) -> InitiateDeviceFlowResponse:
    """Initiate the device flow against DIRAC authorization Server.
    Scope must have exactly up to one `group` (otherwise default) and
    one or more `property` scope.
    If no property, then get default one

    Offers the user to go with the browser to
    `auth/<vo>/device?user_code=XYZ`
    """

    assert client_id in KNOWN_CLIENTS, client_id

    try:
        parse_and_validate_scope(scope, config)
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
        "expires_in": DEVICE_FLOW_EXPIRATION_SECONDS,
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
        "grant_type": "authorization_code",
        "client_id": config.Registry[vo].IdP.ClientID,
        "code": code,
        "code_verifier": state["code_verifier"],
        "redirect_uri": redirect_uri,
    }

    async with httpx.AsyncClient() as c:
        res = await c.post(
            token_endpoint,
            data=data,
            timeout=60,
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
    auth_db: Annotated[AuthDB, Depends(get_auth_db)],
    user_code: str,
    config: Annotated[Config, Depends(get_config)],
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

    # Here we make sure the user_code actualy exists
    scope = await auth_db.device_flow_validate_user_code(
        user_code, DEVICE_FLOW_EXPIRATION_SECONDS
    )
    parsed_scope = parse_and_validate_scope(scope, config)

    redirect_uri = f"{request.url.replace(query='')}/complete"

    state_for_iam = {
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
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
    auth_db: Annotated[AuthDB, Depends(get_auth_db)],
    config: Annotated[Config, Depends(get_config)],
):
    """
    This the url callbacked by IAM/Checkin after the authorization
    flow was granted.
    It gets us the code we need for the authorization flow, and we
    can map it to the corresponding device flow using the user_code
    in the cookie/session
    """
    decrypted_state = decrypt_state(state)
    assert (
        decrypted_state["grant_type"] == "urn:ietf:params:oauth:grant-type:device_code"
    )

    id_token = await get_token_from_iam(
        config,
        decrypted_state["vo"],
        code,
        decrypted_state,
        str(request.url.replace(query="")),
    )
    await auth_db.device_flow_insert_id_token(
        decrypted_state["user_code"], id_token, DEVICE_FLOW_EXPIRATION_SECONDS
    )

    return responses.RedirectResponse(f"{request.url.replace(query='')}/finished")


@router.get("/device/complete/finished")
def finished(response: Response):
    response.body = b"<h1>Please close the window</h1>"
    response.status_code = 200
    response.media_type = "text/html"
    return response


class DeviceCodeTokenForm(BaseModel):
    grant_type: Literal["urn:ietf:params:oauth:grant-type:device_code"]
    device_code: str
    client_id: str


class ScopeInfoDict(TypedDict):
    group: str
    properties: list[str]
    vo: str


def parse_and_validate_scope(scope: str, config: Config) -> ScopeInfoDict:
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
            raise ValueError(f"VO {vo} is not known to his installation")

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
        properties = [p.value for p in config.Registry[vo].Groups[group].Properties]

    if not set(properties).issubset(SecurityProperty):
        raise ValueError(
            f"{set(properties)-set(SecurityProperty)} are not valid properties"
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
#     auth_db: Annotated[AuthDB, Depends(get_auth_db)],
#     config: Annotated[Config, Depends(get_config)],
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
#     auth_db: Annotated[AuthDB, Depends(get_auth_db)],
#     config: Annotated[Config, Depends(get_config)],
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
    grant_type: Annotated[
        Literal["authorization_code"]
        | Literal["urn:ietf:params:oauth:grant-type:device_code"],
        Form(description="OAuth2 Grant type"),
    ],
    client_id: Annotated[str, Form(description="OAuth2 client id")],
    auth_db: Annotated[AuthDB, Depends(get_auth_db)],
    config: Annotated[Config, Depends(get_config)],
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
) -> TokenResponse:
    """ " Token endpoint to retrieve the token at the end of a flow.
    This is the endpoint being pulled by dirac-login when doing the device flow
    """

    if grant_type == "urn:ietf:params:oauth:grant-type:device_code":
        assert device_code is not None
        try:
            info = await auth_db.get_device_flow(
                device_code, DEVICE_FLOW_EXPIRATION_SECONDS
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

    elif grant_type == "authorization_code":
        assert code is not None
        info = await auth_db.get_authorization_flow(
            code, AUTHORIZATION_FLOW_EXPIRATION_SECONDS
        )
        if redirect_uri != info["redirect_uri"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid redirect_uri",
            )

        try:
            assert code_verifier is not None
            code_challenge = (
                base64.urlsafe_b64encode(
                    hashlib.sha256(code_verifier.encode()).digest()
                )
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

    else:
        raise NotImplementedError(f"Grant type not implemented {grant_type}")

    # TODO: use HTTPException while still respecting the standard format
    # required by the RFC
    if info["status"] != FlowStatus.READY:
        # That should never ever happen
        raise NotImplementedError(f"Unexpected flow status {info['status']!r}")

    parsed_scope = parse_and_validate_scope(info["scope"], config)

    return await exchange_token(
        parsed_scope["vo"], parsed_scope["group"], info["id_token"], config
    )


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
    auth_db: Annotated[AuthDB, Depends(get_auth_db)],
    config: Annotated[Config, Depends(get_config)],
):
    assert client_id in KNOWN_CLIENTS, client_id
    assert redirect_uri in KNOWN_CLIENTS[client_id]["allowed_redirects"]

    try:
        parsed_scope = parse_and_validate_scope(scope, config)
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
        "grant_type": "authorization_code",
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
    auth_db: Annotated[AuthDB, Depends(get_auth_db)],
    config: Annotated[Config, Depends(get_config)],
):
    decrypted_state = decrypt_state(state)
    assert decrypted_state["grant_type"] == "authorization_code"

    id_token = await get_token_from_iam(
        config,
        decrypted_state["vo"],
        code,
        decrypted_state,
        str(request.url.replace(query="")),
    )

    code, redirect_uri = await auth_db.authorization_flow_insert_id_token(
        decrypted_state["uuid"], id_token, AUTHORIZATION_FLOW_EXPIRATION_SECONDS
    )

    return responses.RedirectResponse(
        f"{redirect_uri}?code={code}&state={decrypted_state['external_state']}"
    )
