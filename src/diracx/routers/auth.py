from __future__ import annotations

import base64
import hashlib
import json
import re
import secrets
from datetime import datetime, timedelta
from typing import Annotated, Literal, Optional
from uuid import UUID, uuid4

import httpx
from authlib.integrations.starlette_client import OAuth, OAuthError, StarletteOAuth2App
from authlib.jose import JoseError, JsonWebKey, JsonWebToken
from authlib.oidc.core import IDToken
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
from fastapi.security import OpenIdConnect
from pydantic import BaseModel

from diracx.core.config import Config, get_config
from diracx.core.exceptions import (
    ExpiredFlowError,
    PendingAuthorizationError,
)
from diracx.core.properties import SecurityProperty
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


lhcb_iam_endpoint = "https://lhcb-auth.web.cern.ch/"
# to get a string like this run:
# openssl rand -hex 32
SECRET_KEY = "21e98a30bb41420dc601dea1dc1f85ecee3b4d702547bea355c07ab44fd7f3c3"
ALGORITHM = "HS256"
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

SID_TO_USERNAME = {"b824d4dc-1f9d-4ee8-8df5-c0ae55d46041": "chaen"}

# Duration for which the flows against DIRAC AS are valid
DEVICE_FLOW_EXPIRATION_SECONDS = 600
AUTHORIZATION_FLOW_EXPIRATION_SECONDS = 300

oauth = OAuth()
# chris-hack-a-ton
# lhcb_iam_client_id = "5c0541bf-85c8-4d7f-b1df-beaeea19ff5b"
# chris-hack-a-ton-2
lhcb_iam_client_id = "d396912e-2f04-439b-8ae7-d8c585a34790"
# lhcb_iam_client_secret = os.environ["LHCB_IAM_CLIENT_SECRET"]
oauth.register(
    name="lhcb",
    server_metadata_url=f"{lhcb_iam_endpoint}/.well-known/openid-configuration",
    client_id=lhcb_iam_client_id,
    client_kwargs={"scope": "openid profile email"},
)


async def parse_id_token(
    raw_id_token: str, audience: str, oauth2_app: StarletteOAuth2App
):
    metadata = await oauth2_app.load_server_metadata()
    alg_values = metadata.get("id_token_signing_alg_values_supported") or ["RS256"]
    jwt = JsonWebToken(alg_values)
    jwk_set = await oauth2_app.fetch_jwk_set()

    token = jwt.decode(
        raw_id_token,
        key=JsonWebKey.import_key_set(jwk_set),
        claims_cls=IDToken,
        claims_options={
            "iss": {"values": [metadata["issuer"]]},
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
    properties: list[str]


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

    try:
        jwt = JsonWebToken(ALGORITHM)
        token = jwt.decode(
            raw_token,
            key=SECRET_KEY,
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

    jwt = JsonWebToken(ALGORITHM)
    encoded_jwt = jwt.encode({"alg": ALGORITHM}, to_encode, SECRET_KEY).decode("ascii")
    return encoded_jwt


async def exchange_token(
    dirac_group: str, id_token: dict[str:str], config: Config
) -> TokenResponse:
    """Method called to exchange the OIDC token for a DIRAC generated access token"""

    vo = id_token["organisation_name"]
    subId = SID_TO_USERNAME[id_token["sub"]]
    if subId not in config.Registry.Groups[vo][dirac_group].Users:
        raise ValueError(
            f"User is not a member of the requested group ({id_token['preferred_username']}, {dirac_group})"
        )

    payload = {
        "sub": f"{vo}:{subId}",
        "vo": vo,
        "aud": AUDIENCE,
        "iss": ISSUER,
        "dirac_properties": config.Registry.Groups[vo][dirac_group].Properties,
        "jti": str(uuid4()),
        "preferred_username": id_token["preferred_username"],
        "dirac_group": dirac_group,
    }

    return TokenResponse(
        access_token=create_access_token(payload),
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        state="None",
    )


@router.post("/{vo}/device")
async def initiate_device_flow(
    vo: str,
    client_id: str,
    scope: str,
    audience: str,
    request: Request,
    auth_db: Annotated[AuthDB, Depends(get_auth_db)],
    config: Annotated[Config, Depends(get_config)],
):
    """Initiate the device flow against DIRAC authorization Server.
    Scope must have exactly up to one `group` (otherwise default) and
    one or more `property` scope.
    If no property, then get default one

    Offers the user to go with the browser to
    `auth/<vo>/device?user_code=XYZ`
    """

    assert client_id in KNOWN_CLIENTS, client_id

    try:
        parse_and_validate_scope(scope, vo, config)
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
        "verification_uri": verification_uri,
        "expires_in": DEVICE_FLOW_EXPIRATION_SECONDS,
    }


async def initiate_authorization_flow_with_iam(
    vo: str, redirect_uri: str, state: dict[str, str]
):
    # code_verifier: https://www.rfc-editor.org/rfc/rfc7636#section-4.1
    code_verifier = secrets.token_hex()

    # code_challenge: https://www.rfc-editor.org/rfc/rfc7636#section-4.2
    code_challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest())
        .decode()
        .replace("=", "")
    )

    client = oauth.create_client(vo)
    await client.load_server_metadata()

    # Take these two from CS/.well-known
    authorization_endpoint = client.server_metadata["authorization_endpoint"]

    # TODO : encrypt it for good
    encrypted_state = base64.urlsafe_b64encode(
        json.dumps(state | {"code_verifier": code_verifier}).encode()
    ).decode()

    urlParams = [
        "response_type=code",
        f"code_challenge={code_challenge}",
        "code_challenge_method=S256",
        f"client_id={lhcb_iam_client_id}",
        f"redirect_uri={redirect_uri}",
        "scope=openid%20profile",
        f"state={encrypted_state}",
    ]
    authorization_flow_url = f"{authorization_endpoint}?{'&'.join(urlParams)}"
    return authorization_flow_url


async def get_token_from_iam(vo: str, code: str, state: str, redirect_uri: str) -> str:
    client = oauth.create_client(vo)
    await client.load_server_metadata()

    # Take these two from CS/.well-known
    token_endpoint = client.server_metadata["token_endpoint"]

    data = {
        "grant_type": "authorization_code",
        "client_id": lhcb_iam_client_id,
        "code": code,
        "code_verifier": state["code_verifier"],
        "redirect_uri": redirect_uri,
    }

    async with httpx.AsyncClient() as c:
        res = await c.post(
            token_endpoint,
            data=data,
            # auth=(lhcb_iam_client_id, "123")
        )
        if res.status_code >= 400:
            raise NotImplementedError(res, res.text)

    raw_id_token = res.json()["id_token"]
    # Extract the payload and verify it
    try:
        id_token = await parse_id_token(
            raw_id_token=raw_id_token,
            audience=lhcb_iam_client_id,
            oauth2_app=getattr(oauth, vo),
        )
        if id_token["organisation_name"] != vo:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="vo does not match organization_name",
            )
    except OAuthError:
        raise

    return id_token


@router.get("/{vo}/device")
async def do_device_flow(
    vo: str,
    request: Request,
    response: Response,
    auth_db: Annotated[AuthDB, Depends(get_auth_db)],
    user_code: Optional[str] = None,
):
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
    await auth_db.device_flow_validate_user_code(
        user_code, DEVICE_FLOW_EXPIRATION_SECONDS
    )

    redirect_uri = f"{request.url.replace(query='')}/complete"

    state_for_iam = {
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        "user_code": user_code,
    }

    authorization_flow_url = await initiate_authorization_flow_with_iam(
        vo, redirect_uri, state_for_iam
    )

    response.status_code = 200
    response.media_type = "text/html"
    response.body = (
        f'<a href="{authorization_flow_url}">click here to login</a>'
    ).encode()
    return response


@router.get("/{vo}/device/complete")
async def finish_device_flow(
    vo: str,
    request: Request,
    code: str,
    state: str,
    auth_db: Annotated[AuthDB, Depends(get_auth_db)],
):
    """
    This the url callbacked by IAM/Checkin after the authorization
    flow was granted.
    It gets us the code we need for the authorization flow, and we
    can map it to the corresponding device flow using the user_code
    in the cookie/session
    """
    # TODO: There have been better schemes like rot13
    decrypted_state = json.loads(base64.urlsafe_b64decode(state).decode())
    assert (
        decrypted_state["grant_type"] == "urn:ietf:params:oauth:grant-type:device_code"
    )

    id_token = await get_token_from_iam(
        vo,
        code,
        decrypted_state,
        str(request.url.replace(query="")),
    )
    await auth_db.device_flow_insert_id_token(
        decrypted_state["user_code"], id_token, DEVICE_FLOW_EXPIRATION_SECONDS
    )

    return responses.RedirectResponse(f"{request.url.replace(query='')}/finished")


@router.get("/{vo}/device/complete/finished")
def finished(vo: str, response: Response):
    response.body = b"<h1>Please close the window</h1>"
    response.status_code = 200
    response.media_type = "text/html"
    return response


class DeviceCodeTokenForm(BaseModel):
    grant_type: Literal["urn:ietf:params:oauth:grant-type:device_code"]
    device_code: str
    client_id: str


def parse_and_validate_scope(scope: str, vo: str, config: Config) -> dict[str, str]:
    """
    Check:
        * At most one group
        * group belongs to VO
        * properties are known
    return dict with group and properties

    :raises:
        * ValueError in case the scope isn't valide
    """
    scopes = set(scope.split(" "))

    parsed_scope = {}

    groups = []
    properties = []
    unrecognised = []
    for scope in scopes:
        if scope.startswith("group:"):
            groups.append(scope.split(":", 1)[1])
        elif scope.startswith("property:"):
            properties.append(scope.split(":", 1)[1])
        else:
            unrecognised.append(scope)
    if unrecognised:
        raise ValueError(f"Unrecognised scopes: {unrecognised}")

    if not groups:
        group = config.DIRAC.DefaultGroup[vo]
    elif len(groups) > 1:
        raise ValueError(f"Only one DIRAC group allowed but got {groups}")
    else:
        group = groups[0]
        if group not in config.Registry.Groups[vo]:
            raise ValueError(f"{group} not in {vo} groups")
    parsed_scope["group"] = group

    if not set(properties).issubset(SecurityProperty):
        raise ValueError(
            f"{set(properties)-set(SecurityProperty)} are not valid properties"
        )

    parsed_scope["properties"] = properties

    return parsed_scope


@router.post("/{vo}/token")
async def token(
    vo: str,
    # data: Annotated[DeviceCodeTokenForm, Form()],
    grant_type: Annotated[
        Literal["urn:ietf:params:oauth:grant-type:device_code"]
        | Literal["authorization_code"],
        Form(description="OAuth2 Grant type"),
    ],
    client_id: Annotated[str, Form(description="OAuth2 client id")],
    auth_db: Annotated[AuthDB, Depends(get_auth_db)],
    config: Annotated[Config, Depends(get_config)],
    device_code: Annotated[
        Optional[str], Form(description="device code for OAuth2 device flow")
    ] = None,
    code: Annotated[
        Optional[str], Form(description="Code for OAuth2 authorization code flow")
    ] = None,
    redirect_uri: Annotated[
        Optional[str],
        Form(description="redirect_uri used with OAuth2 authorization code flow"),
    ] = None,
    code_verifier: Annotated[
        Optional[str],
        Form(
            description="Verifier for the code challenge for the OAuth2 authorization flow with PKCE"
        ),
    ] = None,
) -> TokenResponse:
    """ " Token endpoint to retrieve the token at the end of a flow.
    This is the endpoint being pulled by dirac-login when doing the device flow
    """

    if grant_type == "urn:ietf:params:oauth:grant-type:device_code":
        try:
            info = await auth_db.get_device_flow(
                device_code, DEVICE_FLOW_EXPIRATION_SECONDS
            )
        except PendingAuthorizationError:
            # TODO: use HTTPException while still respecting the standard format
            # required by the RFC

            return Response(
                '{"error": "authorization_pending"}',
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        except ExpiredFlowError:
            return Response(
                '{"error": "expired_token"}',
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        if info["client_id"] != client_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Bad client_id",
            )

        # return Response('{"error": "slow_down"}', status_code=400)
        # return Response('{"error": "expired_token"}', status_code=400)

    elif grant_type == "authorization_code":
        info = await auth_db.get_authorization_flow(
            code, AUTHORIZATION_FLOW_EXPIRATION_SECONDS
        )
        if redirect_uri != info["redirect_uri"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid redirect_uri",
            )

        try:
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

    parsed_scope = parse_and_validate_scope(info["scope"], vo, config)

    return await exchange_token(parsed_scope["group"], info["id_token"], config)


@router.get("/{vo}/authorize")
async def authorization_flow(
    vo: str,
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
        parse_and_validate_scope(scope, vo, config)
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
        vo, f"{request.url.replace(query='')}/complete", state_for_iam
    )

    return responses.RedirectResponse(authorization_flow_url)


@router.get("/{vo}/authorize/complete")
async def authorization_flow_complete(
    vo: str,
    code: str,
    state: str,
    request: Request,
    auth_db: Annotated[AuthDB, Depends(get_auth_db)],
):
    # TODO: There have been better schemes like rot13
    decrypted_state = json.loads(base64.urlsafe_b64decode(state).decode())
    assert decrypted_state["grant_type"] == "authorization_code"

    id_token = await get_token_from_iam(
        vo,
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
