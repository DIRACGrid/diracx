from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal, Optional
from uuid import UUID, uuid4

import httpx
from authlib.integrations.starlette_client import OAuth, OAuthError, StarletteOAuth2App
from authlib.jose import JoseError, JsonWebKey, JsonWebToken
from authlib.oidc.core import IDToken
from fastapi import (
    APIRouter,
    Cookie,
    Depends,
    Form,
    Header,
    HTTPException,
    Request,
    Response,
    responses,
    status,
)
from fastapi.security import OpenIdConnect
from pydantic import BaseModel

from ..config import Registry

oidc_scheme = OpenIdConnect(
    openIdConnectUrl="http://localhost:8000/.well-known/openid-configuration"
)


class TokenResponse(BaseModel):
    # Base on RFC 6749
    access_token: str
    # refresh_token: str
    expires_in: int
    state: str


router = APIRouter()

lhcb_iam_endpoint = "https://lhcb-auth.web.cern.ch/"
# to get a string like this run:
# openssl rand -hex 32
SECRET_KEY = "21e98a30bb41420dc601dea1dc1f85ecee3b4d702547bea355c07ab44fd7f3c3"
ALGORITHM = "HS256"
ISSUER = "http://lhcbdirac.cern.ch/"
AUDIENCE = "dirac"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


DIRAC_CLIENT_ID = "myDIRACClientID"


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


# @router.get("/login")
# async def login(vo: str) -> LoginResponse:
#     """Method called by dirac-login to be redirected to the OpenID endpoint"""

#     client = oauth.create_client(vo)
#     await client.load_server_metadata()

#     # Take these two from CS/.well-known
#     device_auth_endpoint = client.server_metadata["device_authorization_endpoint"]
#     token_endpoint = client.server_metadata["token_endpoint"]

#     # That's a config parameter
#     client_id = lhcb_iam_client_id
#     return {
#         "device_auth_endpoint": device_auth_endpoint,
#         "token_endpoint": token_endpoint,
#         "client_id": client_id,
#     }


async def parse_jwt(authorization: str, audience: str, oauth2_app: StarletteOAuth2App):
    if match := re.fullmatch(r"Bearer (.+)", authorization):
        token = match.group(1)
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid authorization header",
        )

    metadata = await oauth2_app.load_server_metadata()
    alg_values = metadata.get("id_token_signing_alg_values_supported") or ["RS256"]
    jwt = JsonWebToken(alg_values)
    jwk_set = await oauth2_app.fetch_jwk_set()

    token = jwt.decode(
        token,
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
    diracGroup: str, authorization: Annotated[str, Header()]
) -> TokenResponse:
    """Method called to exchange the OIDC token for a DIRAC generated access token"""
    try:
        token = await parse_jwt(
            authorization=authorization,
            audience=lhcb_iam_client_id,
            oauth2_app=oauth.lhcb,
        )
    except OAuthError:
        raise

    vo = token["organisation_name"]
    subId = token["sub"]
    if subId not in Registry[vo]["Groups"][diracGroup]["members"]:
        raise ValueError(
            f"User is not a member of the requested group ({token['preferred_username']}, {diracGroup})"
        )

    payload = {
        "sub": f"{vo}:{subId}",
        "aud": AUDIENCE,
        "iss": ISSUER,
        "dirac_properties": Registry[vo]["Groups"][diracGroup]["properties"],
        "jti": str(uuid4()),
        "preferred_username": token["preferred_username"],
        "dirac_group": diracGroup,
    }

    return TokenResponse(
        access_token=create_access_token(payload),
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        state="None",
    )


# This should be randomly generated and stored in a DB
generated_user_code = "2QRKPY"
generated_device_code = "b5dfda24-7dc1-498a-9409-82f1c72e6656"
generated_state = "xyzABC12"


@router.post("/{vo}/device")
async def initiate_device_flow(
    vo: str, client_id: str, scope: str, audience: str, request: Request
):
    """Initiate the device flow against DIRAC authorization Server.
    Scope must have exactly one `group` and
    one or more `property` scope.

    Offers the user to go with the browser to
    `auth/<vo>/device?user_code=XYZ`
    """
    device_metadata = {}
    device_metadata["scopes"] = scope.split()
    device_metadata["group"] = "lhcb_user"
    device_metadata["audience"] = audience
    with open("/tmp/data.json", "wt") as f:
        json.dump(device_metadata, f)

    generated_verification_uri_complete = (
        f"{request.url.replace(query={})}?user_code={generated_user_code}"
    )
    generated_verification_uri = str(request.url.replace(query={}))

    return {
        "user_code": generated_user_code,
        "device_code": generated_device_code,
        "verification_uri_complete": generated_verification_uri_complete,
        "verification_uri": generated_verification_uri,
        "expires_in": 600,
        "interval": 5,
    }


async def initiate_authorization_flow_with_iam(vo: str, redirect_uri: str):
    # code_verifier: https://www.rfc-editor.org/rfc/rfc7636#section-4.1
    # 48*2 = 96 characters, which is within 43-128 limits.
    code_verifier = binascii.hexlify(os.urandom(48))

    # code_challenge: https://www.rfc-editor.org/rfc/rfc7636#section-4.2
    code_challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(code_verifier).digest())
        .decode()
        .replace("=", "")
    )

    client = oauth.create_client(vo)
    await client.load_server_metadata()

    # Take these two from CS/.well-known
    authorization_endpoint = client.server_metadata["authorization_endpoint"]

    urlParams = [
        "response_type=code",
        f"code_challenge={code_challenge}",
        "code_challenge_method=S256",
        f"client_id={lhcb_iam_client_id}",
        f"redirect_uri={redirect_uri}",
        "scope=openid%20profile",
        f"state={generated_state}",
    ]
    authorization_flow_url = f"{authorization_endpoint}?{'&'.join(urlParams)}"
    return code_verifier, authorization_flow_url


async def get_token_from_iam(
    vo: str, code: str, state: str, code_verifier: str, redirect_uri: str
) -> str:
    assert state == generated_state

    client = oauth.create_client(vo)
    await client.load_server_metadata()

    # Take these two from CS/.well-known
    token_endpoint = client.server_metadata["token_endpoint"]

    data = {
        "grant_type": "authorization_code",
        "client_id": lhcb_iam_client_id,
        "code": code,
        "code_verifier": code_verifier,
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

    return res.json()["id_token"]


@router.get("/{vo}/device")
async def do_device_flow(
    vo: str, request: Request, response: Response, user_code: Optional[str] = None
):
    """
    This is called as the verification URI for the device flow.
    It will redirect to the actual OpenID server (IAM, CheckIn) to
    perform a authorization code flow.

    We set the user_code obtained from the device flow in a cookie
    to be able to map the authorization flow with the corresponding
    device flow.
    (note: it can't be put as parameter or in the URL)

    TODO: replace cookie with js session storage
    """

    assert user_code == generated_user_code

    redirect_uri = f"{request.url.replace(query='')}/complete"
    code_verifier, authorization_flow_url = await initiate_authorization_flow_with_iam(
        vo, redirect_uri
    )

    with open("/tmp/data.json", "rt") as f:
        device_metadata = json.load(f)

    device_metadata["code_verifier"] = code_verifier.decode()

    with open("/tmp/data.json", "wt") as f:
        json.dump(device_metadata, f)

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
    response: Response,
    code: str,
    state: str,
    user_code: Annotated[str | None, Cookie()] = None,
):
    """
    This the url callbacked by IAM/Checkin after the authorization
    flow was granted.
    It gets us the code we need for the authorization flow, and we
    can map it to the corresponding device flow using the user_code
    in the cookie/session
    """

    with open("/tmp/data.json", "rt") as f:
        device_metadata = json.load(f)

    id_token = await get_token_from_iam(
        vo,
        code,
        state,
        device_metadata["code_verifier"],
        str(request.url.replace(query="")),
    )

    dirac_token = await exchange_token(device_metadata["group"], f"Bearer {id_token}")
    device_metadata["dirac_token"] = dirac_token.dict()

    with open("/tmp/data.json", "wt") as f:
        json.dump(device_metadata, f)

    response.body = b"<h1>Please close the window</h1>"
    response.status_code = 200
    response.media_type = "text/html"
    return response


class DeviceCodeTokenForm(BaseModel):
    grant_type: Literal["urn:ietf:params:oauth:grant-type:device_code"]
    device_code: str
    client_id: str


@router.post("/{vo}/token")
def token(
    vo: str,
    # data: Annotated[DeviceCodeTokenForm, Form()],
    grant_type: Annotated[
        Literal["urn:ietf:params:oauth:grant-type:device_code"]
        | Literal["authorization_code"],
        Form(),
    ],
    client_id: Annotated[str, Form()],
    device_code: Annotated[Optional[str], Form()] = None,
    code: Annotated[Optional[str], Form()] = None,
    redirect_uri: Annotated[Optional[str], Form()] = None,
    code_verifier: Annotated[Optional[str], Form()] = None,
) -> TokenResponse:
    """ " Token endpoint to retrieve the token at the end of a flow.
    This is the endpoint being pulled by dirac-login when doing the device flow
    """
    assert client_id == DIRAC_CLIENT_ID
    if grant_type == "urn:ietf:params:oauth:grant-type:device_code":
        assert device_code == generated_device_code

        device_metadata = json.loads(Path("/tmp/data.json").read_text())
        if "dirac_token" in device_metadata:
            return device_metadata["dirac_token"]
        return Response('{"error": "authorization_pending"}', status_code=400)
        return Response('{"error": "slow_down"}', status_code=400)
        return Response('{"error": "expired_token"}', status_code=400)
        return Response('{"error": "access_denied"}', status_code=400)
    if grant_type == "authorization_code":
        assert code_verifier, code_verifier
        assert redirect_uri == REDIRECT_URI
        assert (
            base64.urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest())
            .decode()
            .strip("=")
            == DIRAC_CODE_CHALLENGE
        )

        assert code == DIRAC_AUTHORIZATION_CODE
        device_metadata = json.loads(Path("/tmp/data.json").read_text())

        return device_metadata["dirac_token"]

    raise NotImplementedError(grant_type)


DIRAC_CODE_CHALLENGE = None
STATE = None
REDIRECT_URI = None
DIRAC_AUTHORIZATION_CODE = "UVW"


@router.get("/{vo}/authorize")
async def authorization_flow(
    vo: str,
    request: Request,
    response: Response,
    response_type: Literal["code"],
    code_challenge: str,
    code_challenge_method: Literal["S256"],
    client_id: str,
    redirect_uri: str,
    scope: str,
    state: str,
):
    global DIRAC_CODE_CHALLENGE, STATE, REDIRECT_URI
    assert client_id == DIRAC_CLIENT_ID
    assert redirect_uri == "http://localhost:8000/docs/oauth2-redirect", redirect_uri

    assert set(scope.split()).issubset(
        {"group:lhcb_user", "property:fc_management", "property:normal_user"}
    )

    DIRAC_CODE_CHALLENGE = code_challenge
    STATE = state
    REDIRECT_URI = redirect_uri

    redirect_uri = f"{request.url.replace(query='')}/complete"
    code_verifier, authorization_flow_url = await initiate_authorization_flow_with_iam(
        vo, redirect_uri
    )

    device_metadata = {"code_verifier": code_verifier.decode()}
    device_metadata["scopes"] = scope.split()
    device_metadata["group"] = "lhcb_user"

    with open("/tmp/data.json", "wt") as f:
        json.dump(device_metadata, f)

    response.status_code = 200
    response.media_type = "text/html"
    response.body = (
        f'<a href="{authorization_flow_url}">click here to login</a>'
    ).encode()
    return response


@router.get("/{vo}/authorize/complete")
async def authorization_flow_complete(vo: str, code: str, state: str, request: Request):
    assert state == generated_state, state

    with open("/tmp/data.json", "rt") as f:
        device_metadata = json.load(f)

    id_token = await get_token_from_iam(
        vo,
        code,
        state,
        device_metadata["code_verifier"],
        str(request.url.replace(query="")),
    )
    dirac_token = await exchange_token(device_metadata["group"], f"Bearer {id_token}")
    device_metadata["dirac_token"] = dirac_token.dict()

    with open("/tmp/data.json", "wt") as f:
        json.dump(device_metadata, f)

    return responses.RedirectResponse(
        f"{REDIRECT_URI}?code={DIRAC_AUTHORIZATION_CODE}&state={STATE}"
    )
