from __future__ import annotations

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
    Form,
    Header,
    HTTPException,
    Request,
    Response,
    status,
)
from fastapi.security import OpenIdConnect
from pydantic import BaseModel

from ..config import Registry


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
lhcb_iam_client_id = "5c0541bf-85c8-4d7f-b1df-beaeea19ff5b"
lhcb_iam_client_secret = os.environ["LHCB_IAM_CLIENT_SECRET"]
oauth.register(
    name="lhcb",
    server_metadata_url=f"{lhcb_iam_endpoint}/.well-known/openid-configuration",
    client_id=lhcb_iam_client_id,
    client_kwargs={"scope": "openid profile email"},
)


oidc_scheme = OpenIdConnect(
    openIdConnectUrl=f"{lhcb_iam_endpoint}/.well-known/openid-configuration"
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


async def verify_dirac_token(authorization: Annotated[str, Header()]) -> UserInfo:
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

    client = oauth.create_client(vo)
    await client.load_server_metadata()

    # Take these two from CS/.well-known
    authorization_endpoint = client.server_metadata["authorization_endpoint"]

    response.set_cookie(key="user_code", value=user_code)
    response.status_code = 200
    response.media_type = "text/html"
    response.body = (
        f'<a href="{authorization_endpoint}?redirect_uri={request.url.replace(query="")}/complete'
        f'&response_type=code&client_id={lhcb_iam_client_id}">click here to login</a>'
    ).encode()
    return response


@router.get("/{vo}/device/complete")
async def finish_device_flow(
    vo: str,
    request: Request,
    response: Response,
    code: str,
    user_code: Annotated[str | None, Cookie()] = None,
):
    """
    This the url callbacked by IAM/Checkin after the authorization
    flow was granted.
    It gets us the code we need for the authorization flow, and we
    can map it to the corresponding device flow using the user_code
    in the cookie/session
    """
    response.delete_cookie("user_code")

    client = oauth.create_client(vo)
    await client.load_server_metadata()

    # Take these two from CS/.well-known
    token_endpoint = client.server_metadata["token_endpoint"]

    data = {
        "grant_type": "authorization_code",
        # "client_id": lhcb_iam_client_id,
        # "client_secret": lhcb_iam_client_secret,
        "code": code,
        "redirect_uri": str(request.url.replace(query="")),
    }

    async with httpx.AsyncClient() as c:
        res = await c.post(
            token_endpoint, data=data, auth=(lhcb_iam_client_id, lhcb_iam_client_secret)
        )

        res.raise_for_status()

    with open("/tmp/data.json", "rt") as f:
        device_metadata = json.load(f)

    dirac_token = await exchange_token(
        device_metadata["group"], f"Bearer {res.json()['id_token']}"
    )
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
        Literal["urn:ietf:params:oauth:grant-type:device_code"], Form()
    ],
    device_code: Annotated[str, Form()],
    client_id: Annotated[str, Form()],
) -> TokenResponse:
    """ " Token endpoint to retrieve the token at the end of a flow.
    This is the endpoint being pulled by dirac-login when doing the device flow
    """

    if grant_type == "urn:ietf:params:oauth:grant-type:device_code":
        assert device_code == generated_device_code
        assert client_id == DIRAC_CLIENT_ID
        device_metadata = json.loads(Path("/tmp/data.json").read_text())
        if "dirac_token" in device_metadata:
            return device_metadata["dirac_token"]
        return Response('{"error": "authorization_pending"}', status_code=400)
        return Response('{"error": "slow_down"}', status_code=400)
        return Response('{"error": "expired_token"}', status_code=400)
        return Response('{"error": "access_denied"}', status_code=400)
    raise NotImplementedError(grant_type)
