from __future__ import annotations
from typing import Annotated
from uuid import uuid4, UUID

import re
from datetime import datetime, timedelta

from authlib.integrations.starlette_client import OAuth, OAuthError, StarletteOAuth2App
from authlib.jose import JsonWebKey, JsonWebToken, JoseError
from authlib.oidc.core import IDToken
from fastapi import APIRouter, Header, status, HTTPException
from fastapi.security import OpenIdConnect
from pydantic import BaseModel, HttpUrl

from ..config import Registry


class LoginResponse(BaseModel):
    device_auth_endpoint: HttpUrl
    token_endpoint: HttpUrl
    client_id: str


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


oauth = OAuth()
lhcb_iam_client_id = "5c0541bf-85c8-4d7f-b1df-beaeea19ff5b"
oauth.register(
    name="lhcb",
    server_metadata_url=f"{lhcb_iam_endpoint}/.well-known/openid-configuration",
    client_id=lhcb_iam_client_id,
    client_kwargs={"scope": "openid profile email"},
)


oidc_scheme = OpenIdConnect(
    openIdConnectUrl=f"{lhcb_iam_endpoint}/.well-known/openid-configuration"
)


@router.get("/login")
async def login(vo: str) -> LoginResponse:
    """Method called by dirac-login to be redirected to the OpenID endpoint"""

    client = oauth.create_client(vo)
    await client.load_server_metadata()

    # Take these two from CS/.well-known
    device_auth_endpoint = client.server_metadata["device_authorization_endpoint"]
    token_endpoint = client.server_metadata["token_endpoint"]

    # That's a config parameter
    client_id = lhcb_iam_client_id
    return {
        "device_auth_endpoint": device_auth_endpoint,
        "token_endpoint": token_endpoint,
        "client_id": client_id,
    }


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


def create_access_token(payload: dict, expires_delta: timedelta | None = None):
    to_encode = payload.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})

    jwt = JsonWebToken(ALGORITHM)
    encoded_jwt = jwt.encode({"alg": ALGORITHM}, to_encode, SECRET_KEY)
    return encoded_jwt


@router.post("/login")
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
