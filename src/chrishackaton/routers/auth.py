from __future__ import annotations

from fastapi import APIRouter, Header
from fastapi.security import OpenIdConnect
from pydantic import BaseModel, HttpUrl
import re
from datetime import datetime, timedelta

from authlib.jose import JsonWebKey, JsonWebToken
from authlib.oidc.core import IDToken

from authlib.integrations.starlette_client import OAuth, OAuthError


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
        raise ValueError("Invalid authorization header")

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
    diracGroup: str, authorization: str = Header(...)
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
            f"User is not a member of the requested group ({token['prefered_username']}, {diracGroup})"
        )

    payload = {
        "sub": f"{vo}:{subId}",
        "aud": "dirac",
        "dirac_properties": Registry[vo]["Groups"][diracGroup]["properties"],
    }

    return TokenResponse(
        access_token=create_access_token(payload),
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        state="None",
    )
