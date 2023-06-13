import base64
import hashlib
import secrets
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import pytest_asyncio
from pytest_httpx import HTTPXMock

from diracx.core.config import Config
from diracx.routers.auth import (
    _server_metadata_cache,
    get_server_metadata,
    parse_and_validate_scope,
)

DIRAC_CLIENT_ID = "myDIRACClientID"


@pytest.fixture
def non_mocked_hosts(test_client) -> list[str]:
    return [test_client.base_url.host]


@pytest_asyncio.fixture
async def auth_httpx_mock(httpx_mock: HTTPXMock, monkeypatch):
    data_dir = Path(__file__).parent.parent / "data"
    path = "lhcb-auth.web.cern.ch/.well-known/openid-configuration"
    httpx_mock.add_response(url=f"https://{path}", text=(data_dir / path).read_text())

    server_metadata = await get_server_metadata(f"https://{path}")

    def custom_response(request: httpx.Request):
        if b"&code=valid-code&" in request.content:
            return httpx.Response(status_code=200, json={"id_token": "my-id-token"})
        return httpx.Response(status_code=401)

    httpx_mock.add_callback(custom_response, url=server_metadata["token_endpoint"])

    monkeypatch.setattr("diracx.routers.auth.parse_id_token", fake_parse_id_token)

    yield httpx_mock

    _server_metadata_cache.clear()


async def fake_parse_id_token(raw_id_token: str, audience: str, *args, **kwargs):
    if raw_id_token == "my-id-token":
        return {
            "aud": "5c0541bf-85c8-4d7f-b1df-beaeea19ff5b",
            "cern_person_id": "705305",
            "email": "christophe.haen@cern.ch",
            "exp": 1680613292,
            "iat": 1680612692,
            "iss": "https://lhcb-auth.web.cern.ch/",
            "jti": "38dbb060-19ad-4a77-9c54-15901b96e286",
            "kid": "rsa1",
            "name": "CHRISTOPHE DENIS HAEN",
            "organisation_name": "lhcb",
            "preferred_username": "chaen",
            "sub": "b824d4dc-1f9d-4ee8-8df5-c0ae55d46041",
            "wlcg.ver": "1.0",
        }
    raise NotImplementedError(raw_id_token)


@pytest.mark.asyncio
async def test_authorization_flow(
    test_client, auth_httpx_mock: HTTPXMock, fake_secrets
):
    code_verifier = secrets.token_hex()
    code_challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest())
        .decode()
        .replace("=", "")
    )

    r = test_client.get(
        "/auth/authorize",
        params={
            "response_type": "code",
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
            "client_id": DIRAC_CLIENT_ID,
            "redirect_uri": "http://localhost:8000/docs/oauth2-redirect",
            "scope": "property:NormalUser",
            "state": "external-state",
        },
        follow_redirects=False,
    )
    assert r.status_code == 307, r.text
    query_paramers = parse_qs(urlparse(r.headers["Location"]).query)
    redirect_uri = query_paramers["redirect_uri"][0]
    state = query_paramers["state"][0]

    # Check that an invalid code returns an error
    r = test_client.get(redirect_uri, params={"code": "invalid-code", "state": state})
    assert r.status_code == 401, r.text

    # Check that an invalid state returns an error
    r = test_client.get(
        redirect_uri, params={"code": "invalid-code", "state": "invalid-state"}
    )
    assert r.status_code == 400, r.text
    assert "Invalid state" in r.text

    # See if a valid code works
    r = test_client.get(
        redirect_uri,
        params={"code": "valid-code", "state": state},
        follow_redirects=False,
    )
    assert r.status_code == 307, r.text
    assert urlparse(r.headers["Location"]).netloc == "localhost:8000"
    assert urlparse(r.headers["Location"]).path == "/docs/oauth2-redirect"
    query_paramers = parse_qs(urlparse(r.headers["Location"]).query)
    assert query_paramers["state"][0] == "external-state"
    code = query_paramers["code"][0]

    _get_token(
        test_client,
        {
            "grant_type": "authorization_code",
            "code": code,
            "state": state,
            "client_id": DIRAC_CLIENT_ID,
            "redirect_uri": "http://localhost:8000/docs/oauth2-redirect",
            "code_verifier": code_verifier,
        },
    )


@pytest.mark.asyncio
async def test_device_flow(test_client, auth_httpx_mock: HTTPXMock):
    # Initiate the device flow (would normally be done from CLI)
    r = test_client.post(
        "/auth/device",
        params={
            "client_id": DIRAC_CLIENT_ID,
            "audience": "Dirac server",
            "scope": "group:lhcb_user property:FileCatalogManagement property:NormalUser",
        },
    )
    assert r.status_code == 200, r.json()
    data = r.json()
    assert data["user_code"]
    assert data["device_code"]
    assert data["verification_uri_complete"]
    assert data["verification_uri"]
    assert data["expires_in"] == 600

    # Check that token requests return "authorization_pending"
    r = test_client.post(
        "/auth/token",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            "device_code": data["device_code"],
            "client_id": DIRAC_CLIENT_ID,
        },
    )
    assert r.status_code == 400, r.json()
    assert r.json()["error"] == "authorization_pending"

    # Open the DIRAC login page and ensure it redirects to the IdP
    r = test_client.get(data["verification_uri_complete"], follow_redirects=False)
    assert r.status_code == 307, r.text
    login_url = r.headers["Location"]
    assert "/authorize?response_type=code" in login_url
    query_paramers = parse_qs(urlparse(login_url).query)
    redirect_uri = query_paramers["redirect_uri"][0]
    state = query_paramers["state"][0]

    # Check that an invalid code returns an error
    r = test_client.get(redirect_uri, params={"code": "invalid-code", "state": state})
    assert r.status_code == 401, r.text

    # Check that an invalid state returns an error
    r = test_client.get(
        redirect_uri, params={"code": "invalid-code", "state": "invalid-state"}
    )
    assert r.status_code == 400, r.text
    assert "Invalid state" in r.text

    # See if a valid code works
    r = test_client.get(redirect_uri, params={"code": "valid-code", "state": state})
    assert r.status_code == 200, r.text
    assert "Please close the window" in r.text

    # Ensure a valid code does not work a second time
    r = test_client.get(redirect_uri, params={"code": "valid-code", "state": state})
    assert r.status_code == 400, r.text

    _get_token(
        test_client,
        {
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            "device_code": data["device_code"],
            "client_id": DIRAC_CLIENT_ID,
        },
    )


def _get_token(test_client, request_data):
    # Check that token request now works
    r = test_client.post("/auth/token", data=request_data)
    assert r.status_code == 200, r.json()
    response_data = r.json()
    assert response_data["access_token"]
    # TODO assert response_data["refresh_token"]
    assert response_data["expires_in"]
    assert response_data["state"]

    # Ensure the token request doesn't work a second time
    r = test_client.post("/auth/token", data=request_data)
    assert r.status_code == 400, r.json()
    assert r.json()["detail"] == "Code was already used"

    return response_data


@pytest.mark.parametrize(
    "vos, groups, scope, expected",
    [
        [
            ["lhcb"],
            ["lhcb_user"],
            "group:lhcb_user",
            {"group": "lhcb_user", "properties": ["NormalUser"], "vo": "lhcb"},
        ],
        [
            ["lhcb"],
            ["lhcb_user"],
            "vo:lhcb group:lhcb_user",
            {"group": "lhcb_user", "properties": ["NormalUser"], "vo": "lhcb"},
        ],
    ],
)
def test_parse_scopes(vos, groups, scope, expected):
    # TODO: Extend test for extra properties
    config = Config.parse_obj(
        {
            "DIRAC": {},
            "Registry": {
                vo: {
                    "DefaultGroup": "lhcb_user",
                    "IdP": {"URL": "https://idp.invalid", "ClientID": "test-idp"},
                    "Users": {},
                    "Groups": {
                        group: {"Properties": ["NormalUser"], "Users": []}
                        for group in groups
                    },
                }
                for vo in vos
            },
            "Operations": {"Defaults": {}},
        }
    )

    assert parse_and_validate_scope(scope, config) == expected
