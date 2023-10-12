import base64
import hashlib
import json
import secrets
import time
from typing import Any

import pytest


@pytest.fixture
def legacy_credentials(monkeypatch):
    secret = secrets.token_bytes()
    valid_token = f"diracx:legacy:{base64.urlsafe_b64encode(secret).decode()}"
    monkeypatch.setenv(
        "DIRACX_LEGACY_EXCHANGE_HASHED_API_KEY", hashlib.sha256(secret).hexdigest()
    )
    yield {"Authorization": f"Bearer {valid_token}"}


def _jwt_payload(jwt: str) -> dict[str, Any]:
    header, payload, signature = jwt.split(".")

    # Add padding to the payload, if necessary
    padding = len(payload) % 4
    if padding:
        payload += "=" * (4 - padding)

    # Base64 decode the payload
    decoded_payload = base64.urlsafe_b64decode(payload)

    # Convert the JSON to a Python dictionary
    return json.loads(decoded_payload.decode("utf-8"))


@pytest.mark.parametrize("expires_seconds", [None, 0, 3600, 24 * 3600])
async def test_valid(test_client, legacy_credentials, expires_seconds):
    params = {"preferred_username": "chaen", "scope": "vo:lhcb group:lhcb_user"}
    if expires_seconds is not None:
        params["expires_minutes"] = expires_seconds // 60
    r = test_client.get(
        "/api/auth/legacy-exchange", params=params, headers=legacy_credentials
    )
    assert r.status_code == 200
    access_token = r.json()["access_token"]

    # The refresh token should be valid for 1 hour
    refresh_token = r.json()["refresh_token"]
    refresh_duration = _jwt_payload(refresh_token)["exp"] - time.time()
    if expires_seconds is None:
        assert refresh_duration > 3600 - 5
        assert refresh_duration < 3600
    else:
        assert refresh_duration > expires_seconds - 5
        assert refresh_duration < expires_seconds

    r = test_client.get(
        "/api/auth/userinfo", headers={"Authorization": f"Bearer {access_token}"}
    )
    assert r.status_code == 200
    user_info = r.json()
    assert user_info["sub"] == "lhcb:b824d4dc-1f9d-4ee8-8df5-c0ae55d46041"
    assert user_info["vo"] == "lhcb"
    assert user_info["dirac_group"] == "lhcb_user"
    assert user_info["properties"] == ["NormalUser", "PrivateLimitedDelegation"]


async def test_disabled(test_client):
    r = test_client.get(
        "/api/auth/legacy-exchange",
        params={"preferred_username": "chaen", "scope": "vo:lhcb group:lhcb_user"},
        headers={"Authorization": "Bearer diracx:legacy:ChangeME"},
    )
    assert r.status_code == 503


async def test_no_credentials(test_client, legacy_credentials):
    r = test_client.get(
        "/api/auth/legacy-exchange",
        params={"preferred_username": "chaen", "scope": "vo:lhcb group:lhcb_user"},
        headers={"Authorization": "Bearer invalid"},
    )
    assert r.status_code == 400
    assert r.json()["detail"] == "Invalid authorization header"


async def test_invalid_credentials(test_client, legacy_credentials):
    r = test_client.get(
        "/api/auth/legacy-exchange",
        params={"preferred_username": "chaen", "scope": "vo:lhcb group:lhcb_user"},
        headers={"Authorization": "Bearer invalid"},
    )
    assert r.status_code == 400
    assert r.json()["detail"] == "Invalid authorization header"


async def test_wrong_credentials(test_client, legacy_credentials):
    r = test_client.get(
        "/api/auth/legacy-exchange",
        params={"preferred_username": "chaen", "scope": "vo:lhcb group:lhcb_user"},
        headers={"Authorization": "Bearer diracx:legacy:ChangeME"},
    )
    assert r.status_code == 403
    assert r.json()["detail"] == "Invalid credentials"


async def test_unknown_vo(test_client, legacy_credentials):
    r = test_client.get(
        "/api/auth/legacy-exchange",
        params={"preferred_username": "chaen", "scope": "vo:unknown group:lhcb_user"},
        headers=legacy_credentials,
    )
    assert r.status_code == 400
    assert r.json()["detail"] == "Invalid scope or preferred_username"


async def test_unknown_group(test_client, legacy_credentials):
    r = test_client.get(
        "/api/auth/legacy-exchange",
        params={"preferred_username": "chaen", "scope": "vo:lhcb group:unknown"},
        headers=legacy_credentials,
    )
    assert r.status_code == 400
    assert r.json()["detail"] == "Invalid scope or preferred_username"


async def test_unknown_user(test_client, legacy_credentials):
    r = test_client.get(
        "/api/auth/legacy-exchange",
        params={"preferred_username": "unknown", "scope": "vo:lhcb group:lhcb_user"},
        headers=legacy_credentials,
    )
    assert r.status_code == 400
    assert r.json()["detail"] == "Invalid scope or preferred_username"
