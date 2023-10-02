import base64
import hashlib
import secrets

import pytest


@pytest.fixture
def legacy_credentials(monkeypatch):
    secret = secrets.token_bytes()
    valid_token = f"diracx:legacy:{base64.urlsafe_b64encode(secret).decode()}"
    monkeypatch.setenv(
        "DIRACX_LEGACY_EXCHANGE_HASHED_API_KEY", hashlib.sha256(secret).hexdigest()
    )
    yield {"Authorization": f"Bearer {valid_token}"}


async def test_valid(test_client, legacy_credentials):
    r = test_client.get(
        "/auth/legacy-exchange",
        params={"preferred_username": "chaen", "scope": "vo:lhcb group:lhcb_user"},
        headers=legacy_credentials,
    )
    assert r.status_code == 200
    access_token = r.json()["access_token"]

    r = test_client.get(
        "/auth/userinfo", headers={"Authorization": f"Bearer {access_token}"}
    )
    assert r.status_code == 200
    user_info = r.json()
    assert user_info["sub"] == "lhcb:b824d4dc-1f9d-4ee8-8df5-c0ae55d46041"
    assert user_info["vo"] == "lhcb"
    assert user_info["dirac_group"] == "lhcb_user"
    assert user_info["properties"] == ["NormalUser", "PrivateLimitedDelegation"]


async def test_disabled(test_client):
    r = test_client.get(
        "/auth/legacy-exchange",
        params={"preferred_username": "chaen", "scope": "vo:lhcb group:lhcb_user"},
        headers={"Authorization": "Bearer diracx:legacy:ChangeME"},
    )
    assert r.status_code == 503


async def test_no_credentials(test_client, legacy_credentials):
    r = test_client.get(
        "/auth/legacy-exchange",
        params={"preferred_username": "chaen", "scope": "vo:lhcb group:lhcb_user"},
        headers={"Authorization": "Bearer invalid"},
    )
    assert r.status_code == 400
    assert r.json()["detail"] == "Invalid authorization header"


async def test_invalid_credentials(test_client, legacy_credentials):
    r = test_client.get(
        "/auth/legacy-exchange",
        params={"preferred_username": "chaen", "scope": "vo:lhcb group:lhcb_user"},
        headers={"Authorization": "Bearer invalid"},
    )
    assert r.status_code == 400
    assert r.json()["detail"] == "Invalid authorization header"


async def test_wrong_credentials(test_client, legacy_credentials):
    r = test_client.get(
        "/auth/legacy-exchange",
        params={"preferred_username": "chaen", "scope": "vo:lhcb group:lhcb_user"},
        headers={"Authorization": "Bearer diracx:legacy:ChangeME"},
    )
    assert r.status_code == 403
    assert r.json()["detail"] == "Invalid credentials"


async def test_unknown_vo(test_client, legacy_credentials):
    r = test_client.get(
        "/auth/legacy-exchange",
        params={"preferred_username": "chaen", "scope": "vo:unknown group:lhcb_user"},
        headers=legacy_credentials,
    )
    assert r.status_code == 400
    assert r.json()["detail"] == "Invalid scope or preferred_username"


async def test_unknown_group(test_client, legacy_credentials):
    r = test_client.get(
        "/auth/legacy-exchange",
        params={"preferred_username": "chaen", "scope": "vo:lhcb group:unknown"},
        headers=legacy_credentials,
    )
    assert r.status_code == 400
    assert r.json()["detail"] == "Invalid scope or preferred_username"


async def test_unknown_user(test_client, legacy_credentials):
    r = test_client.get(
        "/auth/legacy-exchange",
        params={"preferred_username": "unknown", "scope": "vo:lhcb group:lhcb_user"},
        headers=legacy_credentials,
    )
    assert r.status_code == 400
    assert r.json()["detail"] == "Invalid scope or preferred_username"
