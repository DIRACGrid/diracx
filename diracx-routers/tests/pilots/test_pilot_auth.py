from __future__ import annotations

from datetime import timedelta
from time import sleep

import pytest
from fastapi.testclient import TestClient
from pytest_httpx import HTTPXMock

from diracx.core.utils import extract_timestamp_from_uuid7
from diracx.db.sql.pilot_agents.db import PilotAgentsDB
from diracx.db.sql.utils.functions import raw_hash

from ..auth.test_standard import _get_tokens, auth_httpx_mock  # noqa: F401

pytestmark = pytest.mark.enabled_dependencies(
    [
        "PilotCredentialsAccessPolicy",
        "DevelopmentSettings",
        "AuthDB",
        "AuthSettings",
        "ConfigSource",
        "BaseAccessPolicy",
        "PilotAgentsDB",
        "PilotLogsDB",
    ]
)

MAIN_VO = "lhcb"
N = 100


@pytest.fixture
def test_client(client_factory):
    with client_factory.unauthenticated() as client:
        yield client


@pytest.fixture
def normal_test_client(client_factory):
    with client_factory.normal_user() as client:
        yield client


@pytest.fixture
def non_mocked_hosts(test_client) -> list[str]:
    return [test_client.base_url.host]


@pytest.fixture
async def add_stamps(test_client):

    db = test_client.app.dependency_overrides[PilotAgentsDB.transaction].args[0]

    async with db as pilot_agents_db:

        # Add pilots
        refs = [f"ref_{i}" for i in range(N)]
        stamps = [f"stamp_{i}" for i in range(N)]
        pilot_references = dict(zip(stamps, refs))

        vo = MAIN_VO

        await pilot_agents_db.add_pilots_bulk(
            stamps, vo, grid_type="DIRAC", pilot_references=pilot_references
        )

        pilots = await pilot_agents_db.get_pilots_by_stamp_bulk(stamps)

        return pilots


@pytest.fixture
async def add_secrets_and_time(test_client, add_stamps, secret_duration_sec):

    db = test_client.app.dependency_overrides[PilotAgentsDB.transaction].args[0]

    async with db as pilot_agents_db:
        # Retrieve the stamps from the add_stamps fixture
        stamps = [pilot["PilotStamp"] for pilot in add_stamps]

        secrets = [f"AW0nd3rfulS3cr3t_{str(i)}" for i in range(len(stamps))]
        hashed_secrets = [raw_hash(secret) for secret in secrets]

        # Add creds
        await pilot_agents_db.insert_unique_secrets_bulk(
            hashed_secrets=hashed_secrets, vo=MAIN_VO
        )

        # Associate with pilot
        secrets_obj = await pilot_agents_db.get_secrets_by_hashed_secrets_bulk(
            hashed_secrets
        )

        assert len(secrets_obj) == len(hashed_secrets) == len(stamps)

        # Associate pilot with its secret
        pilot_to_secret_uuid_mapping_values = [
            {
                "b_PilotSecretUUID": secret["SecretUUID"],
                "b_PilotStamp": stamp,
            }
            for secret, stamp in zip(secrets_obj, stamps)
        ]
        await pilot_agents_db.associate_pilots_with_secrets_bulk(
            pilot_to_secret_uuid_mapping_values
        )

        expiration_date = [
            extract_timestamp_from_uuid7(secret_obj["SecretUUID"])
            + timedelta(seconds=secret_duration_sec)
            for secret_obj in secrets_obj
        ]

        await pilot_agents_db.set_secret_expirations_bulk(
            secret_uuids=[secret_obj["SecretUUID"] for secret_obj in secrets_obj],
            pilot_secret_expiration_dates=expiration_date,
        )

        # Return both non-hashed secrets and stamps
        return {"stamps": stamps, "secrets": secrets}


@pytest.mark.parametrize("secret_duration_sec", [10])
async def test_verify_secret(test_client, add_secrets_and_time):

    # Add pilots
    result = add_secrets_and_time
    stamps = result["stamps"]
    secrets = result["secrets"]

    pilot_stamp = stamps[0]
    secret = secrets[0]

    # -----------------  Wrong password  -----------------
    body = {
        "pilot_stamp": pilot_stamp,
        "pilot_secret": "My 1ncr3d1bl3 t0k3n",
    }

    r = test_client.post("/api/pilots/token", json=body)

    assert r.status_code == 401, r.json()
    assert r.json()["detail"] == "bad pilot_secret"

    # ----------------- Good password  -----------------

    body = {"pilot_stamp": pilot_stamp, "pilot_secret": secret}

    r = test_client.post("/api/pilots/token", json=body)

    assert r.status_code == 200, r.json()

    access_token = r.json()["access_token"]
    refresh_token = r.json()["refresh_token"]

    assert access_token is not None
    assert refresh_token is not None

    # -----------------  Wrong ID  -----------------
    body = {"pilot_stamp": "It is a stamp", "pilot_secret": secret}

    r = test_client.post(
        "/api/pilots/token",
        json=body,
    )

    assert r.status_code == 401
    assert r.json()["detail"] == "bad pilot_stamp"

    # ----------------- Exchange for new tokens -----------------
    body = {"refresh_token": refresh_token}
    r = test_client.post(
        "/api/pilots/refresh-token",
        json=body,
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert r.status_code == 200

    new_access_token = r.json()["access_token"]
    new_refresh_token = r.json()["refresh_token"]

    # ----------------- Exchange token with old token -----------------
    body = {"refresh_token": refresh_token}
    r = test_client.post(
        "/api/pilots/refresh-token",
        json=body,
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert r.status_code == 401, r.json()

    # ----------------- Exchange token with new token -----------------
    body = {"refresh_token": new_refresh_token}
    r = test_client.post(
        "/api/pilots/refresh-token",
        json=body,
        headers={"Authorization": f"Bearer {new_access_token}"},
    )

    # RFC6749
    # https://datatracker.ietf.org/doc/html/rfc6749#section-10.4
    assert r.status_code == 401, r.json()

    # ----------------- Overused Secret -----------------
    body = {"pilot_stamp": pilot_stamp, "pilot_secret": secret}

    r = test_client.post("/api/pilots/token", json=body)

    assert r.status_code == 401
    assert r.json()["detail"] == "bad pilot_secret"


@pytest.mark.parametrize("secret_duration_sec", [2])
async def test_expired_secret(test_client, add_secrets_and_time):

    # Add pilots
    result = add_secrets_and_time
    stamps = result["stamps"]
    secrets = result["secrets"]

    pilot_stamp = stamps[0]
    secret = secrets[0]

    # ----------------- Secret that expired -----------------
    sleep(2)

    body = {"pilot_stamp": pilot_stamp, "pilot_secret": secret}

    r = test_client.post("/api/pilots/token", json=body)

    assert r.status_code == 401
    assert r.json()["detail"] == "secret expired"

    # ----------------- Secret that expired, but reused -----------------
    # Should be deleted by the verify_pilot_secret

    body = {"pilot_stamp": pilot_stamp, "pilot_secret": secret}

    r = test_client.post("/api/pilots/token", json=body)

    assert r.status_code == 401
    assert r.json()["detail"] == "bad pilot_secret"


@pytest.mark.parametrize("secret_duration_sec", [10])
async def test_access_user_info_with_pilot_token(test_client, add_secrets_and_time):
    # ----------------- Access user info but with a pilot token -----------------
    # Add pilots
    result = add_secrets_and_time
    stamps = result["stamps"]
    secrets = result["secrets"]

    pilot_stamp = stamps[0]
    secret = secrets[0]
    body = {"pilot_stamp": pilot_stamp, "pilot_secret": secret}
    r = test_client.post("/api/pilots/token", json=body)

    assert r.status_code == 200, r.json()

    access_token = r.json()["access_token"]
    refresh_token = r.json()["refresh_token"]

    assert access_token is not None
    assert refresh_token is not None

    # Get a pilot token, and try to access a user endpoint
    r = test_client.get(
        "/api/auth/userinfo", headers={"Authorization": f"Bearer {access_token}"}
    )

    assert r.status_code == 401


async def test_refresh_pilot_token_with_user_token(
    normal_test_client: TestClient, auth_httpx_mock: HTTPXMock  # noqa: F811
):
    # ----------------- Exchange for new tokens but with a user token -----------------
    # This will test that a user can't access a pilot endpoint *by default*
    access_token = normal_test_client.headers["Authorization"]

    refresh_token = _get_tokens(normal_test_client)["refresh_token"]

    assert access_token
    assert refresh_token

    body = {"refresh_token": refresh_token}
    r = normal_test_client.post(
        "/api/pilots/refresh-token",
        json=body,
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert r.status_code == 401
