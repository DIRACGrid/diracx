from __future__ import annotations

from datetime import datetime, timedelta, timezone
from time import sleep

import pytest
from fastapi.testclient import TestClient
from pytest_httpx import HTTPXMock

from diracx.core.models import PilotSecretConstraints
from diracx.db.sql import AuthDB, PilotAgentsDB
from diracx.db.sql.utils.functions import raw_hash
from diracx.logic.pilots.query import (
    get_pilots_by_stamp,
    get_secrets_by_hashed_secrets,
)

from ..auth.test_standard import _get_tokens, auth_httpx_mock  # noqa: F401

pytestmark = pytest.mark.enabled_dependencies(
    [
        "PilotCredentialsAccessPolicy",
        "PilotManagementAccessPolicy",
        "DevelopmentSettings",
        "AuthDB",
        "AuthSettings",
        "ConfigSource",
        "BaseAccessPolicy",
        "PilotAgentsDB",
    ]
)

MAIN_VO = "lhcb"
DIRAC_CLIENT_ID = "myDIRACClientID"
N = 100


@pytest.fixture
def normal_test_client(client_factory):
    with client_factory.normal_user() as client:
        yield client


@pytest.fixture
def diracx_pilot_client(client_factory):
    with client_factory.diracx_pilot() as client:
        yield client


@pytest.fixture
def non_mocked_hosts(normal_test_client) -> list[str]:
    return [normal_test_client.base_url.host]


@pytest.fixture
async def add_stamps(normal_test_client):
    db = normal_test_client.app.dependency_overrides[PilotAgentsDB.transaction].args[0]

    async with db as pilot_db:
        # Add pilots
        refs = [f"ref_{i}" for i in range(N)]
        stamps = [f"stamp_{i}" for i in range(N)]
        pilot_references = dict(zip(stamps, refs))

        vo = MAIN_VO

        await pilot_db.add_pilots(
            stamps, vo, grid_type="DIRAC", pilot_references=pilot_references
        )

        pilots = await get_pilots_by_stamp(db, stamps)

    return pilots


@pytest.fixture
async def add_secrets_and_time(normal_test_client, add_stamps, secret_duration_sec):
    db = normal_test_client.app.dependency_overrides[AuthDB.transaction].args[0]

    async with db as auth_db:
        # Retrieve the stamps from the add_stamps fixture
        stamps = [pilot["PilotStamp"] for pilot in add_stamps]

        # Add a VO restriction as well as association with a specific pilot
        secrets = [f"AW0nd3rfulS3cr3t_{str(i)}" for i in range(len(stamps))]
        hashed_secrets = [raw_hash(secret) for secret in secrets]
        constraints = {
            hashed_secret: PilotSecretConstraints(VOs=[MAIN_VO], PilotStamps=[stamp])
            for hashed_secret, stamp in zip(hashed_secrets, stamps)
        }

        # Add creds
        await auth_db.insert_unique_secrets(
            hashed_secrets=hashed_secrets, secret_constraints=constraints
        )

        # Associate with pilot
        secrets_obj = await get_secrets_by_hashed_secrets(db, hashed_secrets)

        assert len(secrets_obj) == len(hashed_secrets) == len(stamps)

        # extract_timestamp_from_uuid7(secret_obj["SecretUUID"]) does not work here
        # See #548
        expiration_date = [
            datetime.now(timezone.utc) + timedelta(seconds=secret_duration_sec)
            for secret_obj in secrets_obj
        ]

        await auth_db.set_secret_expirations(
            secret_uuids=[secret_obj["SecretUUID"] for secret_obj in secrets_obj],
            pilot_secret_expiration_dates=expiration_date,
        )

        # Return both non-hashed secrets and stamps
        return {"stamps": stamps, "secrets": secrets}


@pytest.mark.parametrize("secret_duration_sec", [10])
async def test_verify_secret(normal_test_client, add_secrets_and_time):
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

    r = normal_test_client.post("/api/auth/secret-exchange", json=body)

    assert r.status_code == 401, r.json()
    assert r.json()["detail"] == "bad pilot_secret"

    # ----------------- Good password  -----------------

    body = {"pilot_stamp": pilot_stamp, "pilot_secret": secret}

    r = normal_test_client.post("/api/auth/secret-exchange", json=body)

    assert r.status_code == 200, r.json()

    access_token = r.json()["access_token"]
    refresh_token = r.json()["refresh_token"]

    assert access_token is not None
    assert refresh_token is not None

    # -----------------  Wrong ID  -----------------
    body = {"pilot_stamp": "It is a stamp", "pilot_secret": secret}

    r = normal_test_client.post(
        "/api/auth/secret-exchange",
        json=body,
    )

    assert r.status_code == 401
    assert r.json()["detail"] == "bad pilot_stamp"

    # ----------------- Exchange for new tokens -----------------
    body = {"refresh_token": refresh_token, "pilot_stamp": pilot_stamp}
    r = normal_test_client.post(
        "/api/auth/pilot-token",
        json=body,
    )

    assert r.status_code == 200, r.json()

    new_access_token = r.json()["access_token"]
    new_refresh_token = r.json()["refresh_token"]

    # ----------------- Exchange token with old token -----------------
    body = {"refresh_token": refresh_token, "pilot_stamp": pilot_stamp}
    r = normal_test_client.post(
        "/api/auth/pilot-token",
        json=body,
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert r.status_code == 401, r.json()

    # ----------------- Exchange token with new token -----------------
    body = {"refresh_token": new_refresh_token, "pilot_stamp": pilot_stamp}
    r = normal_test_client.post(
        "/api/auth/pilot-token",
        json=body,
        headers={"Authorization": f"Bearer {new_access_token}"},
    )

    # RFC6749
    # https://datatracker.ietf.org/doc/html/rfc6749#section-10.4
    assert r.status_code == 401, r.json()

    # ----------------- Overused Secret -----------------
    body = {"pilot_stamp": pilot_stamp, "pilot_secret": secret}

    r = normal_test_client.post("/api/auth/secret-exchange", json=body)

    assert r.status_code == 401
    assert r.json()["detail"] == "bad pilot_secret"


@pytest.mark.parametrize("secret_duration_sec", [10])
async def test_vacuum_case(normal_test_client, add_secrets_and_time):
    result = add_secrets_and_time
    secrets = result["secrets"]

    pilot_stamp = "this_might_be_totally_unknown"
    secret = secrets[0]

    # ----------------- Good password but unknown stamp -----------------

    body = {"pilot_stamp": pilot_stamp, "pilot_secret": secret}

    r = normal_test_client.post("/api/auth/secret-exchange", json=body)

    assert r.status_code == 401
    assert r.json()["detail"] == "bad pilot_stamp"

    # ----------------- Good password and vacuum case but wrong stamp for the secret -----------------
    # add_secrets_and_time associates secret_n with stamp_n.
    #  Because our pilot_stamp does not have a secret associated to it
    # (or at least one where it can't access), we have to create an "opened" secret (for every stamp)
    # This will be done in the next section

    body = {
        "pilot_stamp": pilot_stamp,
        "pilot_secret": secret,
        "vo": MAIN_VO,
        "grid_type": "test",
        "grid_site": "test",
        "status": "Waiting",
    }

    r = normal_test_client.post("/api/auth/secret-exchange", json=body)

    assert r.status_code == 401, r.json()
    assert r.json()["detail"] == "bad credentials"

    # ----------------- Add secret without restricting it to a certain stamp -----------------
    body = {
        "n": 1,
        "vo": MAIN_VO,
        "expiration_minutes": 1,
        "pilot_secret_use_count_max": 1,
    }

    r = normal_test_client.post(
        "/api/pilots/secrets",
        json=body,
        headers={"Content-Type": "application/json"},
    )
    assert r.status_code == 200, r.json()

    # Format : {"pilot_secret": "...", "pilot_secret_expires_in": ..., "pilot_stamps": None}
    secrets_mapping = r.json()
    secrets = [el["pilot_secret"] for el in secrets_mapping]

    assert len(secrets) == 1

    secret = secrets[0]

    # ----------------- Good password and vacuum case -----------------

    body = {
        "pilot_stamp": pilot_stamp,
        "pilot_secret": secret,
        "vo": MAIN_VO,
        "grid_type": "test",
        "grid_site": "test",
        "status": "Waiting",
    }

    r = normal_test_client.post("/api/auth/secret-exchange", json=body)

    assert r.status_code == 200, r.json()

    access_token = r.json()["access_token"]
    refresh_token = r.json()["refresh_token"]

    assert access_token is not None
    assert refresh_token is not None

    # Get a pilot token, and try to access a pilot endpoint
    r = normal_test_client.get(
        "/api/pilots/internal/pilotinfo",
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert r.status_code == 200


@pytest.mark.parametrize("secret_duration_sec", [2])
async def test_expired_secret(normal_test_client, add_secrets_and_time):
    # Add pilots
    result = add_secrets_and_time
    stamps = result["stamps"]
    secrets = result["secrets"]

    pilot_stamp = stamps[0]
    secret = secrets[0]

    # ----------------- Secret that expired -----------------
    sleep(2)

    body = {"pilot_stamp": pilot_stamp, "pilot_secret": secret}

    r = normal_test_client.post("/api/auth/secret-exchange", json=body)

    assert r.status_code == 401
    assert r.json()["detail"] == "secret expired"

    # ----------------- Secret that expired, but reused -----------------
    # Should be deleted by the verify_pilot_secret

    body = {"pilot_stamp": pilot_stamp, "pilot_secret": secret}

    r = normal_test_client.post("/api/auth/secret-exchange", json=body)

    assert r.status_code == 401
    assert r.json()["detail"] == "bad pilot_secret"


@pytest.mark.parametrize("secret_duration_sec", [10])
async def test_access_user_info_with_pilot_token(
    normal_test_client, add_secrets_and_time
):
    # ----------------- Access user info but with a pilot token -----------------
    # Add pilots
    result = add_secrets_and_time
    stamps = result["stamps"]
    secrets = result["secrets"]

    pilot_stamp = stamps[0]
    secret = secrets[0]
    body = {"pilot_stamp": pilot_stamp, "pilot_secret": secret}
    r = normal_test_client.post("/api/auth/secret-exchange", json=body)

    assert r.status_code == 200, r.json()

    access_token = r.json()["access_token"]
    refresh_token = r.json()["refresh_token"]

    assert access_token is not None
    assert refresh_token is not None

    # Get a pilot token, and try to access a user endpoint
    r = normal_test_client.get(
        "/api/auth/userinfo", headers={"Authorization": f"Bearer {access_token}"}
    )

    assert r.status_code == 401

    # Get a pilot token, and try to access a pilot endpoint
    r = normal_test_client.get(
        "/api/pilots/internal/pilotinfo",
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert r.status_code == 200


async def test_refresh_pilot_token_with_user_token(
    normal_test_client: TestClient,
    auth_httpx_mock: HTTPXMock,  # noqa: F811
):
    # ----------------- Exchange for new tokens but with a user token -----------------
    # This will test that a user can't access a pilot endpoint *by default*
    access_token = normal_test_client.headers["Authorization"]

    refresh_token = _get_tokens(normal_test_client)["refresh_token"]

    assert access_token
    assert refresh_token

    # ----------------- First, with a pilot that does not exist -----------------
    body = {"refresh_token": refresh_token, "pilot_stamp": "stamp_0"}
    r = normal_test_client.post(
        "/api/auth/pilot-token",
        json=body,
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert r.status_code == 401
    assert "not found" in r.json()["detail"]

    # ----------------- Then, with a pilot that does exist -----------------
    # First, we need to create this pilot

    pilot_stamp = "stamp_1"
    body = {"vo": MAIN_VO, "pilot_stamps": [pilot_stamp]}

    # Create a pilot
    r = normal_test_client.post(
        "/api/pilots/",
        json=body,
    )

    assert r.status_code == 200, r.json()

    body = {"refresh_token": refresh_token, "pilot_stamp": "stamp_1"}
    r = normal_test_client.post(
        "/api/auth/pilot-token",
        json=body,
    )

    assert r.status_code == 401
    assert r.json()["detail"] == "This is not a pilot token."


async def test_get_pilot_info_with_user_token(
    normal_test_client: TestClient,
):
    r = normal_test_client.get(
        "/api/pilots/internal/pilotinfo",
    )

    assert r.status_code == 401


@pytest.mark.parametrize("secret_duration_sec", [10])
async def test_refresh_user_token_with_pilot_token(
    normal_test_client, add_secrets_and_time
):
    # Add pilots
    result = add_secrets_and_time
    stamps = result["stamps"]
    secrets = result["secrets"]

    pilot_stamp = stamps[0]
    secret = secrets[0]

    # ----------------- Good password  -----------------

    body = {"pilot_stamp": pilot_stamp, "pilot_secret": secret}

    r = normal_test_client.post("/api/auth/secret-exchange", json=body)

    assert r.status_code == 200, r.json()

    refresh_token = r.json()["refresh_token"]

    request_data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": DIRAC_CLIENT_ID,
    }

    r = normal_test_client.post("/api/auth/token", data=request_data)

    assert r.status_code == 403, r.json()
    assert r.json()["detail"] == "This is not a user token."
