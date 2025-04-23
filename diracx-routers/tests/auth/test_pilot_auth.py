from __future__ import annotations

from datetime import timedelta
from time import sleep

import pytest

from diracx.db.sql.pilot_agents.db import PilotAgentsDB
from diracx.db.sql.utils import hash

pytestmark = pytest.mark.enabled_dependencies(
    [
        "DevelopmentSettings",
        "AuthDB",
        "AuthSettings",
        "ConfigSource",
        "BaseAccessPolicy",
        "PilotAgentsDB",
    ]
)


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


async def test_create_pilot_and_verify_secret(test_client):

    # see https://github.com/DIRACGrid/diracx/blob/78e00aa57f4191034dbf643c7ed2857a93b53f60/diracx-routers/tests/pilots/test_pilot_logger.py#L37
    db = test_client.app.dependency_overrides[PilotAgentsDB.transaction].args[0]

    async with db as pilot_agents_db:
        pilot_stamp = "pilot-stamp"
        # Register a pilot
        await pilot_agents_db.add_pilots_bulk(
            vo="lhcb",
            pilot_stamps=[pilot_stamp],
            grid_type="grid-type",
        )

        secret = "AW0nd3rfulS3cr3t"
        pilot_hashed_secret = hash(secret)

        # Add creds
        secrets_added = await pilot_agents_db.add_pilots_credentials_bulk(
            pilot_stamps=[pilot_stamp],
            pilot_hashed_secrets=[pilot_hashed_secret],
            pilot_secret_use_count_max=1,  # Important later
        )

        assert len(secrets_added) == 1

        secret_added = secrets_added[0]

        expiration_date = secret_added["SecretCreationDate"] + timedelta(seconds=2)

        await pilot_agents_db.set_secret_expirations_bulk(
            secret_ids=[secret_added["SecretID"]],
            pilot_secret_expiration_dates=[expiration_date],
        )

    body = {"pilot_stamp": pilot_stamp, "pilot_secret": secret}

    r = test_client.post("/api/auth/pilot-login", json=body)

    assert r.status_code == 200, r.json()

    access_token = r.json()["access_token"]
    refresh_token = r.json()["refresh_token"]

    assert access_token is not None
    assert refresh_token is not None

    # -----------------  Get pilot info without permissions -----------------
    r = test_client.get(
        "/api/auth/userinfo",
    )

    assert r.status_code == 401

    # -----------------  Get pilot info with access_token  -----------------
    r = test_client.get(
        "/api/auth/userinfo", headers={"Authorization": f"Bearer {access_token}"}
    )

    assert r.status_code == 200

    # -----------------  Get pilot info with wrong access_token  -----------------
    r = test_client.get(
        "/api/auth/userinfo", headers={"Authorization": "Bearer 4dm1n B34r3r"}
    )

    assert r.status_code == 401, r.json()
    assert r.json()["detail"] == "Invalid JWT"

    # -----------------  Wrong password  -----------------
    body = {
        "pilot_stamp": pilot_stamp,
        "pilot_secret": "My 1ncr3d1bl3 t0k3n",
    }

    r = test_client.post("/api/auth/pilot-login", json=body)

    assert r.status_code == 401, r.json()
    assert r.json()["detail"] == "bad pilot_secret"

    # -----------------  Wrong ID  -----------------
    body = {"pilot_stamp": "It is a stamp", "pilot_secret": secret}

    r = test_client.post(
        "/api/auth/pilot-login",
        json=body,
    )

    assert r.status_code == 401
    assert r.json()["detail"] == "bad credentials"

    # ----------------- Exchange for new tokens -----------------
    body = {"refresh_token": refresh_token}
    r = test_client.post(
        "/api/auth/pilot-refresh-token",
        json=body,
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert r.status_code == 200

    new_access_token = r.json()["access_token"]
    new_refresh_token = r.json()["refresh_token"]

    # ----------------- Get info with new token -----------------
    r = test_client.get(
        "/api/auth/userinfo", headers={"Authorization": f"Bearer {new_access_token}"}
    )

    assert r.status_code == 200

    # ----------------- Exchange token with old token -----------------
    body = {"refresh_token": refresh_token}
    r = test_client.post(
        "/api/auth/pilot-refresh-token",
        json=body,
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert r.status_code == 401, r.json()

    # ----------------- Exchange token with new token -----------------
    body = {"refresh_token": new_refresh_token}
    r = test_client.post(
        "/api/auth/pilot-refresh-token",
        json=body,
        headers={"Authorization": f"Bearer {new_access_token}"},
    )

    # RFC6749
    # https://datatracker.ietf.org/doc/html/rfc6749#section-10.4
    assert r.status_code == 401, r.json()

    # ----------------- Overused Secret -----------------
    body = {"pilot_stamp": pilot_stamp, "pilot_secret": secret}

    r = test_client.post("/api/auth/pilot-login", json=body)

    assert r.status_code == 401
    assert r.json()["detail"] == "secret has been overused"

    # ----------------- Secret that expired -----------------
    sleep(2)

    body = {"pilot_stamp": pilot_stamp, "pilot_secret": secret}

    r = test_client.post("/api/auth/pilot-login", json=body)

    assert r.status_code == 401
    assert r.json()["detail"] == "secret expired"


async def test_create_pilots_with_credentials(normal_test_client):
    # Lots of request, to validate that it returns the credentials in the same order as the input references
    pilot_stamps = [f"stamps_{i}" for i in range(100)]
    vo = "lhcb"

    #  -------------- Bulk insert --------------
    body = {"vo": vo, "pilot_stamps": pilot_stamps}

    r = normal_test_client.post(
        "/api/auth/register-new-pilots",
        json=body,
    )

    assert r.status_code == 200, r.json()

    #  -------------- Logins --------------
    pilot_credentials_list = r.json()["pilot_credentials"]
    for credentials in pilot_credentials_list:
        pilot_stamp, secret, _ = credentials.values()

        body = {"pilot_stamp": pilot_stamp, "pilot_secret": secret}

        r = normal_test_client.post(
            "/api/auth/pilot-login",
            json=body,
            headers={"Content-Type": "application/json"},
        )

        assert r.status_code == 200, r.json()

    #  -------------- Register a pilot that already exist, and one that does not --------------

    body = {"vo": vo, "pilot_stamps": [pilot_stamps[0], pilot_stamps[0] + "_new_one"]}

    r = normal_test_client.post(
        "/api/auth/register-new-pilots",
        json=body,
        headers={
            "Content-Type": "application/json",
        },
    )

    assert r.status_code == 409
    assert (
        r.json()["detail"]
        == f"Pilot (pilot_stamps: {{'{pilot_stamps[0]}'}}) already exists"
    )

    #  -------------- Register a pilot that does not exists **but** was called before in an error --------------
    # To prove that, if I tried to register a pilot that does not exist with one that already exists,
    # i can normally add the one that did not exist before (it should not have added it before)
    body = {"vo": vo, "pilot_stamps": [pilot_stamps[0] + "_new_one"]}

    r = normal_test_client.post(
        "/api/auth/register-new-pilots",
        json=body,
        headers={
            "Content-Type": "application/json",
        },
    )

    assert r.status_code == 200
    _, secret, _ = r.json()["pilot_credentials"][0].values()

    #  -------------- Login with a pilot that does not exists **but** was called before in an error --------------

    body = {
        "pilot_stamp": pilot_stamps[0] + "_new_one",
        "pilot_secret": secret,
    }

    r = normal_test_client.post(
        "/api/auth/pilot-login",
        json=body,
        headers={"Content-Type": "application/json"},
    )

    assert r.status_code == 200, r.json()

    #  -------------- Login with a pilot credentials of another pilot --------------

    body = {
        "pilot_stamp": pilot_stamps[0] + "_new_one",
        "pilot_secret": pilot_credentials_list[0][
            "pilot_secret"
        ],  # [0] = first pilot from the list before, [1] = the secret
    }

    r = normal_test_client.post(
        "/api/auth/pilot-login",
        json=body,
        headers={"Content-Type": "application/json"},
    )

    assert r.status_code == 401, r.json()
    assert r.json()["detail"] == "bad credentials"
