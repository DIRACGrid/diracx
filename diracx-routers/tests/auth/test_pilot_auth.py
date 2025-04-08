from __future__ import annotations

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
        "RegisteredPilotAccessPolicy",
    ]
)


@pytest.fixture
def test_client(client_factory):
    with client_factory.unauthenticated() as client:
        yield client


@pytest.fixture
def non_mocked_hosts(test_client) -> list[str]:
    return [test_client.base_url.host]


async def test_create_pilot_and_verify_secret(test_client):

    # see https://github.com/DIRACGrid/diracx/blob/78e00aa57f4191034dbf643c7ed2857a93b53f60/diracx-routers/tests/pilots/test_pilot_logger.py#L37
    db = test_client.app.dependency_overrides[PilotAgentsDB.transaction].args[0]

    # Add a pilot vo
    pilot_vo = "lhcb"
    pilot_reference = "pilot-test-ref"

    secret = "AW0nd3rfulS3cr3t"
    pilot_hashed_secret = hash(secret)

    async with db as pilot_agents_db:
        # Register a pilot
        await pilot_agents_db.add_pilot_references(
            vo=pilot_vo,
            pilot_ref=[pilot_reference],
            grid_type="grid-type",
        )

        pilot = await pilot_agents_db.get_pilot_by_reference(pilot_reference)

        pilot_id = pilot["PilotID"]

        # Add credentials to this pilot
        await pilot_agents_db.add_pilot_credentials(
            pilot_id=pilot_id, pilot_hashed_secret=pilot_hashed_secret
        )

    request_data = {"pilot_job_reference": pilot_reference, "pilot_secret": secret}

    r = test_client.post(
        "/api/auth/pilot-login",
        params=request_data,
        headers={"Content-Type": "application/json"},
    )

    assert r.status_code == 200

    access_token = r.json()["access_token"]
    refresh_token = r.json()["refresh_token"]

    assert access_token is not None
    assert refresh_token is not None

    # -----------------  Get pilot info without permissions -----------------
    r = test_client.get(
        "/api/pilots/info",
    )

    assert r.status_code == 401

    # -----------------  Get pilot info with access_token  -----------------
    r = test_client.get(
        "/api/pilots/info", headers={"Authorization": f"Bearer {access_token}"}
    )

    assert r.status_code == 200

    # -----------------  Get pilot info with wrong access_token  -----------------
    r = test_client.get(
        "/api/pilots/info", headers={"Authorization": "Bearer 4dm1n B34r3r"}
    )

    assert r.status_code == 401, r.json()
    assert r.json()["detail"] == "Invalid JWT"

    # -----------------  Wrong password  -----------------
    request_data = {
        "pilot_job_reference": pilot_reference,
        "pilot_secret": "My 1ncr3d1bl3 t0k3n",
    }

    r = test_client.post(
        "/api/auth/pilot-login",
        params=request_data,
        headers={"Content-Type": "application/json"},
    )

    assert r.status_code == 401, r.json()
    assert r.json()["detail"] == "bad pilot_id / pilot_secret"

    # -----------------  Wrong ID  -----------------
    request_data = {"pilot_job_reference": "It is a reference", "pilot_secret": secret}

    r = test_client.post(
        "/api/auth/pilot-login",
        params=request_data,
        headers={"Content-Type": "application/json"},
    )

    assert r.status_code == 401
    assert r.json()["detail"] == "bad pilot_id / pilot_secret"

    # ----------------- Exchange for new tokens -----------------
    request_data = {"refresh_token": refresh_token}
    r = test_client.post(
        "/api/auth/pilot-refresh-token",
        params=request_data,
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert r.status_code == 200

    new_access_token = r.json()["access_token"]
    new_refresh_token = r.json()["refresh_token"]

    # ----------------- Get info with new token -----------------
    r = test_client.get(
        "/api/pilots/info", headers={"Authorization": f"Bearer {new_access_token}"}
    )

    assert r.status_code == 200

    # ----------------- Exchange token with old token -----------------
    request_data = {"refresh_token": refresh_token}
    r = test_client.post(
        "/api/auth/pilot-refresh-token",
        params=request_data,
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert r.status_code == 401, r.json()

    # ----------------- Exchange token with new token -----------------
    request_data = {"refresh_token": new_refresh_token}
    r = test_client.post(
        "/api/auth/pilot-refresh-token",
        params=request_data,
        headers={"Authorization": f"Bearer {new_access_token}"},
    )

    # RFC6749
    # https://datatracker.ietf.org/doc/html/rfc6749#section-10.4
    assert r.status_code == 401, r.json()
