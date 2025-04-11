from __future__ import annotations

from datetime import timedelta
from random import shuffle

import pytest
from pytest_httpx import HTTPXMock

from diracx.db.sql.pilot_agents.db import PilotAgentsDB
from diracx.db.sql.utils import hash

from .test_standard import _get_tokens

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
            pilot_refs=[pilot_reference],
            grid_type="grid-type",
        )

        pilots = await pilot_agents_db.get_pilots_by_references_bulk([pilot_reference])

        assert len(pilots) == 1

        pilot = pilots[0]

        pilot_id = pilot["PilotID"]

        # Add credentials to this pilot
        date_added = await pilot_agents_db.add_pilots_credentials(
            pilot_ids=[pilot_id], pilot_hashed_secrets=[pilot_hashed_secret]
        )

        assert len(date_added) == 1

        date = date_added[0]

        expiration_date = date + timedelta(seconds=2)

        await pilot_agents_db.set_pilot_credentials_expiration(
            pilot_ids=[pilot_id], pilot_secret_expiration_dates=[expiration_date]
        )

    request_data = {"pilot_job_reference": pilot_reference, "pilot_secret": secret}

    r = test_client.post(
        "/api/auth/pilot-login",
        params=request_data,
        headers={"Content-Type": "application/json"},
    )

    assert r.status_code == 200, r.json()

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
    assert r.json()["detail"] == "bad pilot_id / pilot_secret or secret has expired"

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


async def test_create_pilots_with_credentials(test_client, auth_httpx_mock: HTTPXMock):
    # Lots of request, to validate that it returns the credentials in the same order as the input references
    pilot_refs = [f"ref_{i}" for i in range(100)]
    vo = "lhcb"
    token = _get_tokens(test_client)["access_token"]

    #  -------------- Bulk insert --------------
    request_data = {"vo": vo}
    body = {"pilot_references": pilot_refs}

    r = test_client.post(
        "/api/auth/register-new-pilots",
        params=request_data,
        json=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )

    assert r.status_code == 200

    #  -------------- Logins --------------

    pairs = list(zip(pilot_refs, r.json()["credentials"]))
    # Shuffle it to prove that credentials are well associated
    shuffle(pairs)

    for pilot_reference, secret in pairs:
        request_data = {"pilot_job_reference": pilot_reference, "pilot_secret": secret}

        r = test_client.post(
            "/api/auth/pilot-login",
            params=request_data,
            headers={"Content-Type": "application/json"},
        )

        assert r.status_code == 200, r.json()

    #  -------------- Register a pilot that already exist, and one that does not --------------

    request_data = {"vo": vo}
    body = {"pilot_references": [pilot_refs[0], pilot_refs[0] + "_new_one"]}

    r = test_client.post(
        "/api/auth/register-new-pilots",
        params=request_data,
        json=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )

    assert r.status_code == 409
    assert r.json()["detail"] == f"Pilot (Ref: {{'{pilot_refs[0]}'}}) already exists"

    #  -------------- Register a pilot that does not exists **but** was called before in an error --------------
    # To prove that, if I tried to register a pilot that does not exist with one that already exists,
    # i can normally add the one that did not exist before (it should not have added it before)

    request_data = {"vo": vo}
    body = {"pilot_references": [pilot_refs[0] + "_new_one"]}

    r = test_client.post(
        "/api/auth/register-new-pilots",
        params=request_data,
        json=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )

    assert r.status_code == 200
    secret = r.json()["credentials"]

    #  -------------- Login with a pilot that does not exists **but** was called before in an error --------------

    request_data = {
        "pilot_job_reference": pilot_refs[0] + "_new_one",
        "pilot_secret": secret,
    }

    r = test_client.post(
        "/api/auth/pilot-login",
        params=request_data,
        headers={"Content-Type": "application/json"},
    )

    assert r.status_code == 200, r.json()

    #  -------------- Login with a pilot credentials of another pilot --------------

    request_data = {
        "pilot_job_reference": pilot_refs[0] + "_new_one",
        "pilot_secret": pairs[0][
            1
        ],  # [0] = first pilot from the list before, [1] = the secret
    }

    r = test_client.post(
        "/api/auth/pilot-login",
        params=request_data,
        headers={"Content-Type": "application/json"},
    )

    assert r.status_code == 401, r.json()
    assert r.json()["detail"] == "bad pilot_id / pilot_secret or secret has expired"
