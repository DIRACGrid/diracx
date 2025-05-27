from __future__ import annotations

import pytest

from diracx.core.models import PilotFieldsMapping, PilotStatus

pytestmark = pytest.mark.enabled_dependencies(
    [
        "PilotCredentialsAccessPolicy",
        "DevelopmentSettings",
        "AuthDB",
        "AuthSettings",
        "ConfigSource",
        "BaseAccessPolicy",
        "PilotAgentsDB",
        "PilotManagementAccessPolicy",
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


async def test_create_pilots_with_credentials(normal_test_client):
    # Lots of request, to validate that it returns the credentials in the same order as the input references
    pilot_stamps = [f"stamps_{i}" for i in range(N)]

    #  -------------- Bulk insert --------------
    body = {"vo": MAIN_VO, "pilot_stamps": pilot_stamps}

    r = normal_test_client.post(
        "/api/pilot_management/",
        json=body,
    )

    assert r.status_code == 200, r.json()

    #  -------------- Logins --------------
    pilot_credentials_list = r.json()
    for credentials in pilot_credentials_list:
        pilot_stamp, secret, _ = credentials.values()

        body = {"pilot_stamp": pilot_stamp, "pilot_secret": secret}

        r = normal_test_client.post(
            "/api/pilots/token",
            json=body,
            headers={"Content-Type": "application/json"},
        )

        assert r.status_code == 200, r.json()

    #  -------------- Register a pilot that already exist, and one that does not --------------

    body = {
        "vo": MAIN_VO,
        "pilot_stamps": [pilot_stamps[0], pilot_stamps[0] + "_new_one"],
    }

    r = normal_test_client.post(
        "/api/pilot_management/",
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
    body = {"vo": MAIN_VO, "pilot_stamps": [pilot_stamps[0] + "_new_one"]}

    r = normal_test_client.post(
        "/api/pilot_management/",
        json=body,
        headers={
            "Content-Type": "application/json",
        },
    )

    assert r.status_code == 200
    _, secret, _ = r.json()[0].values()

    #  -------------- Login with a pilot that does not exists **but** was called before in an error --------------

    body = {
        "pilot_stamp": pilot_stamps[0] + "_new_one",
        "pilot_secret": secret,
    }

    r = normal_test_client.post(
        "/api/pilots/token",
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
        "/api/pilots/token",
        json=body,
        headers={"Content-Type": "application/json"},
    )

    assert r.status_code == 401, r.json()
    assert r.json()["detail"] == "bad pilot_secret"


async def test_create_secrets_and_login(normal_test_client):

    pilot_stamps = [f"stamps_{i}" for i in range(N)]

    #  -------------- Create N secrets. --------------

    body = {
        "n": N,
        "vo": MAIN_VO,
        "expiration_minutes": 1,
        "pilot_secret_use_count_max": 2 * N,  # Used later
    }

    r = normal_test_client.post(
        "/api/pilot_management/fields/secrets",
        json=body,
        headers={"Content-Type": "application/json"},
    )

    assert r.status_code == 200, r.json()

    # Format : {"pilot_secret": "...", "pilot_secret_expires_in": ..., "pilot_stamps": None}
    secrets_mapping = r.json()

    secrets = [el["pilot_secret"] for el in secrets_mapping]

    assert len(secrets) == N

    #  -------------- Create pilot *without* secrets --------------

    body = {"vo": MAIN_VO, "pilot_stamps": pilot_stamps, "generate_secrets": False}

    r = normal_test_client.post(
        "/api/pilot_management/",
        json=body,
    )

    assert r.status_code == 200, r.json()

    #  -------------- Associate pilot with bad secrets --------------

    body = {"pilot_stamps": pilot_stamps, "pilot_secrets": ["bad_secret"]}

    r = normal_test_client.patch(
        "/api/pilot_management/fields/secrets",
        json=body,
        headers={"Content-Type": "application/json"},
    )

    assert r.status_code == 400
    assert r.json()["detail"] == "one of the secrets does not exist"

    #  -------------- Associate pilot with secrets --------------

    body = {"pilot_stamps": pilot_stamps, "pilot_secrets": secrets}

    r = normal_test_client.patch(
        "/api/pilot_management/fields/secrets",
        json=body,
        headers={"Content-Type": "application/json"},
    )

    assert r.status_code == 204
    #  -------------- Login with the right credentials --------------

    for stamp, secret in zip(pilot_stamps, secrets):

        body = {"pilot_secret": secret, "pilot_stamp": stamp}

        r = normal_test_client.post(
            "/api/pilots/token",
            json=body,
            headers={"Content-Type": "application/json"},
        )

        assert r.status_code == 200, r.json()

    #  -------------- Login with the wrong credentials --------------

    body = {"pilot_secret": secrets[1], "pilot_stamp": pilot_stamps[0]}

    r = normal_test_client.post(
        "/api/pilots/token",
        json=body,
        headers={"Content-Type": "application/json"},
    )

    assert r.status_code == 401, r.json()

    #  -------------- Associate everyone to secret[1] --------------

    # Allowed by the router to avoid sending thousands of the same secret, if we want bunch of pilots to share a secret
    body = {
        "pilot_stamps": pilot_stamps,
        "pilot_secrets": [secrets[1]],
    }

    r = normal_test_client.patch(
        "/api/pilot_management/fields/secrets",
        json=body,
        headers={"Content-Type": "application/json"},
    )

    assert r.status_code == 204

    #  -------------- Login with the right credentials --------------
    for stamp in pilot_stamps:

        body = {"pilot_secret": secrets[1], "pilot_stamp": stamp}

        r = normal_test_client.post(
            "/api/pilots/token",
            json=body,
            headers={"Content-Type": "application/json"},
        )

        assert r.status_code == 200, r.json()


async def test_create_pilot_and_delete_it(normal_test_client):
    pilot_stamp = "stamps_1"

    #  -------------- Insert --------------
    body = {"vo": MAIN_VO, "pilot_stamps": [pilot_stamp]}

    # Create a pilot
    r = normal_test_client.post(
        "/api/pilot_management/",
        json=body,
    )

    assert r.status_code == 200, r.json()

    #  -------------- Duplicate --------------
    # Duplicate because it exists, should have 409
    r = normal_test_client.post(
        "/api/pilot_management/",
        json=body,
    )

    assert r.status_code == 409, r.json()

    #  -------------- Delete --------------
    params = {"pilot_stamps": [pilot_stamp]}

    # We delete the pilot
    r = normal_test_client.delete(
        "/api/pilot_management/",
        params=params,
    )

    assert r.status_code == 204

    #  -------------- Insert --------------
    # Create a the same pilot, but works because he does not exist anymore
    r = normal_test_client.post(
        "/api/pilot_management/",
        json=body,
    )

    assert r.status_code == 200, r.json()


async def test_create_pilot_and_modify_it(normal_test_client):
    pilot_stamps = ["stamps_1", "stamp_2"]

    #  -------------- Insert --------------
    body = {"vo": MAIN_VO, "pilot_stamps": pilot_stamps}

    # Create pilots
    r = normal_test_client.post(
        "/api/pilot_management/",
        json=body,
    )

    assert r.status_code == 200, r.json()

    #  -------------- Modify --------------
    # We modify only the first pilot
    body = {
        "pilot_stamps_to_fields_mapping": [
            PilotFieldsMapping(
                PilotStamp=pilot_stamps[0],
                BenchMark=1.0,
                StatusReason="NewReason",
                AccountingSent=True,
                Status=PilotStatus.WAITING,
            ).model_dump(exclude_unset=True)
        ]
    }

    r = normal_test_client.patch("/api/pilot_management/fields", json=body)

    assert r.status_code == 204

    body = {
        "parameters": [],
        "search": [],
        "sort": [],
        "distinct": True,
    }

    r = normal_test_client.post("/api/pilot_management/search/pilots", json=body)
    assert r.status_code == 200, r.json()
    pilot1 = r.json()[0]
    pilot2 = r.json()[1]

    assert pilot1["BenchMark"] == 1.0
    assert pilot1["StatusReason"] == "NewReason"
    assert pilot1["AccountingSent"]
    assert pilot1["Status"] == "Waiting"

    assert pilot2["BenchMark"] != pilot1["BenchMark"]
    assert pilot2["StatusReason"] != pilot1["StatusReason"]
    assert pilot2["AccountingSent"] != pilot1["AccountingSent"]
    assert pilot2["Status"] != pilot1["Status"]
