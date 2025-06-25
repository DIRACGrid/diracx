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
    ]
)

MAIN_VO = "lhcb"
N = 100


@pytest.fixture
def normal_test_client(client_factory):
    with client_factory.normal_user() as client:
        yield client


async def test_create_pilots(normal_test_client):
    # Lots of request, to validate that it returns the credentials in the same order as the input references
    pilot_stamps = [f"stamps_{i}" for i in range(N)]

    #  -------------- Bulk insert --------------
    body = {"vo": MAIN_VO, "pilot_stamps": pilot_stamps}

    r = normal_test_client.post(
        "/api/pilots/",
        json=body,
    )

    assert r.status_code == 200, r.json()

    #  -------------- Register a pilot that already exists, and one that does not --------------

    body = {
        "vo": MAIN_VO,
        "pilot_stamps": [pilot_stamps[0], pilot_stamps[0] + "_new_one"],
    }

    r = normal_test_client.post(
        "/api/pilots/",
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
        "/api/pilots/",
        json=body,
        headers={
            "Content-Type": "application/json",
        },
    )

    assert r.status_code == 200


async def test_create_pilot_and_delete_it(normal_test_client):
    pilot_stamp = "stamps_1"

    #  -------------- Insert --------------
    body = {"vo": MAIN_VO, "pilot_stamps": [pilot_stamp]}

    # Create a pilot
    r = normal_test_client.post(
        "/api/pilots/",
        json=body,
    )

    assert r.status_code == 200, r.json()

    #  -------------- Duplicate --------------
    # Duplicate because it exists, should have 409
    r = normal_test_client.post(
        "/api/pilots/",
        json=body,
    )

    assert r.status_code == 409, r.json()

    #  -------------- Delete --------------
    params = {"pilot_stamps": [pilot_stamp]}

    # We delete the pilot
    r = normal_test_client.delete(
        "/api/pilots/",
        params=params,
    )

    assert r.status_code == 204

    #  -------------- Insert --------------
    # Create a the same pilot, but works because it does not exist anymore
    r = normal_test_client.post(
        "/api/pilots/",
        json=body,
    )

    assert r.status_code == 200, r.json()


async def test_create_pilot_and_modify_it(normal_test_client):
    pilot_stamps = ["stamps_1", "stamp_2"]

    #  -------------- Insert --------------
    body = {"vo": MAIN_VO, "pilot_stamps": pilot_stamps}

    # Create pilots
    r = normal_test_client.post(
        "/api/pilots/",
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

    r = normal_test_client.patch("/api/pilots/metadata", json=body)

    assert r.status_code == 204

    body = {
        "parameters": [],
        "search": [],
        "sort": [],
        "distinct": True,
    }

    r = normal_test_client.post("/api/pilots/management/search", json=body)
    assert r.status_code == 200, r.json()
    pilot1 = r.json()[0]
    pilot2 = r.json()[1]

    assert pilot1["BenchMark"] == 1.0
    assert pilot1["StatusReason"] == "NewReason"
    assert pilot1["AccountingSent"]
    assert pilot1["Status"] == PilotStatus.WAITING

    assert pilot2["BenchMark"] != pilot1["BenchMark"]
    assert pilot2["StatusReason"] != pilot1["StatusReason"]
    assert pilot2["AccountingSent"] != pilot1["AccountingSent"]
    assert pilot2["Status"] != pilot1["Status"]


async def test_associate_job_with_pilot_and_get_it(normal_test_client):
    pilot_stamps = ["stamps_1", "stamp_2"]

    #  -------------- Insert --------------
    body = {"vo": MAIN_VO, "pilot_stamps": pilot_stamps}

    # Create pilots
    r = normal_test_client.post(
        "/api/pilots/",
        json=body,
    )

    assert r.status_code == 200, r.json()

    # --------------- As DIRAC, associate a job with a pilot --------
