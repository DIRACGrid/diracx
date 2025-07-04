from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import update

from diracx.core.models import (
    PilotFieldsMapping,
    PilotStatus,
    ScalarSearchOperator,
    ScalarSearchSpec,
)
from diracx.db.sql import PilotAgentsDB
from diracx.db.sql.pilots.schema import PilotAgents

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
        "JobDB"
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
    body = {"pilot_stamps": pilot_stamps}

    r = normal_test_client.post(
        "/api/pilots/",
        json=body,
    )

    assert r.status_code == 200, r.json()

    #  -------------- Register a pilot that already exists, and one that does not --------------

    body = {
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
    body = {"pilot_stamps": [pilot_stamps[0] + "_new_one"]}

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
    body = {"pilot_stamps": [pilot_stamp]}

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
    body = {"pilot_stamps": pilot_stamps}

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

    r = normal_test_client.post("/api/pilots/search", json=body)
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


async def test_associate_job_with_pilot_and_get_it(normal_test_client: TestClient):
    pilot_stamps = ["stamps_1", "stamp_2"]

    #  -------------- Insert --------------
    body = {"pilot_stamps": pilot_stamps}

    # Create pilots
    r = normal_test_client.post(
        "/api/pilots/",
        json=body,
    )

    assert r.status_code == 200, r.json()

    # --------------- As DIRAC, associate a job with a pilot --------
    job_ids = [1, 2]
    body = {"pilot_stamp": pilot_stamps[0], "job_ids": job_ids}

    # Create pilots
    r = normal_test_client.patch(
        "/api/pilots/jobs",
        json=body,
    )

    assert r.status_code == 204

    # -------------- Redo it, expect 409 (Conflict) ---------------------
    job_ids = [1, 2, 3]  # Note for next test : add 3
    body = {"pilot_stamp": pilot_stamps[0], "job_ids": job_ids}

    # Create pilots
    r = normal_test_client.patch(
        "/api/pilots/jobs",
        json=body,
    )

    assert r.status_code == 409

    # -------------- Add 3 ---------------------
    body = {"pilot_stamp": pilot_stamps[0], "job_ids": [3]}

    # Create pilots
    r = normal_test_client.patch(
        "/api/pilots/jobs",
        json=body,
    )

    assert r.status_code == 204

    # -------------- Add with unknown pilot ---------------------
    body = {"pilot_stamp": "stampounet", "job_ids": job_ids}

    # Create pilots
    r = normal_test_client.patch(
        "/api/pilots/jobs",
        json=body,
    )

    assert r.status_code == 400

    # -------------- Get its jobs ---------------------
    r = normal_test_client.get(
        "/api/pilots/jobs", params={"pilot_stamp": pilot_stamps[0]}
    )

    assert r.status_code == 200
    assert r.json() == job_ids

    # -------------- Get the other pilot's jobs ---------------------
    r = normal_test_client.get(
        "/api/pilots/jobs", params={"pilot_stamp": pilot_stamps[1]}
    )

    assert r.status_code == 200
    assert r.json() == []

    # -------------- Get pilots associated to job 1 ---------------------
    r = normal_test_client.get("/api/pilots/jobs", params={"job_id": job_ids[0]})

    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1
    expected_pilot_id = r.json()[0]

    # -------------- Get pilot info to verify that its id is expected_pilot_id ---------------------
    condition = ScalarSearchSpec(
        parameter="PilotID",
        operator=ScalarSearchOperator.EQUAL,
        value=expected_pilot_id,
    )

    r = normal_test_client.post(
        "/api/pilots/search",
        json={"parameters": [], "search": [condition], "sorts": []},
    )

    assert r.status_code == 200, r.json()
    assert len(r.json()) == 1
    assert r.json()[0]["PilotStamp"] == pilot_stamps[0]


@pytest.mark.asyncio
async def test_delete_pilots_by_age_and_stamp(normal_test_client):
    # Generate 100 pilot stamps
    pilot_stamps = [f"stamp_{i}" for i in range(100)]

    # -------------- Insert all pilots --------------
    body = {"pilot_stamps": pilot_stamps}
    r = normal_test_client.post("/api/pilots/", json=body)
    assert r.status_code == 200, r.json()

    # -------------- Modify last 50 pilots' fields --------------
    to_modify = pilot_stamps[50:]
    mappings = []
    for idx, stamp in enumerate(to_modify):
        # First 25 of modified set to ABORTED, others to WAITING
        status = PilotStatus.ABORTED if idx < 25 else PilotStatus.WAITING
        mapping = PilotFieldsMapping(
            PilotStamp=stamp,
            BenchMark=idx + 0.1,
            StatusReason=f"Reason_{idx}",
            AccountingSent=(idx % 2 == 0),
            Status=status,
        ).model_dump(exclude_unset=True)
        mappings.append(mapping)

    r = normal_test_client.patch(
        "/api/pilots/metadata",
        json={"pilot_stamps_to_fields_mapping": mappings},
    )
    assert r.status_code == 204

    # -------------- Directly set SubmissionTime to March 14, 2003 for last 50 --------------
    old_date = datetime(2003, 3, 14, tzinfo=timezone.utc)
    # Access DB session from normal_test_client fixtures
    db = normal_test_client.app.dependency_overrides[PilotAgentsDB.transaction].args[0]

    async with db:
        stmt = (
            update(PilotAgents)
            .where(PilotAgents.pilot_stamp.in_(to_modify))
            .values(SubmissionTime=old_date)
        )
        await db.conn.execute(stmt)
        await db.conn.commit()

    # -------------- Verify all 100 pilots exist --------------
    search_body = {"parameters": [], "search": [], "sort": [], "distinct": True}
    r = normal_test_client.post("/api/pilots/search", json=search_body)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 100

    # -------------- 1) Delete only old aborted pilots  (25 expected) --------------
    # age_in_days large enough to include 2003-03-14
    r = normal_test_client.delete(
        "/api/pilots/",
        params={"age_in_days": 15, "delete_only_aborted": True},
    )
    assert r.status_code == 204
    # Expect 75 remaining
    r = normal_test_client.post("/api/pilots/search", json=search_body)
    assert len(r.json()) == 75

    # -------------- 2) Delete all old pilots (remaining 25 old) --------------
    r = normal_test_client.delete(
        "/api/pilots/",
        params={"age_in_days": 15},
    )
    assert r.status_code == 204

    # Expect 50 remaining
    r = normal_test_client.post("/api/pilots/search", json=search_body)
    assert len(r.json()) == 50

    # -------------- 3) Delete one recent pilot by stamp --------------
    one_stamp = pilot_stamps[10]
    r = normal_test_client.delete("/api/pilots/", params={"pilot_stamps": [one_stamp]})
    assert r.status_code == 204
    # Expect 49 remaining
    r = normal_test_client.post("/api/pilots/search", json=search_body)
    assert len(r.json()) == 49

    # -------------- 4) Delete all remaining pilots --------------
    # Collect remaining stamps
    remaining = [p["PilotStamp"] for p in r.json()]
    r = normal_test_client.delete("/api/pilots/", params={"pilot_stamps": remaining})
    assert r.status_code == 204
    # Expect none remaining
    r = normal_test_client.post("/api/pilots/search", json=search_body)
    assert r.status_code == 200
    assert len(r.json()) == 0

    # -------------- 5) Attempt deleting unknown pilot, expect 400 --------------
    r = normal_test_client.delete(
        "/api/pilots/", params={"pilot_stamps": ["unknown_stamp"]}
    )
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_associate_two_pilots_share_jobs_and_delete_first(normal_test_client):
    # 1) Create two pilots
    pilot_stamps = ["stamp_1", "stamp_2"]
    body = {"pilot_stamps": pilot_stamps}
    r = normal_test_client.post("/api/pilots/", json=body)
    assert r.status_code == 200, r.json()

    # 2) Associate first pilot with jobs 1-10
    job_ids = list(range(1, 11))
    body = {"pilot_stamp": pilot_stamps[0], "job_ids": job_ids}
    r = normal_test_client.patch("/api/pilots/jobs", json=body)
    assert r.status_code == 204

    # 3) Associate second pilot with the same jobs
    body = {"pilot_stamp": pilot_stamps[1], "job_ids": job_ids}
    r = normal_test_client.patch("/api/pilots/jobs", json=body)
    assert r.status_code == 204

    # 4) Delete first pilot
    r = normal_test_client.delete(
        "/api/pilots/", params={"pilot_stamps": [pilot_stamps[0]]}
    )
    assert r.status_code == 204

    # 5) Get jobs for pilot_1: expect empty list
    r = normal_test_client.get(
        "/api/pilots/jobs", params={"pilot_stamp": pilot_stamps[0]}
    )
    assert r.status_code == 200
    assert r.json() == []

    # 6) Get jobs for pilot_2: expect original job_ids
    r = normal_test_client.get(
        "/api/pilots/jobs", params={"pilot_stamp": pilot_stamps[1]}
    )
    assert r.status_code == 200
    assert r.json() == job_ids
