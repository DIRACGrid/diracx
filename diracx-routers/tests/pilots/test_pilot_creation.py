"""Router-level tests for pilot register / update / delete."""

from __future__ import annotations

import pytest

from diracx.core.models.pilot import PilotMetadata, PilotStatus

pytestmark = pytest.mark.enabled_dependencies(
    [
        "DevelopmentSettings",
        "AuthDB",
        "AuthSettings",
        "ConfigSource",
        "PilotAgentsDB",
        "PilotManagementAccessPolicy",
        "JobDB",
    ]
)

MAIN_VO = "lhcb"


@pytest.fixture
def normal_test_client(client_factory):
    with client_factory.normal_user() as client:
        yield client


async def test_register_then_duplicate_then_success(normal_test_client):
    """Registering an existing stamp is rejected with 409; a fresh one still succeeds."""
    pilot_stamps = [f"stamps_{i}" for i in range(5)]

    r = normal_test_client.post(
        "/api/pilots/", json={"pilot_stamps": pilot_stamps, "vo": MAIN_VO}
    )
    assert r.status_code == 200, r.json()

    # Mix of existing and new stamps: whole batch is rejected
    r = normal_test_client.post(
        "/api/pilots/",
        json={
            "pilot_stamps": [pilot_stamps[0], "stamps_new"],
            "vo": MAIN_VO,
        },
    )
    assert r.status_code == 409, r.json()

    # The new stamp alone was NOT committed by the failing call above
    r = normal_test_client.post(
        "/api/pilots/", json={"pilot_stamps": ["stamps_new"], "vo": MAIN_VO}
    )
    assert r.status_code == 200, r.json()


async def test_register_delete_by_stamp_roundtrip(normal_test_client):
    r = normal_test_client.post(
        "/api/pilots/", json={"pilot_stamps": ["stamp_a"], "vo": MAIN_VO}
    )
    assert r.status_code == 200

    r = normal_test_client.delete("/api/pilots/", params={"pilot_stamps": ["stamp_a"]})
    assert r.status_code == 204

    # Now the stamp is free again
    r = normal_test_client.post(
        "/api/pilots/", json={"pilot_stamps": ["stamp_a"], "vo": MAIN_VO}
    )
    assert r.status_code == 200


async def test_update_pilot_metadata_applies_partial_fields(normal_test_client):
    """PATCH /pilots/metadata supports heterogeneous field subsets per row."""
    stamps = ["stamp_m1", "stamp_m2"]
    r = normal_test_client.post(
        "/api/pilots/", json={"pilot_stamps": stamps, "vo": MAIN_VO}
    )
    assert r.status_code == 200

    # stamp_m1 updates only BenchMark; stamp_m2 only Status
    r = normal_test_client.patch(
        "/api/pilots/metadata",
        json={
            "pilot_metadata": [
                PilotMetadata(PilotStamp="stamp_m1", BenchMark=1.0).model_dump(
                    exclude_unset=True
                ),
                PilotMetadata(
                    PilotStamp="stamp_m2", Status=PilotStatus.WAITING
                ).model_dump(exclude_unset=True),
            ]
        },
    )
    assert r.status_code == 204, r.json()

    r = normal_test_client.post("/api/pilots/search", json={})
    assert r.status_code == 200
    by_stamp = {p["PilotStamp"]: p for p in r.json()}
    assert by_stamp["stamp_m1"]["BenchMark"] == 1.0
    assert by_stamp["stamp_m1"]["Status"] == PilotStatus.SUBMITTED  # untouched
    assert by_stamp["stamp_m2"]["Status"] == PilotStatus.WAITING
    assert by_stamp["stamp_m2"]["BenchMark"] == 0.0  # untouched


async def test_delete_unknown_stamp_is_a_noop(normal_test_client):
    """Deleting an unknown stamp is a safe no-op under the test harness.

    The test harness replaces `PilotManagementAccessPolicy` with
    `AlwaysAllowAccessPolicy`, so the real policy's unknown-stamp 404
    branch is exercised by the dedicated policy unit test
    (`test_access_policy.py`). Here we only verify the router path does
    not explode and is safely idempotent.
    """
    r = normal_test_client.delete(
        "/api/pilots/", params={"pilot_stamps": ["does_not_exist"]}
    )
    assert r.status_code == 204


async def test_delete_requires_at_least_one_stamp(normal_test_client):
    """DELETE with no stamps must return 422 (FastAPI validation)."""
    r = normal_test_client.delete("/api/pilots/")
    assert r.status_code == 422, r.json()


async def test_unknown_query_params_do_not_trigger_deletion(normal_test_client):
    """Age-based cleanup is handled by the task worker, not the HTTP API.

    The router must NOT accept age_in_days; any such param is either
    ignored by FastAPI or returns 422 on unexpected query usage. The key
    observation is that passing `age_in_days` alone (without
    `pilot_stamps`) must not silently wipe pilots.
    """
    # Create a pilot to ensure there's something that could be deleted
    r = normal_test_client.post(
        "/api/pilots/", json={"pilot_stamps": ["stamp_safe"], "vo": MAIN_VO}
    )
    assert r.status_code == 200

    # age_in_days alone is rejected because pilot_stamps is required
    r = normal_test_client.delete("/api/pilots/", params={"age_in_days": 1})
    assert r.status_code == 422

    # Our pilot is still there
    r = normal_test_client.post("/api/pilots/search", json={})
    assert r.status_code == 200
    assert any(p["PilotStamp"] == "stamp_safe" for p in r.json())
