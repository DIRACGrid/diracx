"""Router-level tests for pilot register / update."""

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

    for stamp in pilot_stamps:
        r = normal_test_client.post(
            "/api/pilots/", json={"pilot_stamp": stamp, "vo": MAIN_VO}
        )
        assert r.status_code == 200, r.json()

    # Existing stamp is rejected
    r = normal_test_client.post(
        "/api/pilots/",
        json={
            "pilot_stamp": pilot_stamps[0],
            "vo": MAIN_VO,
        },
    )
    assert r.status_code == 409, r.json()

    # The new stamp alone was NOT committed by the failing call above
    r = normal_test_client.post(
        "/api/pilots/", json={"pilot_stamp": "stamps_new", "vo": MAIN_VO}
    )
    assert r.status_code == 200, r.json()


async def test_update_pilot_metadata_applies_partial_fields(normal_test_client):
    """PATCH /pilots/metadata supports heterogeneous field subsets per row."""
    stamps = ["stamp_m1", "stamp_m2"]
    for stamp in stamps:
        r = normal_test_client.post(
            "/api/pilots/", json={"pilot_stamp": stamp, "vo": MAIN_VO}
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
