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


async def test_register_pilot_unknown_vo_returns_400(normal_test_client):
    """Registering into a VO absent from the registry is rejected."""
    r = normal_test_client.post(
        "/api/pilots/",
        json={"pilot_stamp": "stamp_vo", "vo": "not-a-registered-vo"},
    )
    assert r.status_code == 400, r.json()


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
            "stamp_m1": PilotMetadata(BenchMark=1.0).model_dump(
                by_alias=True, exclude_unset=True
            ),
            "stamp_m2": PilotMetadata(Status=PilotStatus.WAITING).model_dump(
                by_alias=True, exclude_unset=True
            ),
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


async def test_update_pilot_metadata_unknown_stamp_returns_404(normal_test_client):
    r = normal_test_client.patch(
        "/api/pilots/metadata",
        json={"nonexistent": {"Status": PilotStatus.DONE.value}},
    )
    assert r.status_code == 404, r.json()


async def test_update_pilot_metadata_refreshes_last_update_time(normal_test_client):
    """Any metadata update also bumps LastUpdateTime."""
    r = normal_test_client.post(
        "/api/pilots/", json={"pilot_stamp": "stamp_t", "vo": MAIN_VO}
    )
    assert r.status_code == 200

    r = normal_test_client.post(
        "/api/pilots/search", json={"parameters": ["LastUpdateTime"]}
    )
    before = r.json()[0]["LastUpdateTime"]

    r = normal_test_client.patch(
        "/api/pilots/metadata",
        json={"stamp_t": {"BenchMark": 1.0}},
    )
    assert r.status_code == 204

    r = normal_test_client.post(
        "/api/pilots/search", json={"parameters": ["LastUpdateTime"]}
    )
    assert r.json()[0]["LastUpdateTime"] > before


async def test_register_pilot_reference(normal_test_client):
    """An explicit reference is stored; without one the stamp is used."""
    r = normal_test_client.post(
        "/api/pilots/",
        json={"pilot_stamp": "stamp_r1", "vo": MAIN_VO, "pilot_reference": "ref-1"},
    )
    assert r.status_code == 200, r.json()
    r = normal_test_client.post(
        "/api/pilots/", json={"pilot_stamp": "stamp_r2", "vo": MAIN_VO}
    )
    assert r.status_code == 200, r.json()

    r = normal_test_client.post(
        "/api/pilots/search",
        json={"parameters": ["PilotStamp", "PilotJobReference"]},
    )
    assert r.status_code == 200
    by_stamp = {p["PilotStamp"]: p["PilotJobReference"] for p in r.json()}
    assert by_stamp["stamp_r1"] == "ref-1"
    assert by_stamp["stamp_r2"] == "stamp_r2"
