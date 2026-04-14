"""Router-level tests for pilots search/summary and JobID pseudo-parameter."""

from __future__ import annotations

import pytest

from diracx.core.models.pilot import PilotMetadata, PilotStatus
from diracx.db.sql import PilotAgentsDB
from diracx.logic.pilots.management import assign_jobs_to_pilot

pytestmark = pytest.mark.enabled_dependencies(
    [
        "AuthSettings",
        "ConfigSource",
        "DevelopmentSettings",
        "PilotAgentsDB",
        "PilotManagementAccessPolicy",
        "JobDB",
    ]
)


MAIN_VO = "lhcb"
N = 20

PILOT_STATUSES = list(PilotStatus)


@pytest.fixture
def normal_test_client(client_factory):
    with client_factory.normal_user() as client:
        yield client


@pytest.fixture
async def populated_pilot_client(normal_test_client):
    """Client with N pilots registered and metadata patched."""
    pilot_stamps = [f"stamp_{i}" for i in range(1, N + 1)]

    r = normal_test_client.post(
        "/api/pilots/",
        json={"vo": MAIN_VO, "pilot_stamps": pilot_stamps},
    )
    assert r.status_code == 200, r.json()

    r = normal_test_client.patch(
        "/api/pilots/metadata",
        json={
            "pilot_metadata": [
                PilotMetadata(
                    PilotStamp=stamp,
                    BenchMark=float(i),
                    Status=PILOT_STATUSES[i % len(PILOT_STATUSES)],
                    Queue=f"queue_{i}",
                ).model_dump(exclude_unset=True)
                for i, stamp in enumerate(pilot_stamps)
            ]
        },
    )
    assert r.status_code == 204, r.json()
    return normal_test_client


def test_search_returns_pilots_from_own_vo(populated_pilot_client):
    r = populated_pilot_client.post("/api/pilots/search", json={})
    assert r.status_code == 200, r.json()
    pilots = r.json()
    assert len(pilots) == N
    assert all(p["VO"] == MAIN_VO for p in pilots)


def test_search_filter_by_status(populated_pilot_client):
    r = populated_pilot_client.post(
        "/api/pilots/search",
        json={
            "parameters": ["PilotStamp", "Status"],
            "search": [
                {
                    "parameter": "Status",
                    "operator": "eq",
                    "value": PilotStatus.WAITING.value,
                }
            ],
        },
    )
    assert r.status_code == 200, r.json()
    pilots = r.json()
    assert all(p["Status"] == PilotStatus.WAITING for p in pilots)


def test_search_pagination_content_range(populated_pilot_client):
    r = populated_pilot_client.post(
        "/api/pilots/search?per_page=5&page=1",
        json={},
    )
    assert r.status_code == 206
    assert "Content-Range" in r.headers
    assert r.headers["Content-Range"] == f"pilots 0-4/{N}"
    assert len(r.json()) == 5


def test_summary_groups_by_status(populated_pilot_client):
    r = populated_pilot_client.post(
        "/api/pilots/summary", json={"grouping": ["Status"]}
    )
    assert r.status_code == 200, r.json()
    totals = {row["Status"]: row["count"] for row in r.json()}
    assert sum(totals.values()) == N


# ---------------------------------------------------------------------------
# Cross-table search: JobID pseudo-parameter on POST /api/pilots/search
# ---------------------------------------------------------------------------


async def _assign(client, stamp: str, job_ids: list[int]) -> None:
    """Insert JobToPilotMapping rows directly via the DB dependency override."""
    db = client.app.dependency_overrides[PilotAgentsDB.transaction].args[0]
    async with db:
        await assign_jobs_to_pilot(pilot_db=db, pilot_stamp=stamp, job_ids=job_ids)


async def test_pilots_search_by_job_id_eq(populated_pilot_client):
    """A `JobID` eq filter returns only the pilots that ran that job."""
    await _assign(populated_pilot_client, "stamp_1", [100])
    await _assign(populated_pilot_client, "stamp_2", [100])
    await _assign(populated_pilot_client, "stamp_3", [200])

    r = populated_pilot_client.post(
        "/api/pilots/search",
        json={
            "parameters": ["PilotStamp"],
            "search": [{"parameter": "JobID", "operator": "eq", "value": 100}],
        },
    )
    assert r.status_code == 200, r.json()
    stamps = sorted(p["PilotStamp"] for p in r.json())
    assert stamps == ["stamp_1", "stamp_2"]


async def test_pilots_search_by_job_id_in(populated_pilot_client):
    """An `in` filter over several job IDs returns the union of their pilots."""
    await _assign(populated_pilot_client, "stamp_4", [300])
    await _assign(populated_pilot_client, "stamp_5", [301])

    r = populated_pilot_client.post(
        "/api/pilots/search",
        json={
            "parameters": ["PilotStamp"],
            "search": [
                {
                    "parameter": "JobID",
                    "operator": "in",
                    "values": [300, 301],
                }
            ],
        },
    )
    assert r.status_code == 200, r.json()
    stamps = sorted(p["PilotStamp"] for p in r.json())
    assert stamps == ["stamp_4", "stamp_5"]


def test_pilots_search_by_unknown_job_id_returns_empty(populated_pilot_client):
    r = populated_pilot_client.post(
        "/api/pilots/search",
        json={"search": [{"parameter": "JobID", "operator": "eq", "value": 999999}]},
    )
    assert r.status_code == 200
    assert r.json() == []


def test_pilots_search_job_id_unsupported_operator_raises(populated_pilot_client):
    r = populated_pilot_client.post(
        "/api/pilots/search",
        json={"search": [{"parameter": "JobID", "operator": "neq", "value": 1}]},
    )
    assert r.status_code in (400, 422), r.json()


def test_pilots_search_combining_job_id_and_pilot_id_raises(populated_pilot_client):
    r = populated_pilot_client.post(
        "/api/pilots/search",
        json={
            "search": [
                {"parameter": "JobID", "operator": "eq", "value": 1},
                {"parameter": "PilotID", "operator": "eq", "value": 1},
            ]
        },
    )
    assert r.status_code in (400, 422), r.json()
