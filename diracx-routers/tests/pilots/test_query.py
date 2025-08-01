"""Inspired by pilots and jobs db search tests."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from diracx.core.exceptions import InvalidQueryError
from diracx.core.models import (
    PilotFieldsMapping,
    PilotStatus,
    ScalarSearchOperator,
    ScalarSearchSpec,
    SortDirection,
    SortSpec,
    VectorSearchOperator,
    VectorSearchSpec,
)

pytestmark = pytest.mark.enabled_dependencies(
    [
        "AuthSettings",
        "ConfigSource",
        "DevelopmentSettings",
        "PilotAgentsDB",
        "PilotManagementAccessPolicy",
    ]
)


@pytest.fixture
def normal_test_client(client_factory):
    with client_factory.normal_user() as client:
        yield client


MAIN_VO = "lhcb"
N = 100

PILOT_REASONS = [
    "I was sick",
    "I can't, I have a pony.",
    "I was shopping",
    "I was sleeping",
]

PILOT_STATUSES = list(PilotStatus)


@pytest.fixture
async def populated_pilot_client(normal_test_client):
    pilot_stamps = [f"stamp_{i}" for i in range(1, N + 1)]

    #  -------------- Bulk insert --------------
    body = {"vo": MAIN_VO, "pilot_stamps": pilot_stamps}

    r = normal_test_client.post(
        "/api/pilots/",
        json=body,
    )

    assert r.status_code == 200, r.json()

    body = {
        "pilot_stamps_to_fields_mapping": [
            PilotFieldsMapping(
                PilotStamp=pilot_stamp,
                BenchMark=i**2,
                StatusReason=PILOT_REASONS[i % len(PILOT_REASONS)],
                AccountingSent=True,
                Status=PILOT_STATUSES[i % len(PILOT_STATUSES)],
                CurrentJobID=i,
                Queue=f"queue_{i}",
            ).model_dump(exclude_unset=True)
            for i, pilot_stamp in enumerate(pilot_stamps)
        ]
    }

    r = normal_test_client.patch("/api/pilots/metadata", json=body)

    assert r.status_code == 204

    yield normal_test_client


async def test_pilot_summary(populated_pilot_client: TestClient):
    # Group by StatusReason
    r = populated_pilot_client.post(
        "/api/pilots/summary",
        json={
            "grouping": ["StatusReason"],
        },
    )

    assert r.status_code == 200

    assert sum([el["count"] for el in r.json()]) == N
    assert len(r.json()) == len(PILOT_REASONS)

    # Group by CurrentJobID
    r = populated_pilot_client.post(
        "/api/pilots/summary",
        json={
            "grouping": ["CurrentJobID"],
        },
    )

    assert r.status_code == 200

    assert all(el["count"] == 1 for el in r.json())
    assert len(r.json()) == N

    # Group by CurrentJobID where BenchMark < 10^2
    r = populated_pilot_client.post(
        "/api/pilots/summary",
        json={
            "grouping": ["CurrentJobID"],
            "search": [{"parameter": "BenchMark", "operator": "lt", "value": 10**2}],
        },
    )

    assert r.status_code == 200, r.json()

    assert all(el["count"] == 1 for el in r.json())
    assert len(r.json()) == 10


@pytest.fixture
async def search(populated_pilot_client):
    async def _search(
        parameters, conditions, sorts, distinct=False, page=1, per_page=100
    ):
        body = {
            "parameters": parameters,
            "search": conditions,
            "sort": sorts,
            "distinct": distinct,
        }

        params = {"per_page": per_page, "page": page}

        r = populated_pilot_client.post("/api/pilots/search", json=body, params=params)

        if r.status_code == 400:
            # If we have a status_code 400, that means that the query failed
            raise InvalidQueryError()

        return r.json(), r.headers

    return _search


async def test_search_parameters(search):
    """Test that we can search specific parameters for pilots."""
    # Search a specific parameter: PilotID
    result, headers = await search(["PilotID"], [], [])
    assert len(result) == N
    assert result
    for r in result:
        assert r.keys() == {"PilotID"}
    assert "Content-Range" not in headers

    # Search a specific parameter: Status
    result, headers = await search(["Status"], [], [])
    assert len(result) == N
    assert result
    for r in result:
        assert r.keys() == {"Status"}
    assert "Content-Range" not in headers

    # Search for multiple parameters: PilotID, Status
    result, headers = await search(["PilotID", "Status"], [], [])
    assert len(result) == N
    assert result
    for r in result:
        assert r.keys() == {"PilotID", "Status"}
    assert "Content-Range" not in headers

    # Search for a specific parameter but use distinct: Status
    result, headers = await search(["Status"], [], [], distinct=True)
    assert len(result) == len(PILOT_STATUSES)
    assert result
    assert "Content-Range" not in headers

    # Search for a non-existent parameter: Dummy
    with pytest.raises(InvalidQueryError):
        result, headers = await search(["Dummy"], [], [])


async def test_search_conditions(search):
    """Test that we can search for specific pilots."""
    # Search a specific scalar condition: PilotID eq 3
    condition = ScalarSearchSpec(
        parameter="PilotID", operator=ScalarSearchOperator.EQUAL, value=3
    )
    result, headers = await search([], [condition], [])
    assert len(result) == 1
    assert result
    assert len(result) == 1
    assert result[0]["PilotID"] == 3
    assert "Content-Range" not in headers

    # Search a specific scalar condition: PilotID lt 3
    condition = ScalarSearchSpec(
        parameter="PilotID", operator=ScalarSearchOperator.LESS_THAN, value=3
    )
    result, headers = await search([], [condition], [])
    assert len(result) == 2
    assert result
    assert len(result) == 2
    assert result[0]["PilotID"] == 1
    assert result[1]["PilotID"] == 2
    assert "Content-Range" not in headers

    # Search a specific scalar condition: PilotID neq 3
    condition = ScalarSearchSpec(
        parameter="PilotID", operator=ScalarSearchOperator.NOT_EQUAL, value=3
    )
    result, headers = await search([], [condition], [])
    assert len(result) == 99
    assert result
    assert len(result) == 99
    assert all(r["PilotID"] != 3 for r in result)
    assert "Content-Range" not in headers

    # Search a specific scalar condition: PilotID eq 5873 (does not exist)
    condition = ScalarSearchSpec(
        parameter="PilotID", operator=ScalarSearchOperator.EQUAL, value=5873
    )
    result, headers = await search([], [condition], [])
    assert not result
    assert "Content-Range" not in headers

    # Search a specific vector condition: PilotID in 1,2,3
    condition = VectorSearchSpec(
        parameter="PilotID", operator=VectorSearchOperator.IN, values=[1, 2, 3]
    )
    result, headers = await search([], [condition], [])
    assert len(result) == 3
    assert result
    assert len(result) == 3
    assert all(r["PilotID"] in [1, 2, 3] for r in result)
    assert "Content-Range" not in headers

    # Search a specific vector condition: PilotID in 1,2,5873 (one of them does not exist)
    condition = VectorSearchSpec(
        parameter="PilotID", operator=VectorSearchOperator.IN, values=[1, 2, 5873]
    )
    result, headers = await search([], [condition], [])
    assert len(result) == 2
    assert result
    assert len(result) == 2
    assert all(r["PilotID"] in [1, 2] for r in result)
    assert "Content-Range" not in headers

    # Search a specific vector condition: PilotID not in 1,2,3
    condition = VectorSearchSpec(
        parameter="PilotID", operator=VectorSearchOperator.NOT_IN, values=[1, 2, 3]
    )
    result, headers = await search([], [condition], [])
    assert len(result) == 97
    assert result
    assert len(result) == 97
    assert all(r["PilotID"] not in [1, 2, 3] for r in result)
    assert "Content-Range" not in headers

    # Search a specific vector condition: PilotID not in 1,2,5873 (one of them does not exist)
    condition = VectorSearchSpec(
        parameter="PilotID",
        operator=VectorSearchOperator.NOT_IN,
        values=[1, 2, 5873],
    )
    result, headers = await search([], [condition], [])
    assert len(result) == 98
    assert result
    assert len(result) == 98
    assert all(r["PilotID"] not in [1, 2] for r in result)
    assert "Content-Range" not in headers

    # Search for multiple conditions based on different parameters: PilotID eq 70, PilotID in 4,5,6
    condition1 = ScalarSearchSpec(
        parameter="PilotStamp", operator=ScalarSearchOperator.EQUAL, value="stamp_5"
    )
    condition2 = VectorSearchSpec(
        parameter="PilotID", operator=VectorSearchOperator.IN, values=[4, 5, 6]
    )
    result, headers = await search([], [condition1, condition2], [])

    assert result
    assert len(result) == 1
    assert result[0]["PilotID"] == 5
    assert result[0]["PilotStamp"] == "stamp_5"
    assert "Content-Range" not in headers

    # Search for multiple conditions based on the same parameter: PilotID eq 70, PilotID in 4,5,6
    condition1 = ScalarSearchSpec(
        parameter="PilotID", operator=ScalarSearchOperator.EQUAL, value=70
    )
    condition2 = VectorSearchSpec(
        parameter="PilotID", operator=VectorSearchOperator.IN, values=[4, 5, 6]
    )
    result, headers = await search([], [condition1, condition2], [])
    assert len(result) == 0
    assert not result
    assert "Content-Range" not in headers


async def test_search_sorts(search):
    """Test that we can search for pilots and sort the results."""
    # Search and sort by PilotID in ascending order
    sort = SortSpec(parameter="PilotID", direction=SortDirection.ASC)
    result, headers = await search([], [], [sort])
    assert len(result) == N
    assert result
    for i, r in enumerate(result):
        assert r["PilotID"] == i + 1
    assert "Content-Range" not in headers

    # Search and sort by PilotID in descending order
    sort = SortSpec(parameter="PilotID", direction=SortDirection.DESC)
    result, headers = await search([], [], [sort])
    assert len(result) == N
    assert result
    for i, r in enumerate(result):
        assert r["PilotID"] == N - i
    assert "Content-Range" not in headers

    # Search and sort by PilotStamp in ascending order
    sort = SortSpec(parameter="PilotStamp", direction=SortDirection.ASC)
    result, headers = await search([], [], [sort])
    assert len(result) == N
    assert result
    # Assert that stamp_10 is before stamp_2 because of the lexicographical order
    assert result[2]["PilotStamp"] == "stamp_100"
    assert result[12]["PilotStamp"] == "stamp_2"
    assert "Content-Range" not in headers

    # Search and sort by PilotStamp in descending order
    sort = SortSpec(parameter="PilotStamp", direction=SortDirection.DESC)
    result, headers = await search([], [], [sort])
    assert len(result) == N
    assert result
    # Assert that stamp_10 is before stamp_2 because of the lexicographical order
    assert result[97]["PilotStamp"] == "stamp_100"
    assert result[87]["PilotStamp"] == "stamp_2"
    assert "Content-Range" not in headers

    # Search and sort by PilotStamp in ascending order and PilotID in descending order
    sort1 = SortSpec(parameter="PilotStamp", direction=SortDirection.ASC)
    sort2 = SortSpec(parameter="PilotID", direction=SortDirection.DESC)
    result, headers = await search([], [], [sort1, sort2])
    assert len(result) == N
    assert result
    assert result[0]["PilotStamp"] == "stamp_1"
    assert result[0]["PilotID"] == 1
    assert result[99]["PilotStamp"] == "stamp_99"
    assert result[99]["PilotID"] == 99
    assert "Content-Range" not in headers


async def test_search_pagination(search):
    """Test that we can search for pilots."""
    # Search for the first 10 pilots
    result, headers = await search([], [], [], per_page=10, page=1)
    assert "Content-Range" in headers
    # Because Content-Range = f"pilots {first_idx}-{last_idx}/{total}"
    total = int(headers["Content-Range"].split("/")[1])
    assert total == N
    assert result
    assert len(result) == 10
    assert result[0]["PilotID"] == 1

    # Search for the second 10 pilots
    result, headers = await search([], [], [], per_page=10, page=2)
    assert "Content-Range" in headers
    # Because Content-Range = f"pilots {first_idx}-{last_idx}/{total}"
    total = int(headers["Content-Range"].split("/")[1])
    assert total == N
    assert result
    assert len(result) == 10
    assert result[0]["PilotID"] == 11

    # Search for the last 10 pilots
    result, headers = await search([], [], [], per_page=10, page=10)
    assert "Content-Range" in headers
    # Because Content-Range = f"pilots {first_idx}-{last_idx}/{total}"
    total = int(headers["Content-Range"].split("/")[1])
    assert result
    assert len(result) == 10
    assert result[0]["PilotID"] == 91

    # Search for the second 50 pilots
    result, headers = await search([], [], [], per_page=50, page=2)
    assert "Content-Range" in headers
    # Because Content-Range = f"pilots {first_idx}-{last_idx}/{total}"
    total = int(headers["Content-Range"].split("/")[1])
    assert result
    assert len(result) == 50
    assert result[0]["PilotID"] == 51

    # Invalid page number
    result, headers = await search([], [], [], per_page=10, page=11)
    assert "Content-Range" in headers
    # Because Content-Range = f"pilots {first_idx}-{last_idx}/{total}"
    total = int(headers["Content-Range"].split("/")[1])
    assert not result

    # Invalid page number
    with pytest.raises(InvalidQueryError):
        result = await search([], [], [], per_page=10, page=0)

    # Invalid per_page number
    with pytest.raises(InvalidQueryError):
        result = await search([], [], [], per_page=0, page=1)
