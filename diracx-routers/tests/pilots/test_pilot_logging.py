from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from diracx.core.exceptions import InvalidQueryError

pytestmark = pytest.mark.enabled_dependencies(
    [
        "AuthDB",
        "AuthSettings",
        "PilotAgentsDB",
        "PilotLogsDB",
        "DevelopmentSettings",
        "PilotManagementAccessPolicy",
        "LegacyPilotAccessPolicy",
    ]
)

N = 100


@pytest.fixture
def test_client(client_factory):
    with client_factory.unauthenticated() as client:
        yield client


@pytest.fixture
def normal_test_client(client_factory):
    with client_factory.normal_user() as client:
        yield client


@pytest.fixture
def create_pilots(normal_test_client: TestClient):
    # Add a pilot stamps
    pilot_stamps = [f"stamp_{i}" for i in range(N)]

    body = {"vo": "lhcb", "pilot_stamps": pilot_stamps}

    r = normal_test_client.post(
        "/api/pilots/",
        json=body,
    )
    assert r.status_code == 200, r.json()

    return pilot_stamps


@pytest.fixture
async def create_logs(create_pilots, normal_test_client):
    for i, stamp in enumerate(create_pilots):
        lines = [
            {
                "message": stamp,
                "timestamp": "2022-02-26 13:48:35.123456",
                "scope": "PilotParams" if i % 2 == 1 else "Commands",
                "severity": "DEBUG" if i % 2 == 0 else "INFO",
            }
        ]
        msg_dict = {"lines": lines, "pilot_stamp": stamp}
        r = normal_test_client.post("/api/pilots/legacy/message", json=msg_dict)

        assert r.status_code == 204, r.json()
    # Return only stamps
    return create_pilots


@pytest.fixture
async def search(normal_test_client):
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

        r = normal_test_client.post("/api/pilots/search/logs", json=body, params=params)

        if r.status_code == 400:
            # If we have a status_code 400, that means that the query failed
            raise InvalidQueryError()

        return r.json(), r.headers

    return _search


async def test_single_send_and_retrieve_logs(normal_test_client: TestClient):
    # Add a pilot stamps
    pilot_stamp = ["stamp_1"]

    #  -------------- Bulk insert --------------
    body = {"vo": "lhcb", "pilot_stamps": pilot_stamp}

    r = normal_test_client.post(
        "/api/pilots/",
        json=body,
    )

    assert r.status_code == 200, r.json()

    msg = "JSON file loaded: pilot.json\nJSON file analysed: pilot.json"
    # message dict
    lines = []
    for line in msg.split("\n"):
        lines.append(
            {
                "message": line,
                "timestamp": "2022-02-26 13:48:35.123456",
                "scope": "PilotParams",
                "severity": "DEBUG",
            }
        )
    msg_dict = {"lines": lines, "pilot_stamp": "stamp_1"}

    # send message
    r = normal_test_client.post("/api/pilots/legacy/message", json=msg_dict)

    assert r.status_code == 204, r.json()
    # get the message back:
    data = {
        "search": [{"parameter": "PilotStamp", "operator": "eq", "value": "stamp_1"}]
    }
    r = normal_test_client.post("/api/pilots/search/logs", json=data)
    assert r.status_code == 200, r.text
    assert [hit["Message"] for hit in r.json()] == msg.split("\n")


async def test_query_invalid_stamp(create_logs, normal_test_client):
    data = {
        "search": [
            {"parameter": "PilotStamp", "operator": "eq", "value": "not_a_stamp"}
        ]
    }
    r = normal_test_client.post("/api/pilots/search/logs", json=data)
    assert r.status_code == 200, r.text
    assert len(r.json()) == 0


async def test_query_each_length(create_logs, normal_test_client):
    for stamp in create_logs:
        data = {
            "search": [{"parameter": "PilotStamp", "operator": "eq", "value": stamp}]
        }
        r = normal_test_client.post("/api/pilots/search/logs", json=data)
        assert r.status_code == 200, r.text
        assert len(r.json()) == 1


async def test_query_each_field(create_logs, normal_test_client):
    for i, stamp in enumerate(create_logs):
        data = {
            "search": [{"parameter": "PilotStamp", "operator": "eq", "value": stamp}],
            "sort": [{"parameter": "PilotStamp", "direction": "asc"}],
        }
        r = normal_test_client.post("/api/pilots/search/logs", json=data)
        assert r.status_code == 200, r.text
        assert len(r.json()) == 1

        # Reminder:

        # "message": str(i),
        # "timestamp": "2022-02-26 13:48:35.123456",
        # "scope": "PilotParams" if i % 2 == 1 else "Commands",
        # "severity": "DEBUG" if i % 2 == 0 else "INFO",
        log = r.json()[0]

        assert log["Message"] == f"stamp_{i}"
        assert log["Scope"] == ("PilotParams" if i % 2 == 1 else "Commands")
        assert log["Severity"] == ("DEBUG" if i % 2 == 0 else "INFO")


async def test_search_pagination(create_logs, search):
    """Test that we can search for logs."""
    # Search for the first 10 logs
    result, headers = await search([], [], [], per_page=10, page=1)
    assert "Content-Range" in headers
    # Because Content-Range = f"logs {first_idx}-{last_idx}/{total}"
    total = int(headers["Content-Range"].split("/")[1])
    assert total == N
    assert result
    assert len(result) == 10
    assert result[0]["PilotID"] == 1

    # Search for the second 10 logs
    result, headers = await search([], [], [], per_page=10, page=2)
    assert "Content-Range" in headers
    # Because Content-Range = f"logs {first_idx}-{last_idx}/{total}"
    total = int(headers["Content-Range"].split("/")[1])
    assert total == N
    assert result
    assert len(result) == 10
    assert result[0]["PilotID"] == 11

    # Search for the last 10 logs
    result, headers = await search([], [], [], per_page=10, page=10)
    assert "Content-Range" in headers
    # Because Content-Range = f"logs {first_idx}-{last_idx}/{total}"
    total = int(headers["Content-Range"].split("/")[1])
    assert result
    assert len(result) == 10
    assert result[0]["PilotID"] == 91

    # Search for the second 50 logs
    result, headers = await search([], [], [], per_page=50, page=2)
    assert "Content-Range" in headers
    # Because Content-Range = f"logs {first_idx}-{last_idx}/{total}"
    total = int(headers["Content-Range"].split("/")[1])
    assert result
    assert len(result) == 50
    assert result[0]["PilotID"] == 51

    # Invalid page number
    result, headers = await search([], [], [], per_page=10, page=11)
    assert "Content-Range" in headers
    # Because Content-Range = f"logs {first_idx}-{last_idx}/{total}"
    total = int(headers["Content-Range"].split("/")[1])
    assert not result

    # Invalid page number
    with pytest.raises(InvalidQueryError):
        result = await search([], [], [], per_page=10, page=0)

    # Invalid per_page number
    with pytest.raises(InvalidQueryError):
        result = await search([], [], [], per_page=0, page=1)
