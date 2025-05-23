from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from diracx.db.sql import PilotAgentsDB

pytestmark = pytest.mark.enabled_dependencies(
    [
        "AuthSettings",
        "PilotAgentsDB",
        "PilotLogsDB",
        "DevelopmentSettings",
        "PilotLogsAccessPolicy",
    ]
)


@pytest.fixture
def normal_test_client(client_factory):
    with client_factory.normal_user() as client:
        yield client


async def test_send_and_retrieve_logs(normal_test_client: TestClient):

    # Add a pilot reference
    upper_limit = 6
    refs = [f"ref_{i}" for i in range(1, upper_limit)]
    stamps = [f"stamp_{i}" for i in range(1, upper_limit)]
    pilot_references = dict(zip(stamps, refs))

    db = normal_test_client.app.dependency_overrides[PilotAgentsDB.transaction].args[0]

    async with db:
        await db.add_pilots_bulk(
            stamps, "test_vo", grid_type="DIRAC", pilot_references=pilot_references
        )

    msg = "JSON file loaded: pilot.json\n" "JSON file analysed: pilot.json"
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
    r = normal_test_client.post("/api/pilots/message", json=msg_dict)

    assert r.status_code == 204, r.json()
    # get the message back:
    data = {
        "search": [{"parameter": "PilotStamp", "operator": "eq", "value": "stamp_1"}]
    }
    r = normal_test_client.post("/api/pilots/logs", json=data)
    assert r.status_code == 200, r.text
    assert [hit["Message"] for hit in r.json()] == msg.split("\n")
