from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from diracx.routers.utils.users import AuthSettings

pytestmark = pytest.mark.enabled_dependencies(
    [
        "AuthSettings",
        "PilotAgentsDB",
        "PilotLogsDB",
        "PilotLogsAccessPolicy",
        "DevelopmentSettings",
    ]
)


@pytest.fixture
def normal_user_client(client_factory):
    with client_factory.normal_user() as client:
        yield client


async def test_send_and_retrieve_logs(
    normal_user_client: TestClient, test_auth_settings: AuthSettings
):

    from diracx.db.sql import PilotAgentsDB

    # Add a pilot reference
    upper_limit = 6
    refs = [f"ref_{i}" for i in range(1, upper_limit)]
    stamps = [f"stamp_{i}" for i in range(1, upper_limit)]
    stamp_dict = dict(zip(refs, stamps))

    db = normal_user_client.app.dependency_overrides[PilotAgentsDB.transaction].args[0]

    async with db:
        await db.add_pilot_references(
            refs, "test_vo", grid_type="DIRAC", pilot_stamps=stamp_dict
        )

    msg = (
        "2022-02-26 13:48:35.123456 UTC DEBUG [PilotParams] JSON file loaded: pilot.json\n"
        "2022-02-26 13:48:36.123456 UTC DEBUG [PilotParams] JSON file analysed: pilot.json"
    )
    # message dict
    lines = []
    for i, line in enumerate(msg.split("\n")):
        lines.append({"line_no": i, "line": line})
    msg_dict = {"lines": lines, "pilot_stamp": "stamp_1", "vo": "diracAdmin"}

    # send message
    r = normal_user_client.post("/api/pilots/", json=msg_dict)

    assert r.status_code == 200, r.text
    # it just returns the pilot id corresponding for pilot stamp.
    assert r.json() == 1
    # get the message back:
    r = normal_user_client.get("/api/pilots/logs?pilot_id=1")
    assert r.status_code == 200, r.text
    assert [next(iter(d.values())) for d in r.json()] == msg.split("\n")
