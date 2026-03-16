"""Test my_pilots router."""

import pytest

pytestmark = pytest.mark.enabled_dependencies(
    [
        "AuthSettings",
        "MyPilotDB",
        "MyPilotsAccessPolicy",
        "DevelopmentSettings",
    ]
)


@pytest.fixture
def normal_user_client(client_factory):
    with client_factory.normal_user() as client:
        yield client


def test_pilot_summary_empty(normal_user_client):
    r = normal_user_client.get("/api/my_pilots/summary")
    assert r.status_code == 200
    assert r.json() == {}
