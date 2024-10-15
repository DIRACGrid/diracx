"""
Test lollygag router as a normal router
"""

import pytest

pytestmark = pytest.mark.enabled_dependencies(
    [
        "AuthSettings",
        "LollygagDB",
        "LollygagAccessPolicy",
        "DevelopmentSettings",
    ]
)


@pytest.fixture
def normal_user_client(client_factory):
    with client_factory.normal_user() as client:
        yield client


def test_lollygag(normal_user_client):
    r = normal_user_client.get("/api/lollygag/get_owners")
    assert r.status_code == 200
    assert len(r.json()) == 0

    r = normal_user_client.post("/api/lollygag/insert_owner/username")
    assert r.status_code == 200
    assert r.json()

    r = normal_user_client.get("/api/lollygag/get_owners")
    assert r.status_code == 200
    assert r.json() == ["username"]
