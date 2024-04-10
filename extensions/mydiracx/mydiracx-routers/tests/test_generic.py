import pytest

pytestmark = pytest.mark.enabled_dependencies(["ConfigSource", "AuthSettings"])


@pytest.fixture
def normal_user_client(client_factory):
    with client_factory.normal_user() as client:
        yield client


def test_write(normal_user_client):
    r = normal_user_client.post("/api/mymanager/insert_owner/username")
    assert r.status_code == 200
    assert r.json()


def test_read(normal_user_client):
    r = normal_user_client.get("/api/mymanager/get_owners")
    assert r.status_code == 200
    assert r.json()
    assert r.json()[0] == "username"
