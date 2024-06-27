from http import HTTPStatus

import pytest
from fastapi import HTTPException

pytestmark = pytest.mark.enabled_dependencies(
    ["ConfigSource", "AuthSettings", "OpenAccessPolicy"]
)


@pytest.fixture
def test_client(client_factory):
    with client_factory.unauthenticated() as client:
        yield client


def test_openapi(test_client):
    r = test_client.get("/api/openapi.json")
    assert r.status_code == 200
    assert r.json()


def test_oidc_configuration(test_client):
    r = test_client.get("/.well-known/openid-configuration")
    assert r.status_code == 200
    assert r.json()


def test_installation_metadata(test_client):
    r = test_client.get("/.well-known/dirac-metadata")

    assert r.status_code == 200
    assert r.json()


@pytest.mark.xfail(reason="TODO")
def test_unavailable_db(monkeypatch, test_client):
    # TODO
    # That does not work because test_client is already initialized
    monkeypatch.setenv(
        "DIRACX_DB_URL_JOBDB", "mysql+aiomysql://tata:yoyo@dbod.cern.ch:3306/name"
    )

    r = test_client.get("/api/job/123")
    assert r.status_code == 503
    assert r.json()


def test_min_client_version(test_client):
    with pytest.raises(HTTPException) as response:
        test_client.get("/", headers={"X-DIRACX-CLIENT-VERSION": "0.1.0"})
    assert response.value.status_code == HTTPStatus.UPGRADE_REQUIRED
    assert "not recent enough" in response.value.detail

    with pytest.raises(HTTPException) as response:
        test_client.get("/", headers={})
    assert response.value.status_code == HTTPStatus.BAD_REQUEST
    assert "header is missing" in response.value.detail
