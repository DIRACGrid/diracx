import pytest


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
