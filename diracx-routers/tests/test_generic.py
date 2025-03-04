from __future__ import annotations

from http import HTTPStatus

import pytest
from packaging.version import Version, parse

from diracx.routers import DIRACX_MIN_CLIENT_VERSION

pytestmark = pytest.mark.enabled_dependencies(
    [
        "ConfigSource",
        "AuthSettings",
        "OpenAccessPolicy",
        "DevelopmentSettings",
    ]
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
    assert r.status_code == 200, r.json()
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


def test_min_client_version_lower_than_expected(test_client):
    min_client_version: Version = parse(DIRACX_MIN_CLIENT_VERSION)
    lower_version_than_min: Version = (
        f"{min_client_version.major}.{min_client_version.minor}.dev123"
    )

    r = test_client.get("/", headers={"DiracX-Client-Version": lower_version_than_min})
    assert r.status_code == HTTPStatus.UPGRADE_REQUIRED
    assert str(min_client_version) in r.json()["detail"]


def test_invalid_client_version(test_client, caplog: pytest.LogCaptureFixture):
    invalid_version = "invalid.version"
    r = test_client.get("/", headers={"DiracX-Client-Version": invalid_version})
    assert r.status_code == 400
    assert invalid_version in r.json()["detail"]
