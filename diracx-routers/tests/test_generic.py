from __future__ import annotations

import os
from http import HTTPStatus

import pytest
from packaging.version import Version, parse

from diracx.db.exceptions import MissingDBURLError
from diracx.routers.factory import DIRACX_MIN_CLIENT_VERSION, create_app

pytestmark = pytest.mark.enabled_dependencies(
    [
        "ConfigSource",
        "AuthSettings",
        "OpenAccessPolicy",
        "DevelopmentSettings",
        "FactorySettings",
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


def test_refuses_to_start_without_db_urls(monkeypatch):
    """Enabled systems need a URL for every DB they use; disabling frees them."""
    for name in list(os.environ):
        if name.startswith(("DIRACX_DB_URL_", "DIRACX_OS_DB_", "DIRACX_SERVICE_")):
            monkeypatch.delenv(name)
    monkeypatch.setenv("DIRACX_DB_URL_AUTHDB", "sqlite+aiosqlite:///:memory:")

    with pytest.raises(MissingDBURLError) as exc_info:
        create_app()
    env_vars = exc_info.value.env_vars
    assert "DIRACX_DB_URL_JOBDB" in env_vars
    assert "DIRACX_OS_DB_JOBPARAMETERSDB" in env_vars
    assert "DIRACX_DB_URL_RESOURCESTATUSDB" in env_vars
    assert "DIRACX_DB_URL_AUTHDB" not in env_vars
    # No router uses them
    assert "DIRACX_DB_URL_TASKDB" not in env_vars
    assert "DIRACX_DB_URL_TRANSFORMATIONDB" not in env_vars

    monkeypatch.setenv("DIRACX_SERVICE_RSS_ENABLED", "false")
    with pytest.raises(MissingDBURLError) as exc_info:
        create_app()
    assert "DIRACX_DB_URL_RESOURCESTATUSDB" not in exc_info.value.env_vars


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
    assert r.status_code == HTTPStatus.BAD_REQUEST
    assert str(min_client_version) in r.json()["detail"]


def test_invalid_client_version(test_client, caplog: pytest.LogCaptureFixture):
    invalid_version = "invalid.version"
    r = test_client.get("/", headers={"DiracX-Client-Version": invalid_version})
    assert r.status_code == 400
    assert invalid_version in r.json()["detail"]
