from __future__ import annotations

from http import HTTPStatus

import pytest
from packaging.version import Version, parse

from diracx.routers.factory import DIRACX_MIN_CLIENT_VERSION

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


@pytest.mark.parametrize(
    "forbidden",
    ["Args:", "Raises:", "Returns:", "Yields:", "auth_db", "HTTPException"],
)
def test_openapi_descriptions_exclude_docstring_sections(test_client, forbidden):
    """Server-side docstring sections must be hidden from the API docs by a "\f"."""
    r = test_client.get("/api/openapi.json")
    assert r.status_code == 200
    leaked = [
        f"{method.upper()} {path}"
        for path, operations in r.json()["paths"].items()
        for method, operation in operations.items()
        if forbidden in operation.get("description", "")
    ]
    assert not leaked, (
        f"{forbidden!r} found in the OpenAPI description of {leaked}. FastAPI "
        "publishes the route's docstring as its description, which ends up in "
        "Swagger and in the generated clients. Put a line containing only '\\f' "
        "between the user-facing summary and the server-side sections "
        "(Args, Raises, ...) so that FastAPI cuts the description there."
    )


@pytest.mark.parametrize("forbidden", ["Attributes:", "Args:", "Raises:"])
def test_openapi_schemas_exclude_docstring_sections(test_client, forbidden):
    """Model docstrings must not carry Google-style sections into the API docs."""
    r = test_client.get("/api/openapi.json")
    assert r.status_code == 200
    leaked = [
        name
        for name, schema in r.json()["components"]["schemas"].items()
        if forbidden in schema.get("description", "")
    ]
    assert not leaked, (
        f"{forbidden!r} found in the OpenAPI schema description of {leaked}. "
        "Pydantic publishes the whole class docstring as the schema description "
        "(a '\\f' does not cut it), so it ends up in Swagger and in the generated "
        "clients. Keep the class docstring to a plain description and document "
        "each field with a docstring directly below it, enabling "
        "use_attribute_docstrings=True on the model (or "
        "@with_config(ConfigDict(use_attribute_docstrings=True)) on a TypedDict) "
        "so that pydantic uses them as the field descriptions."
    )


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
    assert r.status_code == HTTPStatus.BAD_REQUEST
    assert str(min_client_version) in r.json()["detail"]


def test_invalid_client_version(test_client, caplog: pytest.LogCaptureFixture):
    invalid_version = "invalid.version"
    r = test_client.get("/", headers={"DiracX-Client-Version": invalid_version})
    assert r.status_code == 400
    assert invalid_version in r.json()["detail"]
