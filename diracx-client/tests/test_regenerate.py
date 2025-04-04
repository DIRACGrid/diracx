from __future__ import annotations

import pytest

from diracx.testing.client_generation import AUTOREST_VERSION, regenerate_client

pytestmark = pytest.mark.enabled_dependencies([])


@pytest.fixture
def test_client(client_factory):
    with client_factory.unauthenticated() as client:
        yield client


def test_regenerate_client(test_client, tmp_path):
    """Regenerate the AutoREST client and run pre-commit checks on it.

    This test is skipped by default, and can be enabled by passing
    --regenerate-client to pytest. It is intended to be run manually
    when the API changes.

    The reason this is a test is that it is the only way to get access to the
    test_client fixture, which is required to get the OpenAPI spec.

    WARNING: This test will modify the source code of the client!
    """
    r = test_client.get("/api/openapi.json")
    r.raise_for_status()
    openapi_spec = tmp_path / "openapi.json"
    openapi_spec.write_text(r.text)

    regenerate_client(openapi_spec, "diracx.client")


if __name__ == "__main__":
    print(AUTOREST_VERSION)
