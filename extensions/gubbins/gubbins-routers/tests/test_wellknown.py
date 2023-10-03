"""
Test the extended well_known endpoint
"""

import pytest
from fastapi import status

pytestmark = pytest.mark.enabled_dependencies(
    ["AuthSettings", "ConfigSource", "BaseAccessPolicy", "DevelopmentSettings"]
)


@pytest.fixture
def test_client(client_factory):
    with client_factory.unauthenticated() as client:
        yield client


async def test_dirac_metadata_is_overwriten(test_client):
    """
    Makes sure that the dirac-metadata endpoint is properly overwriten
    """
    r = test_client.get(
        "/.well-known/dirac-metadata",
    )
    assert r.status_code == 200, r.json()
    assert "gubbins_secrets" in r.json(), r.json()


async def test_openid_configuration_is_not_changed(test_client):
    """test that the endpoint still exists and is unchanged"""

    r = test_client.get(
        "/.well-known/openid-configuration",
    )
    assert r.status_code == status.HTTP_200_OK, r.json()
    assert "authorization_endpoint" in r.json(), r.json()
