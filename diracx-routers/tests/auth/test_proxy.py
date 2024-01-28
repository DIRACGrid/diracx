from functools import partial

import pytest

from diracx.db.sql import ProxyDB
from diracx.testing.proxy import (
    TEST_DATA_DIR,
    check_proxy_string,
    insert_proxy,
    voms_init_cmd_fake,
)

DIRAC_CLIENT_ID = "myDIRACClientID"
pytestmark = pytest.mark.enabled_dependencies(
    ["AuthDB", "AuthSettings", "ConfigSource", "ProxyDB"]
)


@pytest.fixture
def test_client(client_factory):
    with client_factory.normal_user(
        sub="b824d4dc-1f9d-4ee8-8df5-c0ae55d46041", group="lhcb_user"
    ) as client:
        yield client


async def test_valid(client_factory, test_client, monkeypatch):
    proxy_db = client_factory.app.dependency_overrides[ProxyDB.transaction].args[0]
    async with proxy_db as db:
        await insert_proxy(db.conn)

    monkeypatch.setenv("X509_CERT_DIR", str(TEST_DATA_DIR / "certs"))
    monkeypatch.setattr(
        "diracx.db.sql.proxy.db.voms_init_cmd", partial(voms_init_cmd_fake, "lhcb")
    )

    r = test_client.get("/api/auth/proxy")
    assert r.status_code == 200, r.json()
    pem_data = r.json()

    check_proxy_string("lhcb", pem_data)


async def test_wrong_properties(client_factory):
    """Ensure that limited JWTs are rejected to prevent privilege escalation"""
    from diracx.core.properties import NORMAL_USER

    with client_factory.normal_user(
        group="lhcb_user", properties=[NORMAL_USER]
    ) as client:
        r = client.get("/api/auth/proxy")
        assert r.status_code == 403
        assert "group default properties" in r.json()["detail"]


async def test_no_proxy_uploaded(test_client):
    r = test_client.get("/api/auth/proxy")
    assert r.status_code == 400, r.json()
    assert "No available proxy" in r.json()["detail"]
