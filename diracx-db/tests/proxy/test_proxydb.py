from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path
from typing import AsyncGenerator

import pytest
from DIRAC.Core.Security.VOMS import voms_init_cmd
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
from sqlalchemy import insert

from diracx.core.exceptions import DiracError
from diracx.db.sql.proxy.db import ProxyDB
from diracx.db.sql.proxy.schema import CleanProxies

TEST_NAME = "testuser"
TEST_DN = "/O=Dirac Computing/O=CERN/CN=MrUser"
TEST_DATA_DIR = Path(__file__).parent / "data"
TEST_PEM_PATH = TEST_DATA_DIR / "proxy.pem"


@pytest.fixture
async def empty_proxy_db(tmp_path) -> AsyncGenerator[ProxyDB, None]:
    proxy_db = ProxyDB("sqlite+aiosqlite:///:memory:")
    async with proxy_db.engine_context():
        async with proxy_db.engine.begin() as conn:
            await conn.run_sync(proxy_db.metadata.create_all)
        yield proxy_db


@pytest.fixture
async def proxy_db(empty_proxy_db) -> AsyncGenerator[ProxyDB, None]:
    async with empty_proxy_db.engine.begin() as conn:
        await conn.execute(
            insert(CleanProxies).values(
                UserName=TEST_NAME,
                UserDN=TEST_DN,
                ProxyProvider="Certificate",
                Pem=TEST_PEM_PATH.read_bytes(),
                ExpirationTime=datetime(2033, 11, 25, 21, 25, 23, tzinfo=timezone.utc),
            )
        )
    yield empty_proxy_db


async def test_get_stored_proxy(proxy_db: ProxyDB):
    async with proxy_db as proxy_db:
        proxy = await proxy_db.get_stored_proxy(TEST_DN, min_lifetime_seconds=3600)
    assert proxy


async def test_no_proxy_for_dn_1(empty_proxy_db: ProxyDB):
    async with empty_proxy_db as proxy_db:
        with pytest.raises(DiracError, match="No proxy found"):
            await proxy_db.get_stored_proxy(TEST_DN, min_lifetime_seconds=3600)


async def test_no_proxy_for_dn_2(empty_proxy_db: ProxyDB):
    async with empty_proxy_db as proxy_db:
        with pytest.raises(DiracError, match="No proxy found"):
            await proxy_db.get_stored_proxy(
                "/O=OtherOrg/O=CERN/CN=MrUser", min_lifetime_seconds=3600
            )


async def test_proxy_not_long_enough(proxy_db: ProxyDB):
    async with proxy_db as proxy_db:
        with pytest.raises(DiracError, match="No proxy found"):
            # The test proxy we use is valid for 10 years
            # If this code still exists in 2028 we might start having problems with 2K38
            await proxy_db.get_stored_proxy(
                TEST_DN, min_lifetime_seconds=10 * 365 * 24 * 3600
            )


@wraps(voms_init_cmd)
def voms_init_cmd_fake(*args, **kwargs):
    cmd = voms_init_cmd(*args, **kwargs)

    new_cmd = ["voms-proxy-fake"]
    i = 1
    while i < len(cmd):
        # Some options are not supported by voms-proxy-fake
        if cmd[i] in {"-valid", "-vomses", "-timeout"}:
            i += 2
            continue
        new_cmd.append(cmd[i])
        i += 1
    new_cmd.extend(
        [
            "-hostcert",
            f"{TEST_DATA_DIR}/certs/host/hostcert.pem",
            "-hostkey",
            f"{TEST_DATA_DIR}/certs/host/hostkey.pem",
            "-fqan",
            "/fakevo/Role=NULL/Capability=NULL",
        ]
    )
    return new_cmd


async def test_get_proxy(proxy_db: ProxyDB, monkeypatch, tmp_path):
    monkeypatch.setenv("X509_CERT_DIR", str(TEST_DATA_DIR / "certs"))
    monkeypatch.setattr("diracx.db.sql.proxy.db.voms_init_cmd", voms_init_cmd_fake)

    async with proxy_db as proxy_db:
        proxy_pem = await proxy_db.get_proxy(
            TEST_DN, "fakevo", "fakevo_user", "/fakevo", 3600, tmp_path, tmp_path
        )

    proxy_chain = X509Chain()
    returnValueOrRaise(proxy_chain.loadProxyFromString(proxy_pem))

    # Check validity
    not_after = returnValueOrRaise(proxy_chain.getNotAfterDate()).replace(
        tzinfo=timezone.utc
    )
    # The proxy should currently be valid
    assert datetime.now(timezone.utc) < not_after
    # The proxy should be invalid in less than 3601 seconds
    time_left = not_after - datetime.now(timezone.utc)
    assert time_left < timedelta(hours=1, seconds=1)

    # Check VOMS data
    voms_data = returnValueOrRaise(proxy_chain.getVOMSData())
    assert voms_data["vo"] == "fakevo"
    assert voms_data["fqan"] == ["/fakevo/Role=NULL/Capability=NULL"]
