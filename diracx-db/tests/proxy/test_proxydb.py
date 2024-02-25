from __future__ import annotations

from functools import partial
from typing import AsyncGenerator

import pytest

from diracx.core.exceptions import DiracError
from diracx.db.sql.proxy.db import ProxyDB
from diracx.testing.proxy import (
    TEST_DATA_DIR,
    TEST_DN,
    check_proxy_string,
    insert_proxy,
    voms_init_cmd_fake,
)


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
        await insert_proxy(conn)
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


async def test_get_proxy(proxy_db: ProxyDB, monkeypatch, tmp_path):
    monkeypatch.setenv("X509_CERT_DIR", str(TEST_DATA_DIR / "certs"))
    monkeypatch.setattr(
        "diracx.db.sql.proxy.db.voms_init_cmd", partial(voms_init_cmd_fake, "fakevo")
    )

    async with proxy_db as proxy_db:
        proxy_pem = await proxy_db.get_proxy(
            TEST_DN, "fakevo", "fakevo_user", "/fakevo", 3600, tmp_path, tmp_path
        )

    check_proxy_string("fakevo", proxy_pem)
