# diracx-db/tests/sql/rss/test_rss.py
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import insert

from diracx.core.models.rss import (
    AllowedStatus,
    BannedStatus,
    ComputeElementStatus,
    StorageElementStatus,
    map_status,
)
from diracx.core.models.rss import SiteStatus as SiteStatusModel
from diracx.db.sql.rss.db import ResourceStatusDB

_NOW = datetime(2024, 1, 1, tzinfo=timezone.utc)
_FAR = datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc)


@pytest.fixture
async def rss_db(tmp_path):
    rss_db = ResourceStatusDB("sqlite+aiosqlite:///:memory:")
    async with rss_db.engine_context():
        async with rss_db.engine.begin() as conn:
            await conn.run_sync(rss_db.metadata.create_all)
        yield rss_db


@pytest.mark.parametrize("status", ["Active", "Degraded"])
async def test_map_status_allowed(status):
    assert bool(map_status(status, "")) is True
    assert isinstance(map_status(status, ""), AllowedStatus)


@pytest.mark.parametrize("status", ["Banned", "Probing", "Error", "Unknown"])
async def test_map_status_banned(status):
    result = map_status(status, "CE banned")
    assert bool(result) is False
    assert isinstance(result, BannedStatus)
    assert result.reason == "CE banned"


async def test_map_status_unknown_banned():
    result = map_status("WeirdValue", "")
    assert bool(result) is False
    assert isinstance(result, BannedStatus)
    assert result.reason == "Unknown status: WeirdValue"


async def test_site_status(rss_db: ResourceStatusDB):
    # Insert a test Site
    async with rss_db.engine.begin() as conn:
        await conn.execute(
            insert(rss_db.metadata.tables["SiteStatus"]).values(
                Name="TestSite",
                StatusType="all",
                VO="all",
                Status="Active",
                Reason="All good",
                DateEffective=_NOW,
                TokenExpiration=_FAR,
                LastCheckTime=_NOW,
                ElementType="Site",
                TokenOwner="test",
            )
        )

    # Test with the test Site (should be found)
    async with rss_db as rss_db:
        result = await rss_db.get_site_status("TestSite")
    assert isinstance(result, SiteStatusModel)
    assert isinstance(result.all, AllowedStatus)
    assert bool(result.all) is True

    # Test with an unknow Site (should not be found)
    async with rss_db as rss_db:
        result = await rss_db.get_site_status("UnknownSite")
    assert isinstance(result, SiteStatusModel)
    assert isinstance(result.all, BannedStatus)
    assert bool(result.all) is False
    assert result.all.reason == "Not found"


async def test_compute_status(rss_db: ResourceStatusDB):
    # Insert a test Compute Element
    async with rss_db.engine.begin() as conn:
        await conn.execute(
            insert(rss_db.metadata.tables["ResourceStatus"]).values(
                Name="TestCompute",
                StatusType="all",
                VO="all",
                Status="Active",
                Reason="All good",
                DateEffective=_NOW,
                TokenExpiration=_FAR,
                LastCheckTime=_NOW,
                ElementType="ComputeElement",
                TokenOwner="test",
            )
        )

    # Test with the test Compute Element (should be found)
    async with rss_db as rss_db:
        result = await rss_db.get_compute_status("TestCompute")
    assert isinstance(result, ComputeElementStatus)
    assert isinstance(result.all, AllowedStatus)
    assert bool(result.all) is True

    # Test with an unknow Compute Element (should not be found)
    async with rss_db as rss_db:
        result = await rss_db.get_compute_status("UnknownCompute")
    assert isinstance(result, ComputeElementStatus)
    assert isinstance(result.all, BannedStatus)
    assert bool(result.all) is False
    assert result.all.reason == "Not found"


async def test_storage_status(rss_db: ResourceStatusDB):
    # Insert a test Storage Element with all StatusType
    async with rss_db.engine.begin() as conn:
        for statustype in ["ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"]:
            await conn.execute(
                insert(rss_db.metadata.tables["ResourceStatus"]).values(
                    Name="TestStorage",
                    StatusType=statustype,
                    VO="all",
                    Status="Active",
                    Reason="All good",
                    DateEffective=_NOW,
                    TokenExpiration=_FAR,
                    LastCheckTime=_NOW,
                    ElementType="StorageElement",
                    TokenOwner="test",
                )
            )

    # Test with the test Storage Element (should be found)
    async with rss_db as rss_db:
        result = await rss_db.get_storage_status("TestStorage")
    assert isinstance(result, StorageElementStatus)
    assert isinstance(result.read, AllowedStatus)
    assert isinstance(result.write, AllowedStatus)
    assert isinstance(result.check, AllowedStatus)
    assert isinstance(result.remove, AllowedStatus)
    assert bool(result.read) is True
    assert bool(result.write) is True
    assert bool(result.check) is True
    assert bool(result.remove) is True

    # Test with an unknow Storage Element (should not be found)
    async with rss_db as rss_db:
        result = await rss_db.get_storage_status("UnknownStorage")
    assert isinstance(result, StorageElementStatus)
    assert isinstance(result.read, BannedStatus)
    assert isinstance(result.write, BannedStatus)
    assert isinstance(result.check, BannedStatus)
    assert isinstance(result.remove, BannedStatus)
    assert bool(result.read) is False
    assert bool(result.write) is False
    assert bool(result.check) is False
    assert bool(result.remove) is False
    assert result.read.reason == "Not found"
    assert result.write.reason == "Not found"
    assert result.check.reason == "Not found"
    assert result.remove.reason == "Not found"
