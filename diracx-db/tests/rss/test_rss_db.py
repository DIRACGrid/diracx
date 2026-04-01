from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import insert

from diracx.core.exceptions import ResourceNotFoundError
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
        status, reason = await rss_db.get_site_status("TestSite")
    assert status == "Active"
    assert reason == "All good"

    # Test with an unknow Site (should not be found)
    with pytest.raises(ResourceNotFoundError):
        async with rss_db as rss_db:
            await rss_db.get_site_status("Unknown")


async def test_resource_status(rss_db: ResourceStatusDB):
    async with rss_db.engine.begin() as conn:
        # Insert a test Compute Element
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
        # Insert a test FTS
        await conn.execute(
            insert(rss_db.metadata.tables["ResourceStatus"]).values(
                Name="TestFTS",
                StatusType="all",
                VO="all",
                Status="Active",
                Reason="All good",
                DateEffective=_NOW,
                TokenExpiration=_FAR,
                LastCheckTime=_NOW,
                ElementType="FTS",
                TokenOwner="test",
            )
        )
        # Insert a test Storage Element with all StatusType
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

    # Test with the test Compute Element (should be found)
    async with rss_db as rss_db:
        result = await rss_db.get_resource_status("TestCompute")
    assert "all" in result
    assert result["all"].Status == "Active"
    assert result["all"].Reason == "All good"

    # Test with the test FTS (should be found)
    async with rss_db as rss_db:
        result = await rss_db.get_resource_status("TestFTS")
    assert "all" in result
    assert result["all"].Status == "Active"
    assert result["all"].Reason == "All good"

    # Test with the test Storage Element (should be found)
    async with rss_db as rss_db:
        result = await rss_db.get_resource_status(
            "TestStorage", ["ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"]
        )
    assert set(result.keys()) == {
        "ReadAccess",
        "WriteAccess",
        "CheckAccess",
        "RemoveAccess",
    }
    for row in result.values():
        assert row.Status == "Active"
        assert row.Reason == "All good"

    # Test with an unknow Resource (should not be found)
    with pytest.raises(ResourceNotFoundError):
        async with rss_db as rss_db:
            await rss_db.get_resource_status("Unknown")
