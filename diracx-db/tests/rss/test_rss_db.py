from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import insert

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
    async with rss_db as db:
        rows = await db.get_site_statuses()
    assert rows
    name, status, reason, vo = rows[0]
    assert name == "TestSite"
    assert status == "Active"
    assert reason == "All good"
    assert vo == "all"


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
    async with rss_db as db:
        result = await db.get_resource_statuses()
    assert "TestCompute" in result["all"]
    row = result["all"]["TestCompute"]["all"]
    assert row.Status == "Active"
    assert row.Reason == "All good"
    assert row.VO == "all"

    # Test with the test FTS (should be found)
    async with rss_db as db:
        result = await db.get_resource_statuses()
    assert "TestFTS" in result["all"]
    row = result["all"]["TestFTS"]["all"]
    assert row.Status == "Active"
    assert row.Reason == "All good"
    assert row.VO == "all"

    # Test with the test Storage Element (should be found)
    async with rss_db as db:
        result = await db.get_resource_statuses(
            ["ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"]
        )
    assert set(result["all"]["TestStorage"].keys()) == {
        "ReadAccess",
        "WriteAccess",
        "CheckAccess",
        "RemoveAccess",
    }
    for row in result["all"]["TestStorage"].values():
        assert row.Status == "Active"
        assert row.Reason == "All good"

    # The date queries should return the latest date and the row count
    async with rss_db as db:
        max_date, count = await db.get_resource_status_date()
    assert max_date == _NOW
    assert count == 2  # TestCompute + TestFTS "all" rows

    async with rss_db as db:
        max_date, count = await db.get_resource_status_date(
            ["ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"]
        )
    assert max_date == _NOW
    assert count == 4  # TestStorage access rows

    # Filtering by element_type must not leak rows of other element types:
    # TestCompute and TestFTS both use status_type "all".
    async with rss_db as db:
        compute = await db.get_resource_statuses(["all"], element_type="ComputeElement")
        fts = await db.get_resource_statuses(["all"], element_type="FTS")
    assert set(compute["all"]) == {"TestCompute"}
    assert set(fts["all"]) == {"TestFTS"}

    async with rss_db as db:
        max_date, count = await db.get_resource_status_date(
            ["all"], element_type="ComputeElement"
        )
    assert max_date == _NOW
    assert count == 1  # only TestCompute, not TestFTS


async def test_resource_status_same_name_multiple_vos(rss_db: ResourceStatusDB):
    """Rows for the same resource in different VOs must not overwrite each other."""
    async with rss_db as db:
        await db.insert_resource_status(
            name="SharedCE",
            status="Active",
            status_type="all",
            vo="lhcb",
            element_type="ComputeElement",
            date_effective=_NOW,
        )
        await db.insert_resource_status(
            name="SharedCE",
            status="Banned",
            status_type="all",
            vo="atlas",
            element_type="ComputeElement",
            date_effective=_NOW,
        )
        result = await db.get_resource_statuses()
    assert result["lhcb"]["SharedCE"]["all"].Status == "Active"
    assert result["atlas"]["SharedCE"]["all"].Status == "Banned"


async def test_empty_tables(rss_db: ResourceStatusDB):
    """Empty tables yield empty results rather than errors."""
    async with rss_db as db:
        assert await db.get_site_statuses() == []
        assert await db.get_resource_statuses() == {}
        assert await db.get_resource_status_date() == (None, 0)
        assert await db.get_site_status_date() == (None, 0)


async def test_site_status_date(rss_db: ResourceStatusDB):
    async with rss_db as db:
        await db.insert_site_status(
            name="LCG.CERN.cern",
            status="Active",
            vo="lhcb",
            reason="All good",
            date_effective=_NOW,
        )
        max_date, count = await db.get_site_status_date()
    assert max_date == _NOW
    assert count == 1


async def test_insert_resource_status_sets_element_type(rss_db: ResourceStatusDB):
    """insert_resource_status must persist ElementType so reads can filter on it."""
    async with rss_db as db:
        await db.insert_resource_status(
            name="ComputeOnly",
            status="Active",
            status_type="all",
            vo="lhcb",
            element_type="ComputeElement",
            date_effective=_NOW,
        )
        await db.insert_resource_status(
            name="FTSOnly",
            status="Active",
            status_type="all",
            vo="lhcb",
            element_type="FTS",
            date_effective=_NOW,
        )
        compute = await db.get_resource_statuses(["all"], element_type="ComputeElement")
        fts = await db.get_resource_statuses(["all"], element_type="FTS")
    assert set(compute["lhcb"]) == {"ComputeOnly"}
    assert set(fts["lhcb"]) == {"FTSOnly"}
