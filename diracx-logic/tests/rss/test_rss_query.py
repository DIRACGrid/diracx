from __future__ import annotations

from datetime import datetime, timezone

import pytest

from diracx.core.models.rss import (
    AllowedStatus,
    BannedStatus,
)
from diracx.db.sql.rss.db import ResourceStatusDB
from diracx.logic.rss.query import (
    STORAGE_STATUS_TYPES,
    get_compute_statuses,
    get_fts_statuses,
    get_site_statuses,
    get_storage_statuses,
    map_status,
)

_NOW = datetime(2024, 1, 1, tzinfo=timezone.utc)


async def test_map_status_allowed():
    result = map_status("Active", "Resource active")
    assert bool(result) is True
    assert isinstance(result, AllowedStatus)
    assert result.warnings is None

    result = map_status("Degraded", "Resource degraded")
    assert bool(result) is True
    assert isinstance(result, AllowedStatus)
    assert result.warnings == "Resource degraded"


@pytest.mark.parametrize("status", ["Banned", "Probing", "Error", "Unknown"])
async def test_map_status_banned(status):
    result = map_status(status, "Resource banned")
    assert bool(result) is False
    assert isinstance(result, BannedStatus)
    assert result.reason == "Resource banned"


async def test_map_status_unknown_banned():
    result = map_status("WeirdValue", "")
    assert bool(result) is False
    assert isinstance(result, BannedStatus)
    assert result.reason == "Unknown status: WeirdValue"


@pytest.fixture
async def rss_db():
    rss_db = ResourceStatusDB("sqlite+aiosqlite:///:memory:")
    async with rss_db.engine_context():
        async with rss_db.engine.begin() as conn:
            await conn.run_sync(rss_db.metadata.create_all)
        yield rss_db


@pytest.fixture
async def populated_rss_db(rss_db):
    """Seed a compute element and an FTS server, both with status_type "all".

    These two share a status type, so only the element_type filter keeps them
    apart. A fully-populated storage element and a site are added too.
    """
    async with rss_db as db:
        await db.insert_resource_status(
            name="TestCompute",
            status="Active",
            status_type="all",
            vo="all",
            element_type="ComputeElement",
            date_effective=_NOW,
        )
        await db.insert_resource_status(
            name="TestFTS",
            status="Active",
            status_type="all",
            vo="all",
            element_type="FTS",
            date_effective=_NOW,
        )
        for status_type in STORAGE_STATUS_TYPES:
            await db.insert_resource_status(
                name="TestStorage",
                status="Active",
                status_type=status_type,
                vo="all",
                element_type="StorageElement",
                date_effective=_NOW,
            )
        await db.insert_site_status(
            name="TestSite",
            status="Active",
            vo="all",
            date_effective=_NOW,
        )
    return rss_db


async def test_get_compute_statuses_excludes_other_element_types(populated_rss_db):
    """Compute view must not include the FTS server sharing the "all" status type."""
    async with populated_rss_db as db:
        result = await get_compute_statuses(db)
    assert set(result["all"]) == {"TestCompute"}


async def test_get_fts_statuses_excludes_other_element_types(populated_rss_db):
    """FTS view must not include the compute element sharing the "all" status type."""
    async with populated_rss_db as db:
        result = await get_fts_statuses(db)
    assert set(result["all"]) == {"TestFTS"}


async def test_get_storage_statuses_excludes_other_element_types(populated_rss_db):
    async with populated_rss_db as db:
        result = await get_storage_statuses(db)
    assert set(result["all"]) == {"TestStorage"}


async def test_get_site_statuses(populated_rss_db):
    async with populated_rss_db as db:
        result = await get_site_statuses(db)
    assert set(result["all"]) == {"TestSite"}
