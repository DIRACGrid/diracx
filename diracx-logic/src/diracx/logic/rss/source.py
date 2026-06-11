"""Resource Status System source classes.

These classes live in diracx-logic so they can import from diracx-db without
violating the project's dependency flow:

    routers → logic → db → core
"""

from __future__ import annotations

import logging
from abc import abstractmethod
from datetime import datetime, timezone
from typing import ClassVar

from diracx.core.sources import AsyncCacheableSource, Snapshot
from diracx.db.sql.rss.db import ResourceStatusDB

from .query import (
    STORAGE_STATUS_TYPES,
    get_compute_statuses,
    get_fts_statuses,
    get_site_statuses,
    get_storage_statuses,
)

logger = logging.getLogger(__name__)

#: Revision returned when the underlying table contains no rows.
EMPTY_REVISION = ("empty-0", datetime(1970, 1, 1, tzinfo=timezone.utc))


def _make_revision(max_date: datetime | None, count: int) -> tuple[str, datetime]:
    """Build a (revision, modified) pair from the latest date and row count.

    Including the row count in the revision means insertions and deletions
    change the ETag even when they do not advance the latest DateEffective.

    This relies on writes setting DateEffective to the time of the change: an
    update that neither advances max(DateEffective) nor changes the row count
    keeps the same revision, so caches only notice it once the hard TTL expires.
    """
    if max_date is None:
        return EMPTY_REVISION
    return f"{max_date.isoformat()}-{count}", max_date


class ResourceStatusSource(AsyncCacheableSource[Snapshot]):
    """Base caching source for Compute, Storage and FTS resource types.

    Subclasses declare the status types their data lives in and how to fetch
    it from the database.

    One source instance per resource type covers all VOs. VO filtering is done
    in the route after the snapshot is fetched from the cache.
    """

    db_class = ResourceStatusDB

    #: Status types holding this resource type's data, used both for the
    #: revision query and the data fetch.
    status_types: ClassVar[list[str]]

    def __init__(self, *, db: ResourceStatusDB) -> None:
        super().__init__()
        self._db = db

    async def latest_revision(self) -> tuple[str, datetime]:
        async with self._db as db:
            max_date, count = await db.get_resource_status_date(self.status_types)
        return _make_revision(max_date, count)

    async def read_raw(self, hexsha: str, modified: datetime) -> Snapshot:
        async with self._db as db:
            data = await self._fetch(db)
        return Snapshot(data=data, hexsha=hexsha, modified=modified)

    @abstractmethod
    async def _fetch(self, db: ResourceStatusDB) -> dict:
        """Fetch this resource type's statuses, keyed by VO then name."""


class StorageElementStatusSource(ResourceStatusSource):
    status_types = STORAGE_STATUS_TYPES

    async def _fetch(self, db: ResourceStatusDB) -> dict:
        return await get_storage_statuses(db)


class ComputeElementStatusSource(ResourceStatusSource):
    status_types = ["all"]

    async def _fetch(self, db: ResourceStatusDB) -> dict:
        return await get_compute_statuses(db)


class FTSStatusSource(ResourceStatusSource):
    status_types = ["all"]

    async def _fetch(self, db: ResourceStatusDB) -> dict:
        return await get_fts_statuses(db)


class SiteStatusSource(AsyncCacheableSource[Snapshot]):
    """Caching source for Site statuses.

    Uses its own DB table (SiteStatus) and a dedicated date query, so it is a
    direct subclass of AsyncCacheableSource rather than ResourceStatusSource.
    """

    db_class = ResourceStatusDB

    def __init__(self, *, db: ResourceStatusDB) -> None:
        super().__init__()
        self._db = db

    async def latest_revision(self) -> tuple[str, datetime]:
        async with self._db as db:
            max_date, count = await db.get_site_status_date()
        return _make_revision(max_date, count)

    async def read_raw(self, hexsha: str, modified: datetime) -> Snapshot:
        async with self._db as db:
            data = await get_site_statuses(db)
        return Snapshot(data=data, hexsha=hexsha, modified=modified)
