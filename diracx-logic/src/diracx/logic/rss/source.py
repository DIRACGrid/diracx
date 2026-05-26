"""Resource Status System source classes.

These classes live in diracx-logic so they can import from diracx-db without
violating the project's dependency flow:

    routers → logic → db → core
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import ClassVar

from diracx.core.config.sources import AsyncCacheableSource
from diracx.core.models.rss import Snapshot
from diracx.db.sql.rss.db import ResourceStatusDB

from .query import (
    get_compute_statuses,
    get_fts_statuses,
    get_site_statuses,
    get_storage_statuses,
)

logger = logging.getLogger(__name__)


class ResourceStatusSource(AsyncCacheableSource):
    """Base caching source for Compute, Storage and FTS resource types.

    Subclasses declare `resource_type` as a class attribute — latest_revision
    and _fetch dispatch on it automatically.

    One source instance per resource type covers all VOs. VO filtering is done
    in the route after the snapshot is fetched from the cache.
    """

    resource_type: ClassVar[str]

    def __init__(self, *, db: ResourceStatusDB) -> None:
        super().__init__()
        self._db = db

    async def latest_revision(self) -> tuple[str, datetime]:
        async with self._db as db:
            row = await db.get_resource_status_date()
        modified: datetime = row.DateEffective
        return modified.isoformat(), modified

    async def read_raw(self, hexsha: str, modified: datetime) -> Snapshot:
        async with self._db as db:
            data = await self._fetch(db)
        return Snapshot(data=data, hexsha=hexsha, modified=modified)

    async def _fetch(self, db: ResourceStatusDB) -> dict:
        if self.resource_type == "StorageElement":
            return await get_storage_statuses(db)
        if self.resource_type == "ComputeElement":
            return await get_compute_statuses(db)
        if self.resource_type == "FTS":
            return await get_fts_statuses(db)
        raise ValueError(f"Unsupported resource_type: {self.resource_type!r}")


class StorageElementStatusSource(ResourceStatusSource):
    resource_type = "StorageElement"


class ComputeElementStatusSource(ResourceStatusSource):
    resource_type = "ComputeElement"


class FTSStatusSource(ResourceStatusSource):
    resource_type = "FTS"


class SiteStatusSource(AsyncCacheableSource):
    """Caching source for Site statuses.

    Uses its own DB table (SiteStatus) and a dedicated date query, so it is a
    direct subclass of AsyncCacheableSource rather than ResourceStatusSource.
    """

    def __init__(self, *, db: ResourceStatusDB) -> None:
        super().__init__()
        self._db = db

    async def latest_revision(self) -> tuple[str, datetime]:
        async with self._db as db:
            row = await db.get_site_status_date()
        modified: datetime = row.DateEffective
        return modified.isoformat(), modified

    async def read_raw(self, hexsha: str, modified: datetime) -> Snapshot:
        async with self._db as db:
            data = await get_site_statuses(db)
        return Snapshot(data=data, hexsha=hexsha, modified=modified)
