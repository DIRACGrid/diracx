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

from diracx.core.models.resource_status import ResourceType
from diracx.core.sources import AsyncCacheableSource, Snapshot
from diracx.db.sql.resource_status.db import ResourceStatusDB

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

    One source instance per resource type caches a snapshot covering all VOs.
    The route overlays the caller's VO-specific entries on the shared ``all``
    entries. It uses an ETag formed from the snapshot revision and VO, and
    varies the response by ``Authorization`` for conditional HTTP caching.
    """

    db_class = ResourceStatusDB

    #: Status types holding this resource type's data, used both for the
    #: revision query and the data fetch.
    status_types: ClassVar[list[str]]

    #: Element type holding this resource type's data. Compute and FTS share
    #: the "all" status type, so the revision query must also filter on this to
    #: track exactly the rows the data fetch returns; otherwise their revisions
    #: would collide and a change to one would invalidate the other's cache.
    element_type: ClassVar[ResourceType]

    def __init__(self, *, db: ResourceStatusDB) -> None:
        """Initialize the source with its resource status database.

        Args:
            db: Database used to read status rows and revisions.
        """
        super().__init__()
        self._db = db

    async def latest_revision(self) -> tuple[str, datetime]:
        """Return the revision representing the current status rows.

        Returns:
            Revision token and modification time for this resource type.
        """
        async with self._db as db:
            max_date, count = await db.get_resource_status_date(
                self.status_types, element_type=self.element_type
            )
        return _make_revision(max_date, count)

    async def read_raw(self, hexsha: str, modified: datetime) -> Snapshot:
        """Read a cache snapshot for the current resource statuses.

        Args:
            hexsha: Revision token associated with the read.
            modified: Modification time associated with the revision.

        Returns:
            Snapshot containing statuses keyed by VO and resource name.
        """
        async with self._db as db:
            data = await self._fetch(db)
        return Snapshot(data=data, hexsha=hexsha, modified=modified)

    @abstractmethod
    async def _fetch(self, db: ResourceStatusDB) -> dict:
        """Fetch this resource type's statuses, keyed by VO then name."""


class StorageElementStatusSource(ResourceStatusSource):
    """Cache source for storage-element status data."""

    status_types = STORAGE_STATUS_TYPES
    element_type = ResourceType.Storage

    async def _fetch(self, db: ResourceStatusDB) -> dict:
        """Fetch storage-element statuses."""
        return await get_storage_statuses(db)


class ComputeElementStatusSource(ResourceStatusSource):
    """Cache source for compute-element status data."""

    status_types = ["all"]
    element_type = ResourceType.Compute

    async def _fetch(self, db: ResourceStatusDB) -> dict:
        """Fetch compute-element statuses."""
        return await get_compute_statuses(db)


class FTSStatusSource(ResourceStatusSource):
    """Cache source for file-transfer service status data."""

    status_types = ["all"]
    element_type = ResourceType.FTS

    async def _fetch(self, db: ResourceStatusDB) -> dict:
        """Fetch FTS statuses."""
        return await get_fts_statuses(db)


class SiteStatusSource(AsyncCacheableSource[Snapshot]):
    """Caching source for Site statuses.

    Uses its own DB table (SiteStatus) and a dedicated date query, so it is a
    direct subclass of AsyncCacheableSource rather than ResourceStatusSource.
    The cached snapshot covers all VOs; the route overlays the caller's
    VO-specific entries on shared ``all`` entries for the response.
    """

    db_class = ResourceStatusDB

    def __init__(self, *, db: ResourceStatusDB) -> None:
        """Initialize the source with its resource status database.

        Args:
            db: Database used to read site status rows and revisions.
        """
        super().__init__()
        self._db = db

    async def latest_revision(self) -> tuple[str, datetime]:
        """Return the revision representing the current site status rows.

        Returns:
            Revision token and modification time for site statuses.
        """
        async with self._db as db:
            max_date, count = await db.get_site_status_date()
        return _make_revision(max_date, count)

    async def read_raw(self, hexsha: str, modified: datetime) -> Snapshot:
        """Read a cache snapshot of site statuses.

        Args:
            hexsha: Revision token associated with the read.
            modified: Modification time associated with the revision.

        Returns:
            Snapshot containing site statuses keyed by VO and site name.
        """
        async with self._db as db:
            data = await get_site_statuses(db)
        return Snapshot(data=data, hexsha=hexsha, modified=modified)
