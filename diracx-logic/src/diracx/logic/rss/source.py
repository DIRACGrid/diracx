"""Resource Status System source classes.

These classes sit in the logic layer (diracx-logic) so they can import from
diracx-db without violating the project's dependency flow:

    routers → logic → db → core

`CacheableSource` (the abstraction) and `Snapshot` live in diracx-core;
the concrete implementations live here because they need diracx-db.
"""

from __future__ import annotations

import logging
from datetime import datetime

from diracx.core.config.sources import CacheableSource, Snapshot
from diracx.db.sql.rss.db import ResourceStatusDB

from .query import (
    get_compute_statuses,
    get_fts_statuses,
    get_site_statuses,
    get_storage_statuses,
)

logger = logging.getLogger(__name__)


class ResourceStatusSource(CacheableSource[Snapshot]):
    """Caching source for Compute, Storage, and FTS resource statuses.

    Holds a long-lived reference to the app-level ``ResourceStatusDB`` instance
    (created in ``factory.py`` alongside the engine).  Each refresh uses
    ``async with self._db`` so that ``__aenter__`` runs — which sets the
    ``ContextVar`` connection — on the *same* event loop the engine is bound to.

    One source covers *all* VOs for a given resource type. VO-level filtering
    is done in the route after the snapshot is fetched, keeping the cache simple
    and avoiding N redundant poll schedules for the same underlying table.
    """

    def __init__(self, *, db: ResourceStatusDB, resource_type: str) -> None:
        """
        Args:
            db: Long-lived ``ResourceStatusDB`` instance from factory.py.
                Must already have ``engine_context`` registered (i.e. its engine
                is open for the application lifetime).
            resource_type: One of ``"ComputeElement"``, ``"StorageElement"``,
                ``"FTS"``.
        """
        super().__init__()
        self._db = db
        self._resource_type = resource_type

    async def latest_revision(self) -> tuple[str, datetime]:
        """Query the max DateEffective for this resource type across all VOs.

        Uses ``modified.isoformat()`` as the ETag so the value is deterministic
        across replicas (unlike ``hash()``, which is randomised by PYTHONHASHSEED).
        """
        async with self._db as db:
            status_date = await db.get_resource_status_date(
                resource_type=self._resource_type
            )
        modified: datetime = status_date.DateEffective
        return modified.isoformat(), modified

    async def read_raw(self, hexsha: str, modified: datetime) -> Snapshot:
        """Fetch all statuses for this resource type and wrap them in a Snapshot."""
        async with self._db as db:
            data = await self._fetch(db)
        return Snapshot(data=data, hexsha=hexsha, modified=modified)

    async def _fetch(self, db: ResourceStatusDB):
        """Dispatch to the appropriate query helper based on resource type."""
        if self._resource_type == "ComputeElement":
            return await get_compute_statuses(db, vo="all")
        if self._resource_type == "StorageElement":
            return await get_storage_statuses(db, vo="all")
        if self._resource_type == "FTS":
            return await get_fts_statuses(db, vo="all")
        raise ValueError(f"Unsupported resource_type: {self._resource_type!r}")


class SiteStatusSource(CacheableSource[Snapshot]):
    """Caching source for Site statuses.

    Sites have a first-class status row in their own table (``SiteStatus``),
    independent of the per-resource ``ResourceStatus`` table that
    ``ResourceStatusSource`` queries.  They are also always stored with
    ``vo="all"``, so no per-VO filtering is needed on the way out.

    Keeping this as a separate class (rather than a subtype of
    ``ResourceStatusSource``) avoids conflating two different DB tables and
    makes the ``latest_revision`` query correct for each.
    """

    def __init__(self, *, db: ResourceStatusDB) -> None:
        """
        Args:
            db: Long-lived ``ResourceStatusDB`` instance from factory.py.
        """
        super().__init__()
        self._db = db

    async def latest_revision(self) -> tuple[str, datetime]:
        """Query the max DateEffective from the Site status table."""
        async with self._db as db:
            status_date = await db.get_site_status_date()
        modified: datetime = status_date.DateEffective
        return modified.isoformat(), modified

    async def read_raw(self, hexsha: str, modified: datetime) -> Snapshot:
        """Fetch all site statuses and wrap them in a Snapshot."""
        async with self._db as db:
            data = await get_site_statuses(db, vo="all")
        return Snapshot(data=data, hexsha=hexsha, modified=modified)
