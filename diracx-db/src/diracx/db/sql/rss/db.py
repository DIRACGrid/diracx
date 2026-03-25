from __future__ import annotations

from sqlalchemy import select

from diracx.core.models.rss import (
    BannedStatus,
    ComputeElementStatus,
    StorageElementStatus,
    map_status,
)
from diracx.core.models.rss import (
    SiteStatus as SiteStatusModel,
)

from ..utils import BaseSQLDB
from .schema import (
    ResourceStatus,
    RSSBase,
    SiteStatus,
)


class ResourceStatusDB(BaseSQLDB):
    """Class that defines the interactions with the tables of the ResourceStatusDB."""

    metadata = RSSBase.metadata

    async def get_site_status(
        self, name: str, statustype: str = "all", vo: str = "all"
    ) -> SiteStatusModel:
        stmt = select(SiteStatus.status, SiteStatus.reason).where(
            SiteStatus.name == name,
            SiteStatus.statustype == statustype,
            SiteStatus.vo == vo,
        )
        result = await self.conn.execute(stmt)
        row = result.first()
        if not row:
            return SiteStatusModel(all=BannedStatus(allowed=False, reason="Not found"))

        return SiteStatusModel(all=map_status(row.Status, row.Reason))

    async def get_compute_status(
        self, name: str, vo: str = "all"
    ) -> ComputeElementStatus:
        stmt = select(ResourceStatus.status, ResourceStatus.reason).where(
            ResourceStatus.name == name,
            ResourceStatus.statustype == "all",
            ResourceStatus.vo == vo,
        )
        result = await self.conn.execute(stmt)
        row = result.first()

        if not row:
            return ComputeElementStatus(
                all=BannedStatus(allowed=False, reason="Not found")
            )

        return ComputeElementStatus(all=map_status(row.Status, row.Reason))

    async def get_storage_status(
        self, name: str, vo: str = "all"
    ) -> StorageElementStatus:
        async def get_status(statustype: str):
            stmt = select(ResourceStatus.status, ResourceStatus.reason).where(
                ResourceStatus.name == name,
                ResourceStatus.statustype == statustype,
                ResourceStatus.vo == vo,
            )

            result = await self.conn.execute(stmt)
            row = result.first()

            if not row:
                return BannedStatus(allowed=False, reason="Not found")

            return map_status(row.Status, row.Reason)

        return StorageElementStatus(
            read=await get_status("ReadAccess"),
            write=await get_status("WriteAccess"),
            check=await get_status("CheckAccess"),
            remove=await get_status("RemoveAccess"),
        )
