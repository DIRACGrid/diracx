from __future__ import annotations

from sqlalchemy import select

from diracx.core.models.rss import (
    ComputeElementStatus,
    FTSStatus,
    ResourceType,
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

    async def get_site_status(self, name: str, vo: str = "all") -> SiteStatusModel:
        stmt = select(SiteStatus.status, SiteStatus.reason).where(
            SiteStatus.name == name,
            SiteStatus.statustype == "all",
            SiteStatus.vo == vo,
        )
        result = await self.conn.execute(stmt)
        row = result.first()
        if not row:
            raise ValueError(f"Site {name} not found")

        return SiteStatusModel(all=map_status(row.Status, row.Reason))

    async def get_resource_status(
        self,
        name: str,
        vo: str = "all",
    ) -> ComputeElementStatus | FTSStatus:
        stmt = select(
            ResourceStatus.status, ResourceStatus.reason, ResourceStatus.elementtype
        ).where(
            ResourceStatus.name == name,
            ResourceStatus.statustype == "all",
            ResourceStatus.vo == vo,
        )
        result = await self.conn.execute(stmt)
        row = result.first()

        if not row:
            raise ValueError(f"Resource {name} not found")

        element_type = ResourceType(row.ElementType)

        if element_type == ResourceType.Compute:
            return ComputeElementStatus(all=map_status(row.Status, row.Reason))
        if element_type == ResourceType.FTS:
            return FTSStatus(all=map_status(row.Status, row.Reason))

        raise ValueError(f"Unexpected resource type {element_type}")

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
                raise ValueError(f"StorageElement {name} not found")

            return map_status(row.Status, row.Reason)

        return StorageElementStatus(
            read=await get_status("ReadAccess"),
            write=await get_status("WriteAccess"),
            check=await get_status("CheckAccess"),
            remove=await get_status("RemoveAccess"),
        )
