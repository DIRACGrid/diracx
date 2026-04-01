from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.engine import Row

from diracx.core.exceptions import ResourceNotFoundError

from ..utils import BaseSQLDB
from .schema import (
    ResourceStatus,
    RSSBase,
    SiteStatus,
)


class ResourceStatusDB(BaseSQLDB):
    """Class that defines the interactions with the tables of the ResourceStatusDB."""

    metadata = RSSBase.metadata

    async def get_site_status(self, name: str, vo: str = "all") -> tuple[str, str]:
        stmt = select(SiteStatus.status, SiteStatus.reason).where(
            SiteStatus.name == name,
            SiteStatus.statustype == "all",
            SiteStatus.vo == vo,
        )
        result = await self.conn.execute(stmt)
        row = result.one_or_none()
        if not row:
            raise ResourceNotFoundError(name)

        return row.Status, row.Reason

    async def get_resource_status(
        self,
        name: str,
        statustypes: list[str] = ["all"],
        vo: str = "all",
    ) -> dict[str, Row]:
        stmt = select(
            ResourceStatus.status, ResourceStatus.reason, ResourceStatus.statustype
        ).where(
            ResourceStatus.name == name,
            ResourceStatus.statustype.in_(statustypes),
            ResourceStatus.vo == vo,
        )
        result = await self.conn.execute(stmt)
        rows = result.all()

        if not rows:
            raise ResourceNotFoundError(name)
        return {row.StatusType: row for row in rows}
