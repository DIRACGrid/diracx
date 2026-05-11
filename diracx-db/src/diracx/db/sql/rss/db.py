from __future__ import annotations

from datetime import datetime

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

    async def get_site_statuses(self, vo: str = "all") -> list[tuple[str, str, str]]:
        stmt = select(SiteStatus.name, SiteStatus.status, SiteStatus.reason).where(
            SiteStatus.status_type == "all",
            SiteStatus.vo == vo,
        )
        result = await self.conn.execute(stmt)
        rows = result.all()
        if not rows:
            raise ResourceNotFoundError(f"Site statuses for VO {vo}")

        return [(row.Name, row.Status, row.Reason) for row in rows]

    async def get_resource_statuses(
        self,
        status_types: list[str] | None = None,
        vo: str = "all",
    ) -> dict[str, dict[str, Row]]:
        if not status_types:
            status_types = ["all"]
        stmt = select(
            ResourceStatus.name,
            ResourceStatus.status,
            ResourceStatus.reason,
            ResourceStatus.status_type,
        ).where(
            ResourceStatus.status_type.in_(status_types),
            ResourceStatus.vo == vo,
        )
        result = await self.conn.execute(stmt)
        rows = result.all()

        if not rows:
            raise ResourceNotFoundError(f"Resource statuses for VO {vo}")
        statuses: dict[str, dict[str, Row]] = {}
        for row in rows:
            if row.Name not in statuses:
                statuses[row.Name] = {}
            statuses[row.Name][row.StatusType] = row
        return statuses

    async def get_status_date(
        self,
        status_types: list[str] | None = None,
        vo: str = "all",
    ) -> Row[tuple[datetime, datetime]]:
        if not status_types:
            status_types = ["all"]
        stmt = (
            select(
                ResourceStatus.date_effective,
                ResourceStatus.last_check_time,
            )
            .where(
                ResourceStatus.status_type.in_(status_types),
                ResourceStatus.vo == vo,
            )
            .order_by(ResourceStatus.date_effective.desc())  # the most recent date
            .limit(1)
        )
        result = await self.conn.execute(stmt)
        row = result.first()
        if not row:
            raise ResourceNotFoundError(f"Resource statuses for VO {vo}")
        return row
