from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import insert, select
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

    async def get_site_statuses(self) -> list[tuple[str, str, str, str]]:
        """Return all site statuses across all VOs.

        Returns:
            List of (name, status, reason, vo) tuples.

        """
        stmt = select(
            SiteStatus.name,
            SiteStatus.status,
            SiteStatus.reason,
            SiteStatus.vo,
        ).where(SiteStatus.status_type == "all")
        result = await self.conn.execute(stmt)
        rows = result.all()
        if not rows:
            raise ResourceNotFoundError("Site statuses")

        return [(row.Name, row.Status, row.Reason, row.VO) for row in rows]

    async def get_resource_statuses(
        self,
        status_types: list[str] | None = None,
    ) -> dict[str, dict[str, Row]]:
        """Return resource statuses for the given status types across all VOs.

        Args:
            status_types: Status type filter (e.g. ["ReadAccess", "WriteAccess"]).
                          Defaults to ["all"].

        Returns:
            Nested dict keyed by resource name then status type.

        """
        if not status_types:
            status_types = ["all"]
        stmt = select(
            ResourceStatus.name,
            ResourceStatus.status,
            ResourceStatus.reason,
            ResourceStatus.status_type,
            ResourceStatus.vo,
        ).where(
            ResourceStatus.status_type.in_(status_types),
        )
        result = await self.conn.execute(stmt)
        rows = result.all()

        if not rows:
            raise ResourceNotFoundError("Resource statuses")

        statuses: dict[str, dict[str, Row]] = {}
        for row in rows:
            if row.Name not in statuses:
                statuses[row.Name] = {}
            statuses[row.Name][row.StatusType] = row
        return statuses

    async def get_resource_status_date(
        self,
        status_types: list[str] | None = None,
    ) -> Row[tuple[datetime, datetime]]:
        """Return the most recent DateEffective across all VOs for the given status types.

        Args:
            status_types: Status type filter. Defaults to ["all"].

        Returns:
            Row with (date_effective, last_check_time) for the most recent entry.

        """
        if not status_types:
            status_types = ["all"]
        stmt = (
            select(
                ResourceStatus.date_effective,
                ResourceStatus.last_check_time,
            )
            .where(ResourceStatus.status_type.in_(status_types))
            .order_by(ResourceStatus.date_effective.desc())
            .limit(1)
        )
        result = await self.conn.execute(stmt)
        row = result.first()
        if not row:
            raise ResourceNotFoundError("Resource statuses")
        return row

    async def get_site_status_date(self) -> Row[tuple[datetime, datetime]]:
        """Return the most recent DateEffective from the SiteStatus table across all VOs.

        Returns:
            Row with (date_effective, last_check_time) for the most recent entry.

        """
        stmt = (
            select(
                SiteStatus.date_effective,
                SiteStatus.last_check_time,
            )
            .where(SiteStatus.status_type == "all")
            .order_by(SiteStatus.date_effective.desc())
            .limit(1)
        )
        result = await self.conn.execute(stmt)
        row = result.first()
        if not row:
            raise ResourceNotFoundError("Site statuses")
        return row

    async def insert_resource_status(
        self,
        name: str,
        status: str,
        status_type: str,
        vo: str,
        reason: str = "",
        date_effective: datetime | None = None,
        last_check_time: datetime | None = None,
    ) -> None:
        """Insert a single ResourceStatus row.

        Args:
            name: Resource name.
            status: Status value.
            status_type: One of "all", "ReadAccess", "WriteAccess", etc.
            vo: Virtual organisation (e.g. "lhcb", "all").
            reason: Human-readable reason string.
            date_effective: Timestamp when the status became effective.
                            Defaults to now.
            last_check_time: Timestamp of last check. Defaults to now.

        """
        now = datetime.now(timezone.utc)
        stmt = insert(ResourceStatus).values(
            Name=name,
            Status=status,
            StatusType=status_type,
            VO=vo,
            Reason=reason,
            DateEffective=date_effective or now,
            LastCheckTime=last_check_time or now,
        )
        await self.conn.execute(stmt)

    async def insert_site_status(
        self,
        name: str,
        status: str,
        vo: str,
        reason: str = "",
        date_effective: datetime | None = None,
        last_check_time: datetime | None = None,
    ) -> None:
        """Insert a single SiteStatus row.

        Args:
            name: Site name (e.g. "LCG.CERN.cern").
            status: Status value (e.g. "Active", "Banned").
            vo: Virtual organisation.
            reason: Human-readable reason string.
            date_effective: Defaults to now.
            last_check_time: Defaults to now.

        """
        now = datetime.now(timezone.utc)
        stmt = insert(SiteStatus).values(
            Name=name,
            Status=status,
            StatusType="all",
            VO=vo,
            Reason=reason,
            DateEffective=date_effective or now,
            LastCheckTime=last_check_time or now,
        )
        await self.conn.execute(stmt)
