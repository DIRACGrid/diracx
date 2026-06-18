from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, insert, select
from sqlalchemy.engine import Row

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
        )
        result = await self.conn.execute(stmt)
        return [(row.Name, row.Status, row.Reason, row.VO) for row in result.all()]

    async def get_resource_statuses(
        self,
        status_types: list[str] | None = None,
        element_type: str | None = None,
    ) -> dict[str, dict[str, dict[str, Row]]]:
        """Return resource statuses for the given status types across all VOs.

        Args:
            status_types: Status type filter (e.g. ["ReadAccess", "WriteAccess"]).
                          Defaults to ["all"].
            element_type: Element type filter (e.g. "ComputeElement", "FTS",
                          "StorageElement"). Defaults to None (no filter), which
                          returns every element type and would mix, say, compute
                          and FTS rows that both use the "all" status type.

        Returns:
            Nested dict keyed by VO, then resource name, then status type. The
            VO must be part of the key as (Name, StatusType, VO) is the
            table's primary key: the same resource can have rows in several VOs.

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
        if element_type is not None:
            stmt = stmt.where(ResourceStatus.element_type == element_type)
        result = await self.conn.execute(stmt)

        statuses: dict[str, dict[str, dict[str, Row]]] = {}
        for row in result.all():
            vo = row.VO or "all"
            statuses.setdefault(vo, {}).setdefault(row.Name, {})[row.StatusType] = row
        return statuses

    async def get_resource_status_date(
        self,
        status_types: list[str] | None = None,
        element_type: str | None = None,
    ) -> tuple[datetime | None, int]:
        """Return the most recent DateEffective and row count for the given status types.

        The pair is used as the cache revision (and ultimately the HTTP ETag),
        so it relies on every status write setting DateEffective to the time
        of the change: an update that neither advances max(DateEffective) nor
        changes the row count is invisible to caches until the hard TTL expires.

        Args:
            status_types: Status type filter. Defaults to ["all"].
            element_type: Element type filter. Defaults to None (no filter). Pass
                          the same value as get_resource_statuses so the revision
                          tracks exactly the rows that view returns.

        Returns:
            (max_date_effective, row_count) across all VOs. The date is None
            when the table contains no matching rows.

        """
        if not status_types:
            status_types = ["all"]
        stmt = select(
            func.max(ResourceStatus.date_effective),
            func.count(),
        ).where(ResourceStatus.status_type.in_(status_types))
        if element_type is not None:
            stmt = stmt.where(ResourceStatus.element_type == element_type)
        result = await self.conn.execute(stmt)
        max_date, count = result.one()
        return max_date, count

    async def get_site_status_date(self) -> tuple[datetime | None, int]:
        """Return the most recent DateEffective and row count from the SiteStatus table.

        As with get_resource_status_date, the pair serves as the cache
        revision, so writes must set DateEffective to the time of the change
        for caches to notice updates.

        Returns:
            (max_date_effective, row_count) across all VOs. The date is None
            when the table contains no matching rows.

        """
        stmt = select(
            func.max(SiteStatus.date_effective),
            func.count(),
        )
        result = await self.conn.execute(stmt)
        max_date, count = result.one()
        return max_date, count

    async def insert_resource_status(
        self,
        name: str,
        status: str,
        status_type: str,
        vo: str,
        element_type: str,
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
            element_type: One of "ComputeElement", "FTS", "StorageElement". Read
                          queries filter on this, so it must be set correctly.
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
            ElementType=element_type,
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
            ElementType="Site",
            Reason=reason,
            DateEffective=date_effective or now,
            LastCheckTime=last_check_time or now,
        )
        await self.conn.execute(stmt)
