"""Resource status SQL DB access helpers.

Provides a small DB helper around the resource and site status tables used by
the RSS (Resource Status) subsystem.
"""

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
    """Helper for interacting with the RSS database tables.

    The class implements convenience methods to read site and resource status
    entries from the RSS tables.

    Attributes:
        metadata (sqlalchemy.MetaData): Bound SQLAlchemy metadata from RSSBase.
    """

    metadata = RSSBase.metadata

    async def get_site_status(self, name: str, vo: str = "all") -> tuple[str, str]:
        """Return the status and reason for a site.

        Args:
            name (str): Site name to query.
            vo (str): Virtual organization to filter by. Defaults to "all".

        Returns:
            tuple[str, str]: The site ``status`` and ``reason``.

        Raises:
            ResourceNotFoundError: If no site status row is found for ``name``.
        """
        stmt = select(SiteStatus.status, SiteStatus.reason).where(
            SiteStatus.name == name,
            SiteStatus.status_type == "all",
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
        status_types: list[str] | None = None,
        vo: str = "all",
    ) -> dict[str, Row]:
        """Return resource status rows for a given resource name.

        Args:
            name (str): Resource name to query.
            status_types (list[str] | None): List of status types to include.
                If falsy, defaults to ["all"].
            vo (str): Virtual organization to filter by. Defaults to "all".

        Returns:
            dict[str, Row]: A mapping from status_type to the corresponding DB row.

        Raises:
            ResourceNotFoundError: If no resource status rows are found for ``name``.
        """
        if not status_types:
            status_types = ["all"]
        stmt = select(
            ResourceStatus.status, ResourceStatus.reason, ResourceStatus.status_type
        ).where(
            ResourceStatus.name == name,
            ResourceStatus.status_type.in_(status_types),
            ResourceStatus.vo == vo,
        )
        result = await self.conn.execute(stmt)
        rows = result.all()

        if not rows:
            raise ResourceNotFoundError(name)
        return {row.StatusType: row for row in rows}
