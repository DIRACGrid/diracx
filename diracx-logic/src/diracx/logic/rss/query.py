from __future__ import annotations

from diracx.core.models.rss import (
    ALLOWED,
    BANNED,
    AllowedStatus,
    BannedStatus,
    ComputeElementStatus,
    FTSStatus,
    ResourceStatus,
    StorageElementStatus,
)
from diracx.core.models.rss import (
    SiteStatus as SiteStatusModel,
)
from diracx.db.sql import ResourceStatusDB


def map_status(db_status: str, reason: str | None = None) -> ResourceStatus:
    if db_status in ALLOWED:
        return AllowedStatus(
            allowed=True,
            warnings=reason or db_status if db_status == "Degraded" else None,
        )

    if db_status in BANNED:
        return BannedStatus(
            allowed=False,
            reason=reason or db_status,
        )

    return BannedStatus(
        allowed=False,
        reason=f"Unknown status: {db_status}",
    )


async def get_site_statuses(
    resource_status_db: ResourceStatusDB,
) -> dict[str, dict[str, SiteStatusModel]]:
    """Fetch all site statuses across all VOs.

    The returned models carry the vo field so the router can filter to the
    caller's VO from the cached all-VO snapshot.
    """
    rows = await resource_status_db.get_site_statuses()

    result: dict[str, dict[str, SiteStatusModel]] = {}

    for name, status, reason, vo in rows:
        vo = vo or "all"
        if vo not in result:
            result[vo] = {}
        result[vo][name] = SiteStatusModel(all=map_status(status, reason))

    return result


async def get_compute_statuses(
    resource_status_db: ResourceStatusDB,
) -> dict[str, dict[str, ComputeElementStatus]]:
    """Fetch all compute element statuses across all VOs."""
    all_rows = await resource_status_db.get_resource_statuses(["all"])

    result: dict[str, dict[str, ComputeElementStatus]] = {}
    for name, rows in all_rows.items():
        vo = rows["all"].VO or "all"
        if vo not in result:
            result[vo] = {}
        result[vo][name] = ComputeElementStatus(
            all=map_status(rows["all"].Status, rows["all"].Reason)
        )

    return result


async def get_fts_statuses(
    resource_status_db: ResourceStatusDB,
) -> dict[str, dict[str, FTSStatus]]:
    """Fetch all FTS server statuses across all VOs."""
    all_rows = await resource_status_db.get_resource_statuses(["all"])

    result: dict[str, dict[str, FTSStatus]] = {}
    for name, rows in all_rows.items():
        vo = rows["all"].VO or "all"
        if vo not in result:
            result[vo] = {}
        result[vo][name] = FTSStatus(
            all=map_status(rows["all"].Status, rows["all"].Reason)
        )

    return result


async def get_storage_statuses(
    resource_status_db: ResourceStatusDB,
) -> dict[str, dict[str, StorageElementStatus]]:
    """Fetch all storage element statuses across all VOs."""
    all_rows = await resource_status_db.get_resource_statuses(
        ["ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"]
    )

    result: dict[str, dict[str, StorageElementStatus]] = {}
    for name, rows in all_rows.items():
        vo = rows["ReadAccess"].VO or "all"
        if vo not in result:
            result[vo] = {}
        result[vo][name] = StorageElementStatus(
            read=map_status(rows["ReadAccess"].Status, rows["ReadAccess"].Reason),
            write=map_status(rows["WriteAccess"].Status, rows["WriteAccess"].Reason),
            check=map_status(rows["CheckAccess"].Status, rows["CheckAccess"].Reason),
            remove=map_status(rows["RemoveAccess"].Status, rows["RemoveAccess"].Reason),
        )

    return result
