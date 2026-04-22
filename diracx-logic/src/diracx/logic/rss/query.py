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
    resource_status_db: ResourceStatusDB, vo: str
) -> dict[str, SiteStatusModel]:
    rows = await resource_status_db.get_site_statuses(vo)
    return {
        name: SiteStatusModel(all=map_status(status, reason))
        for name, status, reason in rows
    }


async def get_compute_statuses(
    resource_status_db: ResourceStatusDB, vo: str
) -> dict[str, ComputeElementStatus]:
    all_rows = await resource_status_db.get_resource_statuses(["all"], vo)
    return {
        name: ComputeElementStatus(
            all=map_status(rows["all"].Status, rows["all"].Reason)
        )
        for name, rows in all_rows.items()
    }


async def get_fts_statuses(
    resource_status_db: ResourceStatusDB, vo: str
) -> dict[str, FTSStatus]:
    all_rows = await resource_status_db.get_resource_statuses(["all"], vo)
    return {
        name: FTSStatus(all=map_status(rows["all"].Status, rows["all"].Reason))
        for name, rows in all_rows.items()
    }


async def get_storage_statuses(
    resource_status_db: ResourceStatusDB, vo: str
) -> dict[str, StorageElementStatus]:
    all_rows = await resource_status_db.get_resource_statuses(
        ["ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"], vo
    )
    return {
        name: StorageElementStatus(
            read=map_status(rows["ReadAccess"].Status, rows["ReadAccess"].Reason),
            write=map_status(rows["WriteAccess"].Status, rows["WriteAccess"].Reason),
            check=map_status(rows["CheckAccess"].Status, rows["CheckAccess"].Reason),
            remove=map_status(rows["RemoveAccess"].Status, rows["RemoveAccess"].Reason),
        )
        for name, rows in all_rows.items()
    }
