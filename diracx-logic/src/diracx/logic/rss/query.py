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


async def get_site_status(
    resource_status_db: ResourceStatusDB, name: str, vo: str
) -> SiteStatusModel:
    status, reason = await resource_status_db.get_site_status(name, vo)
    return SiteStatusModel(all=map_status(status, reason))


async def get_compute_status(
    resource_status_db: ResourceStatusDB, name: str, vo: str
) -> ComputeElementStatus:
    rows = await resource_status_db.get_resource_status(name, ["all"], vo)
    return ComputeElementStatus(all=map_status(rows["all"].Status, rows["all"].Reason))


async def get_fts_status(
    resource_status_db: ResourceStatusDB, name: str, vo: str
) -> FTSStatus:
    rows = await resource_status_db.get_resource_status(name, ["all"], vo)
    return FTSStatus(all=map_status(rows["all"].Status, rows["all"].Reason))


async def get_storage_status(
    resource_status_db: ResourceStatusDB, name: str, vo: str
) -> StorageElementStatus:
    rows = await resource_status_db.get_resource_status(
        name, ["ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"], vo
    )
    return StorageElementStatus(
        read=map_status(rows["ReadAccess"].Status, rows["ReadAccess"].Reason),
        write=map_status(rows["WriteAccess"].Status, rows["WriteAccess"].Reason),
        check=map_status(rows["CheckAccess"].Status, rows["CheckAccess"].Reason),
        remove=map_status(rows["RemoveAccess"].Status, rows["RemoveAccess"].Reason),
    )
