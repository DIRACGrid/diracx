"""Helpers to map database resource statuses to logic-layer RSS models.

This module translates raw status strings and reasons returned by the
`ResourceStatusDB` into the typed models used by the logic layer and APIs.
"""

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
    """Map a raw database status string to a `ResourceStatus` model.

    Args:
        db_status (str): The raw status string returned by the DB.
        reason (str | None): Optional explanatory reason from the DB.

    Returns:
        ResourceStatus: A typed status model that is either allowed or banned.
    """
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
    """Fetch and return the site-level resource status.

    Args:
        resource_status_db (ResourceStatusDB): DB helper to query statuses.
        name (str): Site name.
        vo (str): Virtual organisation.

    Returns:
        SiteStatusModel: A site-level status model wrapping the mapped status.
    """
    status, reason = await resource_status_db.get_site_status(name, vo)
    return SiteStatusModel(all=map_status(status, reason))


async def get_compute_status(
    resource_status_db: ResourceStatusDB, name: str, vo: str
) -> ComputeElementStatus:
    """Fetch and return a CE (Computing Element's) resource status.

    Args:
        resource_status_db (ResourceStatusDB): DB helper to query statuses.
        name (str): Computing Element name.
        vo (str): Virtual organisation.

    Returns:
        ComputeElementStatus: The compute element status model.
    """
    rows = await resource_status_db.get_resource_status(name, ["all"], vo)
    return ComputeElementStatus(all=map_status(rows["all"].Status, rows["all"].Reason))


async def get_fts_status(
    resource_status_db: ResourceStatusDB, name: str, vo: str
) -> FTSStatus:
    """Fetch and return an FTS (File Transfer Service) resource status.

    Args:
        resource_status_db (ResourceStatusDB): DB helper to query statuses.
        name (str): FTS endpoint name.
        vo (str): Virtual organisation.

    Returns:
        FTSStatus: The FTS status model.
    """
    rows = await resource_status_db.get_resource_status(name, ["all"], vo)
    return FTSStatus(all=map_status(rows["all"].Status, rows["all"].Reason))


async def get_storage_status(
    resource_status_db: ResourceStatusDB, name: str, vo: str
) -> StorageElementStatus:
    """Fetch and return a SE (Storage Element's) access-related statuses.

    Queries the DB for the specific access checks and maps each to a
    ResourceStatus model used by the logic layer.

    Args:
        resource_status_db (ResourceStatusDB): DB helper to query statuses.
        name (str): Storage Element name.
        vo (str): Virtual organisation.

    Returns:
        StorageElementStatus: The storage element status model with per-access fields.
    """
    rows = await resource_status_db.get_resource_status(
        name, ["ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"], vo
    )
    return StorageElementStatus(
        read=map_status(rows["ReadAccess"].Status, rows["ReadAccess"].Reason),
        write=map_status(rows["WriteAccess"].Status, rows["WriteAccess"].Reason),
        check=map_status(rows["CheckAccess"].Status, rows["CheckAccess"].Reason),
        remove=map_status(rows["RemoveAccess"].Status, rows["RemoveAccess"].Reason),
    )
