from __future__ import annotations

import logging

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

logger = logging.getLogger(__name__)


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
    for vo, names in all_rows.items():
        result[vo] = {
            name: ComputeElementStatus(
                all=map_status(rows["all"].Status, rows["all"].Reason)
            )
            for name, rows in names.items()
        }

    return result


async def get_fts_statuses(
    resource_status_db: ResourceStatusDB,
) -> dict[str, dict[str, FTSStatus]]:
    """Fetch all FTS server statuses across all VOs."""
    all_rows = await resource_status_db.get_resource_statuses(["all"])

    result: dict[str, dict[str, FTSStatus]] = {}
    for vo, names in all_rows.items():
        result[vo] = {
            name: FTSStatus(all=map_status(rows["all"].Status, rows["all"].Reason))
            for name, rows in names.items()
        }

    return result


STORAGE_STATUS_TYPES = ["ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"]


async def get_storage_statuses(
    resource_status_db: ResourceStatusDB,
) -> dict[str, dict[str, StorageElementStatus]]:
    """Fetch all storage element statuses across all VOs.

    Storage elements missing one or more of the four access rows are skipped:
    their status is undefined and guessing could grant unintended access.
    """
    all_rows = await resource_status_db.get_resource_statuses(STORAGE_STATUS_TYPES)

    result: dict[str, dict[str, StorageElementStatus]] = {}
    for vo, names in all_rows.items():
        result[vo] = {}
        for name, rows in names.items():
            if missing := [st for st in STORAGE_STATUS_TYPES if st not in rows]:
                logger.warning(
                    "Skipping storage element %r (vo=%r): missing status types %s",
                    name,
                    vo,
                    missing,
                )
                continue
            result[vo][name] = StorageElementStatus(
                read=map_status(rows["ReadAccess"].Status, rows["ReadAccess"].Reason),
                write=map_status(
                    rows["WriteAccess"].Status, rows["WriteAccess"].Reason
                ),
                check=map_status(
                    rows["CheckAccess"].Status, rows["CheckAccess"].Reason
                ),
                remove=map_status(
                    rows["RemoveAccess"].Status, rows["RemoveAccess"].Reason
                ),
            )

    return result
