from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timezone
from enum import StrEnum, auto
from typing import Annotated, cast

from fastapi import (
    Depends,
    Header,
    HTTPException,
    Response,
    status,
)

from diracx.core.config.sources import Snapshot
from diracx.core.models.rss import (
    ComputeElementStatus,
    FTSStatus,
    SiteStatus,
    StorageElementStatus,
)
from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils.users import AuthorizedUserInfo, verify_dirac_access_token

from .fastapi_classes import DiracxRouter

logger = logging.getLogger(__name__)

LAST_MODIFIED_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"

router = DiracxRouter()


# ---------------------------------------------------------------------------
# Access policy
# ---------------------------------------------------------------------------


class ActionType(StrEnum):
    # Create a job or a sandbox
    CREATE = auto()
    # Check job status, download a sandbox
    READ = auto()
    # Delete, kill, remove, set status, etc of a job
    # Delete or assign a sandbox
    MANAGE = auto()
    # Search
    QUERY = auto()
    # Actions from a pilot (e.g. heartbeat)
    PILOT = auto()


class ResourceStatusAccessPolicy(BaseAccessPolicy):
    """Policy: any authenticated user may READ resource statuses.

    Write/admin actions are rejected here; VO scoping is the route's responsibility.
    Registered under ``[project.entry-points."diracx.access_policies"]`` in
    ``diracx-routers/pyproject.toml`` so the framework can discover it.
    """

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
        *,
        action: ActionType | None = None,
    ):
        if action != ActionType.READ:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Resource Status System is read-only.",
            )
        # Any authenticated user may read; VO scoping happens in the route.


ResourceStatusAccessPolicyCallable = Annotated[
    Callable, Depends(ResourceStatusAccessPolicy.check)
]


class RSSSnapshotSentinels:
    @classmethod
    def get_storage_snapshot(cls) -> Snapshot:
        raise NotImplementedError

    @classmethod
    def get_compute_snapshot(cls) -> Snapshot:
        raise NotImplementedError

    @classmethod
    def get_site_snapshot(cls) -> Snapshot:
        raise NotImplementedError

    @classmethod
    def get_fts_snapshot(cls) -> Snapshot:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Shared ETag / 304 helper
# ---------------------------------------------------------------------------


def _apply_cache_headers(
    response: Response,
    snapshot: Snapshot,
    if_none_match: str | None,
    if_modified_since: str | None,
) -> None:
    """Set ETag / Last-Modified headers and raise 304 when appropriate.

    Raises:
        HTTPException(304): when the client's cached copy is still current.
    """
    headers = {
        "ETag": snapshot.hexsha,
        "Last-Modified": snapshot.modified.strftime(LAST_MODIFIED_FORMAT),
    }

    if if_none_match == snapshot.hexsha:
        raise HTTPException(status_code=status.HTTP_304_NOT_MODIFIED, headers=headers)

    if if_modified_since:
        try:
            not_before = datetime.strptime(
                if_modified_since, LAST_MODIFIED_FORMAT
            ).astimezone(timezone.utc)
        except ValueError:
            logger.debug(
                "Failed to parse If-Modified-Since header: %s", if_modified_since
            )
        else:
            # Guard against flip-flop when a replica is momentarily behind.
            if not_before > snapshot.modified:
                raise HTTPException(
                    status_code=status.HTTP_304_NOT_MODIFIED, headers=headers
                )

    response.headers.update(headers)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/storage")
async def get_storage_status(
    response: Response,
    snapshot: Annotated[Snapshot, Depends(RSSSnapshotSentinels.get_storage_snapshot)],
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: ResourceStatusAccessPolicyCallable,
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
) -> dict[str, StorageElementStatus]:
    """Get the latest status of storage elements, scoped to the caller's VO."""
    await check_permissions(action=ActionType.READ)
    _apply_cache_headers(response, snapshot, if_none_match, if_modified_since)
    return cast(
        dict[str, StorageElementStatus],
        {
            name: se
            for name, se in snapshot.data.items()
            if getattr(se, "vo", "all") in (user_info.vo, "all")
        },
    )


@router.get("/compute")
async def get_compute_status(
    response: Response,
    snapshot: Annotated[Snapshot, Depends(RSSSnapshotSentinels.get_compute_snapshot)],
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: ResourceStatusAccessPolicyCallable,
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
) -> dict[str, ComputeElementStatus]:
    """Get the latest status of compute elements, scoped to the caller's VO."""
    await check_permissions(action=ActionType.READ)
    _apply_cache_headers(response, snapshot, if_none_match, if_modified_since)
    return cast(
        dict[str, ComputeElementStatus],
        {
            name: ce
            for name, ce in snapshot.data.items()
            if getattr(ce, "vo", "all") in (user_info.vo, "all")
        },
    )


@router.get("/site")
async def get_site_status(
    response: Response,
    snapshot: Annotated[Snapshot, Depends(RSSSnapshotSentinels.get_site_snapshot)],
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: ResourceStatusAccessPolicyCallable,
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
) -> dict[str, SiteStatus]:
    """Get the latest status of sites.

    Sites are always stored with vo="all" so no VO filtering is applied.
    """
    await check_permissions(action=ActionType.READ)
    _apply_cache_headers(response, snapshot, if_none_match, if_modified_since)
    return cast(dict[str, SiteStatus], snapshot.data)


@router.get("/fts")
async def get_fts_status(
    response: Response,
    snapshot: Annotated[Snapshot, Depends(RSSSnapshotSentinels.get_fts_snapshot)],
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: ResourceStatusAccessPolicyCallable,
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
) -> dict[str, FTSStatus]:
    """Get the latest status of FTS servers, scoped to the caller's VO."""
    await check_permissions(action=ActionType.READ)
    _apply_cache_headers(response, snapshot, if_none_match, if_modified_since)
    return cast(
        dict[str, FTSStatus],
        {
            name: fts
            for name, fts in snapshot.data.items()
            if getattr(fts, "vo", "all") in (user_info.vo, "all")
        },
    )
