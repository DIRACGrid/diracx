from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Annotated, cast

from fastapi import (
    Depends,
    Header,
    HTTPException,
    Response,
    status,
)

from diracx.core.models.rss import (
    ComputeElementStatus,
    FTSStatus,
    SiteStatus,
    Snapshot,
    StorageElementStatus,
)
from diracx.logic.rss.source import (
    ComputeElementStatusSource,
    FTSStatusSource,
    SiteStatusSource,
    StorageElementStatusSource,
)
from diracx.routers.utils.users import AuthorizedUserInfo, verify_dirac_access_token

from ..fastapi_classes import DiracxRouter
from .access_policies import CheckRSSPolicyCallable

logger = logging.getLogger(__name__)

LAST_MODIFIED_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"

router = DiracxRouter()


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
            if not_before > snapshot.modified:
                raise HTTPException(
                    status_code=status.HTTP_304_NOT_MODIFIED, headers=headers
                )

    response.headers.update(headers)


@router.get("/storage")
async def get_storage_status(
    response: Response,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckRSSPolicyCallable,
    snapshot: Annotated[Snapshot, Depends(StorageElementStatusSource.create)],
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
) -> dict[str, StorageElementStatus]:
    """Get the latest status of storage elements, scoped to the caller's VO."""
    _apply_cache_headers(response, snapshot, if_none_match, if_modified_since)
    return cast(
        dict[str, StorageElementStatus],
        {**snapshot.data.get("all", {}), **snapshot.data.get(user_info.vo, {})},
    )


@router.get("/compute")
async def get_compute_status(
    response: Response,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckRSSPolicyCallable,
    snapshot: Annotated[Snapshot, Depends(ComputeElementStatusSource.create)],
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
) -> dict[str, ComputeElementStatus]:
    """Get the latest status of compute elements, scoped to the caller's VO."""
    _apply_cache_headers(response, snapshot, if_none_match, if_modified_since)
    return cast(
        dict[str, ComputeElementStatus],
        {**snapshot.data.get("all", {}), **snapshot.data.get(user_info.vo, {})},
    )


@router.get("/site")
async def get_site_status(
    response: Response,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckRSSPolicyCallable,
    snapshot: Annotated[Snapshot, Depends(SiteStatusSource.create)],
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
) -> dict[str, SiteStatus]:
    """Get the latest status of sites, scoped to the caller's VO."""
    _apply_cache_headers(response, snapshot, if_none_match, if_modified_since)
    return cast(
        dict[str, SiteStatus],
        {**snapshot.data.get("all", {}), **snapshot.data.get(user_info.vo, {})},
    )


@router.get("/fts")
async def get_fts_status(
    response: Response,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckRSSPolicyCallable,
    snapshot: Annotated[Snapshot, Depends(FTSStatusSource.create)],
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
) -> dict[str, FTSStatus]:
    """Get the latest status of FTS servers, scoped to the caller's VO."""
    _apply_cache_headers(response, snapshot, if_none_match, if_modified_since)
    return cast(
        dict[str, FTSStatus],
        {**snapshot.data.get("all", {}), **snapshot.data.get(user_info.vo, {})},
    )
