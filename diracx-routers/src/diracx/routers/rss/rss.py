"""Expose cached, VO-scoped resource status endpoints.

Authenticated callers can retrieve storage, compute, site, and FTS statuses
for their VO. Responses support conditional HTTP caching.
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

from fastapi import Depends, Header, Response

from diracx.core.models.rss import (
    ComputeElementStatus,
    FTSStatus,
    SiteStatus,
    StorageElementStatus,
)
from diracx.core.sources import Snapshot
from diracx.logic.rss.source import (
    ComputeElementStatusSource,
    FTSStatusSource,
    SiteStatusSource,
    StorageElementStatusSource,
)
from diracx.routers.utils.users import AuthorizedUserInfo, verify_dirac_access_token

from ..fastapi_classes import DiracxRouter
from ..utils.http_cache import apply_cache_headers
from .access_policies import CheckRSSPolicyCallable

logger = logging.getLogger(__name__)

router = DiracxRouter()


def _vo_view(
    snapshot: Snapshot,
    vo: str,
    response: Response,
    if_none_match: str | None,
    if_modified_since: str | None,
) -> dict[str, Any]:
    """Apply conditional-cache headers and select a snapshot's VO-specific view.

    The snapshot covers all VOs and is cached once. The returned mapping
    overlays the caller's VO-specific entries on the shared ``all`` entries.
    The VO-specific ETag and ``Vary: Authorization`` header distinguish the
    different response views for the same URL.

    Args:
        snapshot: Cached snapshot containing status data for all VOs.
        vo: Caller's VO used to select and overlay status entries.
        response: Response whose cache headers are populated.
        if_none_match: Client's conditional ETag, if supplied.
        if_modified_since: Client's conditional modification date, if supplied.

    Returns:
        Status entries shared across VOs, overlaid with entries for ``vo``.

    Raises:
        HTTPException: With status 304 if the client's cached representation
            is still current.
    """
    apply_cache_headers(
        response,
        etag=f"{snapshot.hexsha}-{vo}",
        modified=snapshot.modified,
        if_none_match=if_none_match,
        if_modified_since=if_modified_since,
        vary="Authorization",
    )
    return {**snapshot.data.get("all", {}), **snapshot.data.get(vo, {})}


@router.get("/storage")
async def get_storage_status(
    response: Response,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckRSSPolicyCallable,
    snapshot: Annotated[Snapshot, Depends(StorageElementStatusSource.create)],
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
) -> dict[str, StorageElementStatus]:
    """Return storage-element statuses visible to the caller's VO.

    \f

    Args:
        response: Response whose conditional-cache headers are populated.
        user_info: Authenticated user information used to select the VO.
        check_permissions: Access-policy dependency for the RSS request.
        snapshot: Cached storage-element statuses across all VOs.
        if_none_match: Optional ETag from the client's cached response.
        if_modified_since: Optional modification date from the client's cached response.

    Returns:
        Storage-element statuses shared across VOs and specific to the caller's VO.

    Raises:
        HTTPException: With status 304 if the client's cached representation
            is still current.
    """
    await check_permissions()
    return _vo_view(snapshot, user_info.vo, response, if_none_match, if_modified_since)


@router.get("/compute")
async def get_compute_status(
    response: Response,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckRSSPolicyCallable,
    snapshot: Annotated[Snapshot, Depends(ComputeElementStatusSource.create)],
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
) -> dict[str, ComputeElementStatus]:
    """Return compute element statuses visible to the caller's VO.

    \f

    Args:
        response: Response whose conditional-cache headers are populated.
        user_info: Authenticated user information used to select the VO.
        check_permissions: Access-policy dependency for the RSS request.
        snapshot: Cached compute element statuses across all VOs.
        if_none_match: Optional ETag from the client's cached response.
        if_modified_since: Optional modification date from the client's cached response.

    Returns:
        Compute element statuses shared across VOs and specific to the caller's VO.

    Raises:
        HTTPException: With status 304 if the client's cached representation
            is still current.
    """
    await check_permissions()
    return _vo_view(snapshot, user_info.vo, response, if_none_match, if_modified_since)


@router.get("/site")
async def get_site_status(
    response: Response,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckRSSPolicyCallable,
    snapshot: Annotated[Snapshot, Depends(SiteStatusSource.create)],
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
) -> dict[str, SiteStatus]:
    """Return site statuses visible to the caller's VO.

    \f

    Args:
        response: Response whose conditional-cache headers are populated.
        user_info: Authenticated user information used to select the VO.
        check_permissions: Access-policy dependency for the RSS request.
        snapshot: Cached site statuses across all VOs.
        if_none_match: Optional ETag from the client's cached response.
        if_modified_since: Optional modification date from the client's cached response.

    Returns:
        Site statuses shared across VOs and specific to the caller's VO.

    Raises:
        HTTPException: With status 304 if the client's cached representation
            is still current.
    """
    await check_permissions()
    return _vo_view(snapshot, user_info.vo, response, if_none_match, if_modified_since)


@router.get("/fts")
async def get_fts_status(
    response: Response,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckRSSPolicyCallable,
    snapshot: Annotated[Snapshot, Depends(FTSStatusSource.create)],
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
) -> dict[str, FTSStatus]:
    """Return FTS server statuses visible to the caller's VO.

    \f

    Args:
        response: Response whose conditional-cache headers are populated.
        user_info: Authenticated user information used to select the VO.
        check_permissions: Access-policy dependency for the RSS request.
        snapshot: Cached FTS server statuses across all VOs.
        if_none_match: Optional ETag from the client's cached response.
        if_modified_since: Optional modification date from the client's cached response.

    Returns:
        FTS statuses shared across VOs and specific to the caller's VO.

    Raises:
        HTTPException: With status 304 if the client's cached representation
            is still current.
    """
    await check_permissions()
    return _vo_view(snapshot, user_info.vo, response, if_none_match, if_modified_since)
