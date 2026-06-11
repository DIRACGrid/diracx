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
    """Apply cache headers and return the caller's VO view of a snapshot.

    The snapshot covers all VOs so it can be cached once; the response is the
    "all" entries overlaid with the caller's VO-specific entries. The ETag is
    suffixed with the VO (and Vary: Authorization set) since the same URL
    serves different content per VO.
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
    """Get the latest status of storage elements, scoped to the caller's VO."""
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
    """Get the latest status of compute elements, scoped to the caller's VO."""
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
    """Get the latest status of sites, scoped to the caller's VO."""
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
    """Get the latest status of FTS servers, scoped to the caller's VO."""
    return _vo_view(snapshot, user_info.vo, response, if_none_match, if_modified_since)
