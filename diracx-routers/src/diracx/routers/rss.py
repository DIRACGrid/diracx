from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Annotated, Literal, Union, cast

from fastapi import (
    Depends,
    Header,
    HTTPException,
    Query,
    Response,
    status,
)

from diracx.core.config.sources import ResourceStatusSource
from diracx.core.models.rss import (
    ComputeElementStatus,
    FTSStatus,
    SiteStatus,
    StorageElementStatus,
)
from diracx.db.sql.rss.db import ResourceStatusDB
from diracx.routers.utils.users import AuthorizedUserInfo, verify_dirac_access_token

from .fastapi_classes import DiracxRouter

logger = logging.getLogger(__name__)

LAST_MODIFIED_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"

router = DiracxRouter()

# Keep track of the ResourceStatusSource instances
resource_status_sources: dict[tuple[str, str], ResourceStatusSource] = {}


# Override the ResourceStatusSource dependency to use
async def get_resource_status_source(
    resource_status_db: ResourceStatusDB,
    resource_type: Literal[
        "ComputeElement", "StorageElement", "Site", "FTS"
    ] = "StorageElement",
    vo: str = "all",
) -> ResourceStatusSource:
    key = (resource_type, vo)
    if key not in resource_status_sources:
        logger.debug(f"Creating new ResourceStatusSource for {key}")
        resource_status_sources[key] = ResourceStatusSource(
            resource_type=resource_type,
            vo=vo,
            resource_status_db=resource_status_db,
        )
        # populate the cache
        resource_status_sources[key].read()
    else:
        logger.debug(f"Reusing existing ResourceStatusSource for {key}")
    return resource_status_sources[key]


async def get_resource_status(
    response: Response,
    resource_type: str,
    vo: str,
    resource_status_db: ResourceStatusDB,
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
) -> Union[
    dict[str, StorageElementStatus],
    dict[str, ComputeElementStatus],
    dict[str, SiteStatus],
    dict[str, FTSStatus],
]:
    """Get the latest status of resources.

    If If-None-Match header is given and matches the latest ETag, return 304

    If If-Modified-Since is given and is newer than latest,
        return 304: this is to avoid flip/flopping
    """
    resource_status_source = await get_resource_status_source(
        resource_type=resource_type,
        vo=vo,
        resource_status_db=resource_status_db,
    )
    status_data = await resource_status_source.read_non_blocking()

    last_modified = max(val._modified for val in status_data.values())

    headers = {
        "ETag": list(status_data.values())[0]._hexsha,
        "Last-Modified": last_modified.strftime(LAST_MODIFIED_FORMAT),
    }

    if if_none_match == list(status_data.values())[0]._hexsha:
        raise HTTPException(status_code=status.HTTP_304_NOT_MODIFIED, headers=headers)

    # This is to prevent flip/flopping in case
    # a server gets out of sync with disk
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
            if not_before > last_modified:
                raise HTTPException(
                    status_code=status.HTTP_304_NOT_MODIFIED, headers=headers
                )

    response.headers.update(headers)

    return status_data


@router.get("/storage")
async def get_storage_status(
    response: Response,
    resource_status_db: ResourceStatusDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
):
    """Get the latest status of storage elements."""
    return cast(
        dict[str, StorageElementStatus],
        await get_resource_status(
            response=response,
            resource_type="StorageElement",
            resource_status_db=resource_status_db,
            vo=user_info.vo,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        ),
    )


@router.get("/compute")
async def get_compute_status(
    response: Response,
    resource_status_db: ResourceStatusDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
):
    """Get the latest status of compute elements."""
    return cast(
        dict[str, ComputeElementStatus],
        await get_resource_status(
            response=response,
            resource_type="ComputeElement",
            resource_status_db=resource_status_db,
            vo=user_info.vo,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        ),
    )


@router.get("/site")
async def get_site_status(
    response: Response,
    resource_status_db: ResourceStatusDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    vo: Annotated[str | None, Query()] = None,
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
):
    """Get the latest status of sites."""
    return cast(
        dict[str, SiteStatus],
        await get_resource_status(
            response=response,
            resource_type="Site",
            resource_status_db=resource_status_db,
            vo=user_info.vo,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        ),
    )


@router.get("/fts")
async def get_fts_status(
    response: Response,
    resource_status_db: ResourceStatusDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    vo: Annotated[str | None, Query()] = None,
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
):
    """Get the latest status of FTS servers."""
    return cast(
        dict[str, FTSStatus],
        await get_resource_status(
            response=response,
            resource_type="FTS",
            resource_status_db=resource_status_db,
            vo=user_info.vo,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        ),
    )
