"""Expose the current DiracX configuration with conditional HTTP caching."""

from __future__ import annotations

__all__ = ["router"]

import logging
from typing import Annotated

from fastapi import (
    Header,
    Response,
)

from diracx.routers.dependencies import Config

from .access_policies import open_access
from .fastapi_classes import DiracxRouter
from .utils.http_cache import apply_cache_headers

logger = logging.getLogger(__name__)

router = DiracxRouter()


@open_access
@router.get("/")
async def serve_config(
    config: Config,
    response: Response,
    # check_permissions: OpenAccessPolicyCallable,
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
):
    """Return the latest configuration snapshot.

    The response includes ETag and Last-Modified headers. When a supplied
    conditional header indicates the client's cached snapshot is current, the
    cache helper raises an HTTP exception with status 304.

    Args:
        config: Current configuration snapshot and its revision metadata.
        response: Response whose cache headers are populated.
        if_none_match: Optional ETag from the client's cached response.
        if_modified_since: Optional modification date from the client's cached response.

    Returns:
        The current configuration snapshot.

    Raises:
        HTTPException: With status 304 when the client's cached snapshot is
            still current.
    """
    # await check_permissions()
    apply_cache_headers(
        response,
        etag=config._hexsha,
        modified=config._modified,
        if_none_match=if_none_match,
        if_modified_since=if_modified_since,
    )

    return config
