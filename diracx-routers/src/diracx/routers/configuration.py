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
    """Get the latest view of the config.

    If If-None-Match header is given and matches the latest ETag, return 304

    If If-Modified-Since is given and is newer than latest,
        return 304: this is to avoid flip/flopping
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
