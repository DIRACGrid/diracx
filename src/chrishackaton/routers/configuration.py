from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    Header,
    HTTPException,
    Response,
    status,
)

from ..config import Config, get_config
from ..properties import SecurityProperty
from ..utils import has_properties

router = APIRouter(
    tags=["config"],
    dependencies=[has_properties(SecurityProperty.NORMAL_USER)],
)


LAST_MODIFIED_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"


@router.get("/{vo}")
async def serve_config(
    vo: str,
    config: Annotated[Config, Depends(get_config)],
    response: Response,
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
):
    headers = {
        "ETag": config.hexsha,
        "Last-Modified": config.modified.strftime(LAST_MODIFIED_FORMAT),
    }

    if if_none_match == config.hexsha:
        raise HTTPException(status_code=status.HTTP_304_NOT_MODIFIED, headers=headers)

    # This is to prevent flip/flopping in case
    # a server gets out of sync with disk
    if if_modified_since:
        if datetime.strptime(if_modified_since, LAST_MODIFIED_FORMAT) > config.modified:
            raise HTTPException(
                status_code=status.HTTP_304_NOT_MODIFIED, headers=headers
            )

    response.headers.update(headers)

    return config
