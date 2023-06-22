from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated

from fastapi import (
    Depends,
    Header,
    HTTPException,
    Response,
    status,
)

from diracx.core.config import Config, LocalGitConfigSource
from diracx.core.secrets import LocalFileUrl

from .fastapi_classes import DiracxRouter, ServiceSettingsBase

LAST_MODIFIED_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"


class ConfigSettings(ServiceSettingsBase, env_prefix="DIRACX_SERVICE_CONFIG_"):
    backend_url: LocalFileUrl


def get_config(
    settings: Annotated[ConfigSettings, Depends(ConfigSettings.create)]
) -> Config:
    backend_url = settings.backend_url
    if backend_url.scheme == "file":
        assert backend_url.path
        return LocalGitConfigSource(Path(backend_url.path)).read_config()
    else:
        raise NotImplementedError(backend_url.scheme)


router = DiracxRouter(settings_class=ConfigSettings)


@router.get("/{vo}")
async def serve_config(
    vo: str,
    config: Annotated[Config, Depends(get_config)],
    response: Response,
    if_none_match: Annotated[str | None, Header()] = None,
    if_modified_since: Annotated[str | None, Header()] = None,
):
    """ "
    Get the latest view of the config.


    If If-None-Match header is given and matches the latest ETag, return 304

    If If-Modified-Since is given and is newer than latest,
        return 304: this is to avoid flip/flopping
    """
    headers = {
        "ETag": config._hexsha,
        "Last-Modified": config._modified.strftime(LAST_MODIFIED_FORMAT),
    }

    if if_none_match == config._hexsha:
        raise HTTPException(status_code=status.HTTP_304_NOT_MODIFIED, headers=headers)

    # This is to prevent flip/flopping in case
    # a server gets out of sync with disk
    if if_modified_since:
        try:
            not_before = datetime.strptime(
                if_modified_since, LAST_MODIFIED_FORMAT
            ).astimezone(timezone.utc)
        except ValueError:
            pass
        else:
            if not_before > config._modified:
                raise HTTPException(
                    status_code=status.HTTP_304_NOT_MODIFIED, headers=headers
                )

    response.headers.update(headers)

    return config
