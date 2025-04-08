from __future__ import annotations

from cachetools import TTLCache
from fastapi import Request

from diracx.core.models import Metadata, OpenIDConfiguration
from diracx.logic.auth.well_known import (
    get_installation_metadata as get_installation_metadata_bl,
)
from diracx.logic.auth.well_known import (
    get_openid_configuration as get_openid_configuration_bl,
)

from ..dependencies import AuthSettings, Config
from ..fastapi_classes import DiracxRouter

router = DiracxRouter(require_auth=False, path_root="")


_static_cache: TTLCache = TTLCache(maxsize=5, ttl=60)


@router.get("/openid-configuration")
async def get_openid_configuration(
    request: Request,
    config: Config,
    settings: AuthSettings,
) -> OpenIDConfiguration:
    """OpenID Connect discovery endpoint."""
    cached_hash = f"openid-configuration:{config._hexsha}"
    if cached_hash not in _static_cache:
        _static_cache[cached_hash] = await get_openid_configuration_bl(
            str(request.url_for("get_oidc_token")),
            str(request.url_for("userinfo")),
            str(request.url_for("initiate_authorization_flow")),
            str(request.url_for("initiate_device_flow")),
            config,
            settings,
        )
    return _static_cache[cached_hash]


@router.get("/dirac-metadata")
async def get_installation_metadata(
    config: Config,
) -> Metadata:
    """Get metadata about the dirac installation."""
    cached_hash = f"dirac-metadata:{config._hexsha}"
    if cached_hash not in _static_cache:
        _static_cache[cached_hash] = await get_installation_metadata_bl(config)

    return _static_cache[cached_hash]
