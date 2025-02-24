from __future__ import annotations

from fastapi import Request

from diracx.backend.bl.auth.well_known import (
    installation_metadata as installation_metadata_bl,
)
from diracx.backend.bl.auth.well_known import (
    openid_configuration as openid_configuration_bl,
)
from diracx.core.models import Metadata, OpenIDConfiguration
from diracx.core.settings import AuthSettings

from ..dependencies import Config, DevelopmentSettings
from ..fastapi_classes import DiracxRouter

router = DiracxRouter(require_auth=False, path_root="")


@router.get("/openid-configuration")
async def openid_configuration(
    request: Request,
    config: Config,
    settings: AuthSettings,
) -> OpenIDConfiguration:
    """OpenID Connect discovery endpoint."""
    return await openid_configuration_bl(
        request.url_for("token"),
        request.url_for("userinfo"),
        request.url_for("authorize"),
        request.url_for("device_authorization"),
        config,
        settings,
    )


@router.get("/dirac-metadata")
async def installation_metadata(
    config: Config,
    dev_settings: DevelopmentSettings,
) -> Metadata:
    """Get metadata about the dirac installation."""
    return await installation_metadata_bl(config, dev_settings)
