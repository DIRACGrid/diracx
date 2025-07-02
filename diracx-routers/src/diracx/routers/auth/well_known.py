from __future__ import annotations

from fastapi import Request

from diracx.core.models import Metadata, OpenIDConfiguration
from diracx.logic.auth.well_known import (
    get_installation_metadata as get_installation_metadata_bl,
)
from diracx.logic.auth.well_known import (
    get_jwks as get_jwks_bl,
)
from diracx.logic.auth.well_known import (
    get_openid_configuration as get_openid_configuration_bl,
)

from ..dependencies import AuthSettings, Config
from ..fastapi_classes import DiracxRouter

router = DiracxRouter(require_auth=False, path_root="")


@router.get("/openid-configuration")
async def get_openid_configuration(
    request: Request,
    config: Config,
    settings: AuthSettings,
) -> OpenIDConfiguration:
    """OpenID Connect discovery endpoint."""
    return await get_openid_configuration_bl(
        str(request.url_for("get_oidc_token")),
        str(request.url_for("userinfo")),
        str(request.url_for("initiate_authorization_flow")),
        str(request.url_for("initiate_device_flow")),
        str(request.url_for("revoke_refresh_token_by_refresh_token")),
        str(request.url_for("get_jwks")),
        config,
        settings,
    )


@router.get("/jwks.json")
async def get_jwks(
    settings: AuthSettings,
) -> dict:
    """Get the JWKs (public keys)."""
    return await get_jwks_bl(settings)


@router.get("/dirac-metadata")
async def get_installation_metadata(
    config: Config,
) -> Metadata:
    """Get metadata about the dirac installation."""
    return await get_installation_metadata_bl(config)


@router.get("/.well-known/security.txt")
async def get_security_txt() -> str:
    """Get the security.txt file."""
    return """Contact: https://github.com/DIRACGrid/diracx/security/advisories/new
Expires: 2026-07-02T23:59:59.000Z
Preferred-Languages: en
"""
