"""Well-known auth discovery endpoints for DIRACX.

This module exposes OpenID Connect discovery endpoints and metadata
endpoints, including OIDC configuration, JWKS, DIRAC installation metadata,
and security.txt information.
"""

from __future__ import annotations

from fastapi import Request

from diracx.core.models import Metadata, OpenIDConfiguration
from diracx.core.settings import AuthSettings
from diracx.logic.auth import (
    get_installation_metadata as get_installation_metadata_bl,
)
from diracx.logic.auth import (
    get_jwks as get_jwks_bl,
)
from diracx.logic.auth import (
    get_openid_configuration as get_openid_configuration_bl,
)
from diracx.routers.dependencies import Config

from ..fastapi_classes import DiracxRouter

router = DiracxRouter(require_auth=False, path_root="")


@router.get("/openid-configuration")
async def get_openid_configuration(
    request: Request,
    config: Config,
    settings: AuthSettings,
) -> OpenIDConfiguration:
    """Return the OpenID Connect discovery document.

    Provides the standard OIDC discovery information (authorization,
    token, userinfo and other endpoint locations) so clients can
    automatically configure themselves to interact with this issuer.

    Args:
        request (Request): Incoming request used to build absolute URLs for
            endpoints referenced in the discovery document.
        config (Config): Application configuration used to populate
            installation-specific metadata.
        settings (AuthSettings): Authentication settings (used to decide
            supported features and values exposed in the configuration).

    Returns:
        OpenIDConfiguration: A dataclass representing the ephemeral
            discovery document as defined by OpenID Connect.
    """
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
    """Return the JSON Web Key Set (JWKS) for this issuer.

    Clients use the JWKS to retrieve the public keys required to verify
    signatures on issued tokens (ID tokens / access tokens). The JWKS
    contains the public portion of the server's signing keys and is safe
    to publish publicly.

    Args:
        settings (AuthSettings): Authentication settings containing key
            configuration used to build the JWKS.

    Returns:
        dict: A JSON-serializable JWKS mapping (``{"keys": [...]}``).
    """
    return await get_jwks_bl(settings)


@router.get("/dirac-metadata")
async def get_installation_metadata(
    config: Config,
) -> Metadata:
    """Return DIRAC-specific installation metadata.

    Provides metadata describing this DIRAC installation (capabilities,
    supported integrations, or other information consumers may need to
    adapt behavior for a particular deployment).

    Args:
        config (Config): Application configuration used to populate the
            installation metadata.

    Returns:
        Metadata: Installation metadata structure.
    """
    return await get_installation_metadata_bl(config)


@router.get("/security.txt")
async def get_security_txt() -> str:
    """Return the site's security contact information (security.txt).

    The response contains contact and disclosure metadata following the
    security.txt convention, aiding security researchers and reporters in
    finding the appropriate channel to disclose vulnerabilities.

    Returns:
        str: A plaintext ``security.txt`` document.
    """
    return """Contact: https://github.com/DIRACGrid/diracx/security/advisories/new
Expires: 2026-07-02T23:59:59.000Z
Preferred-Languages: en
"""
