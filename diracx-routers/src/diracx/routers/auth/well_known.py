"""Expose public authentication discovery and installation metadata routes.

These unauthenticated endpoints provide OpenID Connect configuration, public
signing keys, installation metadata, and the security contact information.
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

    The document includes route URLs, supported grants and scopes, token
    authentication methods, and PKCE capabilities.

    Args:
        request: Incoming request used to build absolute endpoint URLs.
        config: DiracX configuration used to list supported VOs and groups.
        settings: Authentication settings used to populate issuer and supported algorithms.

    Returns:
        OpenID Connect configuration for this installation.
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
    """Return the public JSON Web Keys used to verify DiracX tokens.

    Args:
        settings: Authentication settings containing the token signing key store.

    Returns:
        A JSON Web Key Set containing public keys only.
    """
    return await get_jwks_bl(settings)


@router.get("/dirac-metadata")
async def get_installation_metadata(
    config: Config,
) -> Metadata:
    """Return authentication metadata for the configured VOs.

    Args:
        config: DiracX configuration containing VO, group, and support information.

    Returns:
        Metadata listing each VO's groups, properties, support contacts, and default group.
    """
    return await get_installation_metadata_bl(config)


@router.get("/security.txt")
async def get_security_txt() -> str:
    """Return the installation's security contact information.

    Returns:
        The ``security.txt`` contents as plain text.
    """
    return """Contact: https://github.com/DIRACGrid/diracx/security/advisories/new
Expires: 2026-07-02T23:59:59.000Z
Preferred-Languages: en
"""
