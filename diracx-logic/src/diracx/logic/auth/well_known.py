"""OpenID Connect discovery and installation metadata endpoints."""

from __future__ import annotations

from diracx.core.config import Config
from diracx.core.models import GroupInfo, Metadata, OpenIDConfiguration
from diracx.core.settings import AuthSettings


async def get_openid_configuration(
    token_endpoint: str,
    userinfo_endpoint: str,
    authorization_endpoint: str,
    device_authorization_endpoint: str,
    revoke_refresh_token_endpoint: str,
    jwks_endpoint: str,
    config: Config,
    settings: AuthSettings,
) -> OpenIDConfiguration:
    """Build OpenID Connect discovery metadata for the installation.

    Args:
        token_endpoint: URL of the token endpoint.
        userinfo_endpoint: URL of the user information endpoint.
        authorization_endpoint: URL of the authorization endpoint.
        device_authorization_endpoint: URL of the device authorization endpoint.
        revoke_refresh_token_endpoint: URL of the token revocation endpoint.
        jwks_endpoint: URL of the JSON Web Key Set endpoint.
        config: Installation configuration used to enumerate VO and group scopes.
        settings: Authentication settings for issuer and supported algorithms.

    Returns:
        OpenID Connect provider metadata, including endpoints and supported scopes.
    """
    scopes_supported = []
    for vo in config.registry:
        scopes_supported.append(f"vo:{vo}")
        scopes_supported += [f"group:{vo}" for vo in config.registry[vo].groups]
    scopes_supported += [f"property:{p}" for p in settings.available_properties]

    return {
        "issuer": settings.token_issuer,
        "token_endpoint": token_endpoint,
        "userinfo_endpoint": userinfo_endpoint,
        "authorization_endpoint": authorization_endpoint,
        "device_authorization_endpoint": device_authorization_endpoint,
        "revocation_endpoint": revoke_refresh_token_endpoint,
        "jwks_uri": jwks_endpoint,
        "grant_types_supported": [
            "authorization_code",
            "urn:ietf:params:oauth:grant-type:device_code",
        ],
        "scopes_supported": scopes_supported,
        "response_types_supported": ["code"],
        "token_endpoint_auth_signing_alg_values_supported": settings.token_allowed_algorithms,
        "token_endpoint_auth_methods_supported": ["none"],
        "code_challenge_methods_supported": ["S256"],
    }


async def get_jwks(settings: AuthSettings) -> dict:
    """Return the public JSON Web Keys used to verify tokens.

    Args:
        settings: Authentication settings containing the signing key set.

    Returns:
        JWK Set dictionary containing public keys only.
    """
    return settings.token_keystore.jwks.as_dict(  # type: ignore[return-value]
        private=False,
    )


async def get_installation_metadata(
    config: Config,
) -> Metadata:
    """Build public metadata describing the DiracX installation.

    Args:
        config: Installation configuration containing VO, group, and support data.

    Returns:
        Metadata describing each virtual organization and its groups.
    """
    metadata: Metadata = {
        "virtual_organizations": {},
    }
    for vo, vo_info in config.registry.items():
        groups: dict[str, GroupInfo] = {
            group: {"properties": sorted(group_info.properties)}
            for group, group_info in vo_info.groups.items()
        }
        metadata["virtual_organizations"][vo] = {
            "groups": groups,
            "support": {
                "message": vo_info.support.message,
                "webpage": vo_info.support.webpage,
                "email": vo_info.support.email,
            },
            "default_group": vo_info.default_group,
        }

    return metadata
